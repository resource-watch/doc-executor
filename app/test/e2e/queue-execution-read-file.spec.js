/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
const fs = require('fs');
const path = require('path');
const chaiMatch = require('chai-match');
const sleep = require('sleep');

const { getTestServer } = require('./test-server');

chai.use(chaiMatch);
const should = chai.should();

let requester;
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect(process.env.HOST_IP);

describe('EXECUTION_READ_FILE handling process', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        let connectAttempts = 10;
        while (connectAttempts >= 0 && rabbitmqConnection === null) {
            try {
                rabbitmqConnection = await amqp.connect(config.get('rabbitmq.url'));
            } catch (err) {
                connectAttempts -= 1;
                await sleep.sleep(5);
            }
        }
        if (!rabbitmqConnection) {
            throw new RabbitMQConnectionError();
        }

        channel = await rabbitmqConnection.createConfirmChannel();
        await channel.assertQueue(config.get('queues.executorTasks'));
        await channel.assertQueue(config.get('queues.status'));
        await channel.assertQueue(config.get('queues.data'));

        await channel.purgeQueue(config.get('queues.executorTasks'));
        await channel.purgeQueue(config.get('queues.status'));
        await channel.purgeQueue(config.get('queues.data'));

        requester = await getTestServer();
    });

    beforeEach(async () => {
        let connectAttempts = 10;
        while (connectAttempts >= 0 && rabbitmqConnection === null) {
            try {
                rabbitmqConnection = await amqp.connect(config.get('rabbitmq.url'));
            } catch (err) {
                connectAttempts -= 1;
                await sleep.sleep(5);
            }
        }
        if (!rabbitmqConnection) {
            throw new RabbitMQConnectionError();
        }

        channel = await rabbitmqConnection.createConfirmChannel();
        await channel.assertQueue(config.get('queues.executorTasks'));
        await channel.assertQueue(config.get('queues.status'));
        await channel.assertQueue(config.get('queues.data'));

        await channel.purgeQueue(config.get('queues.executorTasks'));
        await channel.purgeQueue(config.get('queues.status'));
        await channel.purgeQueue(config.get('queues.data'));

        const executorTasksQueueStatus = await channel.checkQueue(config.get('queues.executorTasks'));
        executorTasksQueueStatus.messageCount.should.equal(0);

        const statusQueueStatus = await channel.checkQueue(config.get('queues.status'));
        statusQueueStatus.messageCount.should.equal(0);

        const dataQueueStatus = await channel.checkQueue(config.get('queues.data'));
        dataQueueStatus.messageCount.should.equal(0);
    });

    it('Consume a EXECUTION_READ_FILE message reads the data on the file and pushes it through new DATA message(s) (happy case)', async () => {
        const timestamp = new Date().getTime();

        nock('http://api.resourcewatch.org')
            .get('/dataset')
            .reply(200, {
                data: JSON.parse(fs.readFileSync(path.join(__dirname, 'dataset-list.json'))),
                links: {
                    self: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    first: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    last: 'http://api.resourcewatch.org/v1/dataset?page[number]=150&page[size]=10',
                    prev: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    next: 'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10'
                },
                meta: { 'total-pages': 150, 'total-items': 1499, size: 10 }
            });

        const message = {
            id: '9745512f-9463-45c1-ba4d-99f2035b8414',
            type: 'EXECUTION_READ_FILE',
            taskId: 'efaa80ad-97e4-487b-83ea-a2d0022fc0da',
            fileUrl: 'http://api.resourcewatch.org/dataset',
            dataPath: 'data',
            provider: 'json',
            index: 'index_ef7d64c631664053a0b7e221d84496a5_1574922887909',
            datasetId: `${timestamp}`
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 2;
        let expectedDataQueueMessageCount = 1;

        const validateDataQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            if (content.type === docImporterMessages.data.MESSAGE_TYPES.DATA) {
                content.should.have.property('id');
                content.should.have.property('index').and.equal(message.index);
                content.should.have.property('taskId').and.equal(message.taskId);
                content.should.have.property('data');
                content.data.forEach((value, index) => {
                    if (index % 2 === 0) {
                        value.should.have.property('index').and.be.an('object');
                        value.index.should.have.property('_index').and.be.a('string');
                        value.index.should.have.property('_type').and.equal('type');
                    } else {
                        value.should.have.property('attributes').and.be.an('object');
                        value.should.have.property('id').and.be.a('string');
                        value.should.have.property('type').and.be.a('string').and.equal('dataset');
                    }
                });
            } else {
                throw new Error('Unexpected message type');
            }

            await channel.ack(msg);

            expectedDataQueueMessageCount -= 1;

            if (expectedDataQueueMessageCount < 0 || expectedStatusQueueMessageCount < 0) {
                throw new Error(`Unexpected message count - expectedDataQueueMessageCount:${expectedDataQueueMessageCount} expectedStatusQueueMessageCount:${expectedStatusQueueMessageCount}`);
            }

            if (expectedStatusQueueMessageCount === 0 && expectedDataQueueMessageCount === 0) {
                resolve();
            }
        };

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            switch (content.type) {

                case docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_DATA:
                    content.should.have.property('id');
                    content.should.have.property('taskId').and.equal(message.taskId);
                    break;
                case docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_FILE:
                    content.should.have.property('id');
                    content.should.have.property('taskId').and.equal(message.taskId);
                    break;
                default:
                    throw new Error('Unexpected message type');

            }

            await channel.ack(msg);

            expectedStatusQueueMessageCount -= 1;

            if (expectedDataQueueMessageCount < 0 || expectedStatusQueueMessageCount < 0) {
                throw new Error(`Unexpected message count - expectedDataQueueMessageCount:${expectedDataQueueMessageCount} expectedStatusQueueMessageCount:${expectedStatusQueueMessageCount}`);
            }

            if (expectedStatusQueueMessageCount === 0 && expectedDataQueueMessageCount === 0) {
                resolve();
            }
        };

        return new Promise((resolve) => {
            channel.consume(config.get('queues.status'), validateStatusQueueMessages(resolve), { exclusive: true });
            channel.consume(config.get('queues.data'), validateDataQueueMessages(resolve), { exclusive: true });
        });
    });

    it('Consume a EXECUTION_READ_FILE message with an empty file reads the data on the file and flags the error', async () => {
        const timestamp = new Date().getTime();

        nock('http://api.resourcewatch.org')
            .get('/dataset')
            .reply(404);

        const message = {
            id: '9745512f-9463-45c1-ba4d-99f2035b8414',
            type: 'EXECUTION_READ_FILE',
            taskId: 'efaa80ad-97e4-487b-83ea-a2d0022fc0da',
            fileUrl: 'http://api.resourcewatch.org/dataset',
            dataPath: 'data',
            provider: 'json',
            index: 'index_ef7d64c631664053a0b7e221d84496a5_1574922887909',
            datasetId: `${timestamp}`
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 2;

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            switch (content.type) {

                case docImporterMessages.status.MESSAGE_TYPES.STATUS_ERROR:
                    content.should.have.property('id');
                    content.should.have.property('taskId').and.equal(message.taskId);
                    content.should.have.property('error').and.contain('File http://api.resourcewatch.org/dataset is empty. Temporary file could not be found at');
                    break;
                case docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_FILE:
                    content.should.have.property('id');
                    content.should.have.property('taskId').and.equal(message.taskId);
                    break;
                default:
                    throw new Error(`Unexpected message type "${content.type}"`);

            }

            await channel.ack(msg);

            expectedStatusQueueMessageCount -= 1;

            if (expectedStatusQueueMessageCount < 0) {
                throw new Error(`Unexpected message count - expectedStatusQueueMessageCount:${expectedStatusQueueMessageCount}`);
            }

            if (expectedStatusQueueMessageCount === 0) {
                resolve();
            }
        };

        return new Promise((resolve) => {
            channel.consume(config.get('queues.status'), validateStatusQueueMessages(resolve), { exclusive: true });
        });
    });

    it('Consume a EXECUTION_READ_FILE message reads the data on the file and fails if the file contains data in invalid format - json file on csv dataset', async () => {
        const timestamp = new Date().getTime();

        nock('http://api.resourcewatch.org')
            .get('/dataset')
            .times(4)
            .reply(200, {
                data: JSON.parse(fs.readFileSync(path.join(__dirname, 'dataset-list.json'))),
                links: {
                    self: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    first: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    last: 'http://api.resourcewatch.org/v1/dataset?page[number]=150&page[size]=10',
                    prev: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    next: 'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10'
                },
                meta: { 'total-pages': 150, 'total-items': 1499, size: 10 }
            });

        nock('http://api.resourcewatch.org')
            .head('/dataset')
            .times(4)
            .reply(200, {
                data: JSON.parse(fs.readFileSync(path.join(__dirname, 'dataset-list.json'))),
                links: {
                    self: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    first: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    last: 'http://api.resourcewatch.org/v1/dataset?page[number]=150&page[size]=10',
                    prev: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    next: 'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10'
                },
                meta: { 'total-pages': 150, 'total-items': 1499, size: 10 }
            });

        const message = {
            id: '9745512f-9463-45c1-ba4d-99f2035b8414',
            type: 'EXECUTION_READ_FILE',
            taskId: 'efaa80ad-97e4-487b-83ea-a2d0022fc0da',
            fileUrl: 'http://api.resourcewatch.org/dataset',
            dataPath: 'data',
            provider: 'csv',
            index: 'index_ef7d64c631664053a0b7e221d84496a5_1574922887909',
            datasetId: `${timestamp}`
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            if (content.type === docImporterMessages.status.MESSAGE_TYPES.STATUS_ERROR) {
                content.should.have.property('id');
                content.should.have.property('taskId').and.equal(message.taskId);
                content.should.have.property('error').and.equal('Exceeded maximum number of attempts to process message of type "EXECUTION_READ_FILE". Error message: "Error processing CSV file from http://api.resourcewatch.org/dataset. Error message: "Parse Error: expected: \'"\' got: \':\'. at \':"dataset"""');
            } else {
                throw new Error(`Unexpected message type "${content.type}"`);
            }

            await channel.ack(msg);

            expectedStatusQueueMessageCount -= 1;

            if (expectedStatusQueueMessageCount < 0) {
                throw new Error(`Unexpected message count - expectedStatusQueueMessageCount:${expectedStatusQueueMessageCount}`);
            }

            if (expectedStatusQueueMessageCount === 0) {
                resolve();
            }
        };

        return new Promise((resolve) => {
            channel.consume(config.get('queues.status'), validateStatusQueueMessages(resolve), { exclusive: true });
        });
    });

    it('Consume a EXECUTION_READ_FILE message reads the data on the file and fails if the file contains data in invalid format - csv file on json dataset', async () => {
        const timestamp = new Date().getTime();

        nock('http://api.resourcewatch.org')
            .get('/csv')
            .times(4)
            .reply(200, fs.readFileSync(path.join(__dirname, 'sample-csv.csv')));

        // nock('http://api.resourcewatch.org')
        //     .head('/dataset')
        //     .times(4)
        //     .reply(200, {
        //         data: JSON.parse(fs.readFileSync(path.join(__dirname, 'dataset-list.json'))),
        //         links: {
        //             self: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
        //             first: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
        //             last: 'http://api.resourcewatch.org/v1/dataset?page[number]=150&page[size]=10',
        //             prev: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
        //             next: 'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10'
        //         },
        //         meta: { 'total-pages': 150, 'total-items': 1499, size: 10 }
        //     });

        const message = {
            id: '9745512f-9463-45c1-ba4d-99f2035b8414',
            type: 'EXECUTION_READ_FILE',
            taskId: 'efaa80ad-97e4-487b-83ea-a2d0022fc0da',
            fileUrl: 'http://api.resourcewatch.org/csv',
            dataPath: 'data',
            provider: 'json',
            index: 'index_ef7d64c631664053a0b7e221d84496a5_1574922887909',
            datasetId: `${timestamp}`
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            if (content.type === docImporterMessages.status.MESSAGE_TYPES.STATUS_ERROR) {
                content.should.have.property('id');
                content.should.have.property('taskId').and.equal(message.taskId);
                content.should.have.property('error').and.equal('Exceeded maximum number of attempts to process message of type "EXECUTION_READ_FILE". Error message: "Error processing JSON file from http://api.resourcewatch.org/csv. Error message: "Invalid JSON (Unexpected "p" at position 0 in state STOP)""');
            } else {
                throw new Error(`Unexpected message type "${content.type}"`);
            }

            await channel.ack(msg);

            expectedStatusQueueMessageCount -= 1;

            if (expectedStatusQueueMessageCount < 0) {
                throw new Error(`Unexpected message count - expectedStatusQueueMessageCount:${expectedStatusQueueMessageCount}`);
            }

            if (expectedStatusQueueMessageCount === 0) {
                resolve();
            }
        };

        return new Promise((resolve) => {
            channel.consume(config.get('queues.status'), validateStatusQueueMessages(resolve), { exclusive: true });
        });
    });

    afterEach(async () => {
        await channel.assertQueue(config.get('queues.executorTasks'));
        const executorQueueStatus = await channel.checkQueue(config.get('queues.executorTasks'));
        executorQueueStatus.messageCount.should.equal(0);

        await channel.assertQueue(config.get('queues.status'));
        const statusQueueStatus = await channel.checkQueue(config.get('queues.status'));
        statusQueueStatus.messageCount.should.equal(0);

        await channel.assertQueue(config.get('queues.data'));
        const dataQueueStatus = await channel.checkQueue(config.get('queues.data'));
        dataQueueStatus.messageCount.should.equal(0);

        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }

        await channel.close();
        channel = null;

        await rabbitmqConnection.close();
        rabbitmqConnection = null;
    });
});
