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

describe('Full queue handling process', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        // Clear queues before staring the app
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

    it('Queueing a STATUS_READ_DATA message when the queue is full should cause the message to not be queued', async () => {
        const timestamp = new Date().getTime();

        const executorQueueMessage = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_READ_FILE',
            taskId: 'fc38cf58-4cd7-4eab-b2db-118584d945bf',
            datasetId: `${timestamp}`,
            fileUrl: 'http://api.resourcewatch.org/dataset',
            provider: 'json',
            legend: {},
            verified: false,
            dataPath: 'data',
            indexType: 'type',
            index: 'index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824'
        };

        const dataQueueMessage = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_APPEND',
            taskId: '1128cf58-4cd7-4eab-b2db-118584d945bf',
            data: [],
            index: 'index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824'
        };

        // eslint-disable-next-line no-plusplus
        for (let i = 0; i < config.get('messageQueueMaxSize'); i++) {
            await channel.sendToQueue(config.get('queues.data'), Buffer.from(JSON.stringify(dataQueueMessage)));
        }

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

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);
        const preDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        preDataQueueStatus.messageCount.should.equal(config.get('messageQueueMaxSize'));


        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(executorQueueMessage)));

        await new Promise(resolve => setTimeout(resolve, 5000));

        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(0);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(config.get('messageQueueMaxSize'));

        let expectedDataQueueMessageCount = config.get('messageQueueMaxSize');
        let expectedExecutionQueueMessageCount = 1;

        const validateExecutorQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                if (content.type === docImporterMessages.execution.MESSAGE_TYPES.EXECUTION_READ_FILE) {
                    content.should.have.property('id');
                    content.should.have.property('index').and.equal(executorQueueMessage.index);
                    content.should.have.property('taskId').and.equal(executorQueueMessage.taskId);
                } else {
                    throw new Error(`Unexpected message type: ${content.type}`);

                }
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);

            expectedExecutionQueueMessageCount -= 1;

            if (expectedExecutionQueueMessageCount === 0) {
                resolve();
            }
        };

        const validateDataQueueMessages = (resolve, reject) => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                if (content.type === dataQueueMessage.type) {
                    content.should.have.property('id');
                    content.should.have.property('index').and.equal(dataQueueMessage.index);
                    content.should.have.property('taskId').and.equal(dataQueueMessage.taskId);
                    content.should.have.property('data');
                } else {
                    throw new Error(`Unexpected message type: ${content.type}`);

                }
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);

            expectedDataQueueMessageCount -= 1;

            if (expectedDataQueueMessageCount < 0) {
                reject(new Error(`Unexpected message count - expectedDataQueueMessageCount:${expectedDataQueueMessageCount}`));
            }

            if (expectedDataQueueMessageCount === 0) {
                resolve();
            }
        };

        await new Promise((resolve, reject) => {
            channel.consume(config.get('queues.executorTasks'), validateExecutorQueueMessages(resolve, reject), { priority: 99999 });
        });

        return new Promise((resolve, reject) => {
            channel.consume(config.get('queues.data'), validateDataQueueMessages(resolve, reject), { exclusive: true });
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
