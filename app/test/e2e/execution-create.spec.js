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

describe('EXECUTION_CREATE handling process', () => {

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

        requester = await getTestServer();
    });

    beforeEach(async () => {
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

    it('Consume a EXECUTION_CREATE message and create a new task and EXECUTION_CREATE message (happy case)', async () => {
        const timestamp = new Date().getTime();

        nock(`http://${process.env.ELASTIC_URL}`)
            .put(new RegExp(`/index_${timestamp}_(\\w*)`), { mappings: { type: { properties: {} } } })
            .reply(200, { acknowledged: true, shards_acknowledged: true });


        nock(`http://${process.env.ELASTIC_URL}`)
            .put(new RegExp(`/index_${timestamp}_(\\w*)/_settings`), {
                index: {
                    refresh_interval: '-1',
                    number_of_replicas: 0
                }
            })
            .reply(200, { acknowledged: true });

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
            }, ['Server',
                'nginx/1.15.8',
                'Content-Type',
                'application/json; charset=utf-8',
                'cache',
                'dataset',
                'X-Response-Time',
                '38 ms',
                'Via',
                '1.1 google',
                'Content-Length',
                '9938',
                'Accept-Ranges',
                'bytes',
                'Date',
                'Wed, 06 Mar 2019 07:09:40 GMT',
                'Via',
                '1.1 varnish',
                'Age',
                '116',
                'Connection',
                'close',
                'X-Served-By',
                'cache-mad9420-MAD',
                'X-Cache',
                'HIT',
                'X-Cache-Hits',
                '1',
                'X-Timer',
                'S1551856181.890113,VS0,VE5',
                'Vary',
                'Origin, Accept-Encoding']);

        const message = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_CREATE',
            taskId: '1128cf58-4cd7-4eab-b2db-118584d945bf',
            datasetId: `${timestamp}`,
            fileUrl: 'http://api.resourcewatch.org/dataset',
            provider: 'json',
            legend: {
                date: [],
                region: [],
                country: [],
                nested: []
            },
            verified: false,
            dataPath: 'data',
            indexType: 'type',
            index: 'index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824'
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        // Give the code 3 seconds to do its thing
        await new Promise(resolve => setTimeout(resolve, 15000));

        const postExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        postExecutorTasksQueueStatus.messageCount.should.equal(0);
        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(3);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(1);

        const validateDataQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                switch (content.type) {

                    case docImporterMessages.data.MESSAGE_TYPES.DATA:
                        content.should.have.property('id');
                        content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
                    default:
                        throw new Error('Unexpected message type');

                }
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);
        };

        const validateStatusQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                switch (content.type) {

                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_CREATED:
                        content.should.have.property('id');
                        content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
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
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);
        };

        await channel.consume(config.get('queues.status'), validateStatusQueueMessages);
        await channel.consume(config.get('queues.data'), validateDataQueueMessages);

        process.on('unhandledRejection', (error) => {
            should.fail(error);
        });
    });

    afterEach(async () => {
        await channel.assertQueue(config.get('queues.executorTasks'));
        await channel.purgeQueue(config.get('queues.executorTasks'));
        const executorQueueStatus = await channel.checkQueue(config.get('queues.executorTasks'));
        executorQueueStatus.messageCount.should.equal(0);

        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });

    after(async () => {
        await channel.purgeQueue(config.get('queues.executorTasks'));
        await channel.purgeQueue(config.get('queues.status'));
        await channel.purgeQueue(config.get('queues.data'));

        rabbitmqConnection.close();
    });
});
