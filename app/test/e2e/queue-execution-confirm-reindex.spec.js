/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
const sleep = require('sleep');

const { getTestServer } = require('./test-server');

const should = chai.should();

let requester;
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect(process.env.HOST_IP);

describe('EXECUTION_CONFIRM_REINDEX handling process', () => {

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

    it('Consume a EXECUTION_CONFIRM_REINDEX message should check that the ES reindex task is done and send STATUS_FINISHED_REINDEX message if task is completed (happy case)', async () => {

        const message = {
            id: 'e27d387a-dd78-43b4-aa06-37f2fd44ce81',
            type: 'EXECUTION_CONFIRM_REINDEX',
            taskId: '278eac45-19b1-46d5-ac75-1005795b2993',
            elasticTaskId: '123456'
        };

        nock(process.env.ELASTIC_URL)
            .get(`/_tasks/${message.elasticTaskId}`)
            .reply(200, {
                completed: true,
                task: {
                    node: 'r2VhXZtqS52PMl7IAVxWuA',
                    id: 94,
                    type: 'transport',
                    action: 'indices:data/write/reindex',
                    status: {
                        total: 0,
                        updated: 0,
                        created: 0,
                        deleted: 0,
                        batches: 0,
                        version_conflicts: 0,
                        noops: 0,
                        retries: {
                            bulk: 0,
                            search: 0
                        },
                        throttled_millis: 0,
                        requests_per_second: 0.0,
                        throttled_until_millis: 0
                    },
                    description: 'reindex from [index_acee7314d61f474881de5a1366b2a457_1593769478026] to [test1][_doc]',
                    start_time_in_millis: 1593770019758,
                    running_time_in_nanos: 5135029,
                    cancellable: true,
                    headers: {}
                }
            });


        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('type').and.equal(docImporterMessages.status.MESSAGE_TYPES.STATUS_FINISHED_REINDEX);
            content.should.have.property('id');
            content.should.have.property('taskId').and.equal(message.taskId);

            await channel.ack(msg);

            expectedStatusQueueMessageCount -= 1;

            if (expectedStatusQueueMessageCount === 0) {
                resolve();
            }
        };

        return new Promise((resolve) => {
            channel.consume(config.get('queues.status'), validateStatusQueueMessages(resolve), { exclusive: true });
        });

    });


    it('Consume a EXECUTION_CONFIRM_REINDEX message should check that the ES reindex task is done and send STATUS_ERROR message after the configured number of failed retries', async () => {

        const message = {
            id: 'e27d387a-dd78-43b4-aa06-37f2fd44ce81',
            type: 'EXECUTION_CONFIRM_REINDEX',
            taskId: '113eac45-19b1-46d5-ac75-1005795b2993',
            elasticTaskId: '234567'
        };

        nock(process.env.ELASTIC_URL)
            .get(`/_tasks/${message.elasticTaskId}`)
            .times(parseInt(config.get('messageRetries'), 10) + 1)
            .reply(200, {
                completed: false
            });


        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('type').and.equal(docImporterMessages.status.MESSAGE_TYPES.STATUS_ERROR);
            content.should.have.property('id');
            content.should.have.property('taskId').and.equal(message.taskId);
            content.should.have.property('error').and
                .equal('Exceeded maximum number of attempts to process message of type "EXECUTION_CONFIRM_REINDEX". Error message: "Reindex Elasticsearch task 234567 not finished"');

            await channel.ack(msg);

            expectedStatusQueueMessageCount -= 1;

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
            const pendingMocks = nock.pendingMocks();
            if (pendingMocks.length > 1) {
                throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
            }
        }

        await channel.close();
        channel = null;

        await rabbitmqConnection.close();
        rabbitmqConnection = null;
    });
});
