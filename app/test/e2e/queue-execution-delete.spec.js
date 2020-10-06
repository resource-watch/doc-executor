/* eslint-disable no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
const chaiMatch = require('chai-match');
const sleep = require('sleep');

const {
    createIndex, deleteTestIndices, insertData, getData
} = require('./utils/helpers');
const { getTestServer } = require('./utils/test-server');

chai.use(chaiMatch);
chai.should();

let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect(host => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('EXECUTION_DELETE handling process', () => {

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

        await getTestServer();

        await deleteTestIndices();
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

    it('Consume a EXECUTION_DELETE message, starts an Elasticsearch task to delete data and issues a STATUS_PERFORMED_DELETE_QUERY message (happy case)', async () => {
        const message = {
            id: '3051e9b8-30dc-424d-a4f7-d4f77bf08688',
            type: 'EXECUTION_DELETE',
            taskId: '1fe7839b-5e64-4301-9389-24d43ca6279b',
            query: `DELETE FROM test_index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824 WHERE 1 = 1`,
            index: `test_index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824`
        };

        await createIndex(message.index);

        await insertData(
            message.index,
            [{
                field1: 'value',
                field2: 1234
            }]
        );

        const preTestData = await getData(message.index);
        preTestData.body.hits.hits.should.have.length(1);

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            if (content.type === docImporterMessages.status.MESSAGE_TYPES.STATUS_PERFORMED_DELETE_QUERY) {
                content.should.have.property('id');
                content.should.have.property('taskId').and.equal(message.taskId);
                content.should.have.property('lastCheckedDate');
                content.should.have.property('elasticTaskId');

                const postTestData = await getData(message.index);
                postTestData.body.hits.hits.should.have.length(0);
            } else {
                throw new Error(`Unexpected message type: ${content.type}`);
            }

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

    it('Consume a EXECUTION_DELETE message with an Elasticsearch error should.... ', async () => {
        const message = {
            id: '3051e9b8-30dc-424d-a4f7-d4f77bf08688',
            type: 'EXECUTION_DELETE',
            taskId: '1fe7839b-5e64-4301-9389-24d43ca6279b',
            query: `DELETE FROM test_index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824 WHERE 1 = 1`,
            index: `test_index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824`
        };

        nock(process.env.ELASTIC_URL, { allowUnmocked: true })
            .post(`/_opendistro/_sql/_explain`, { query: 'select *  FROM test_index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824 WHERE 1 = 1' })
            .reply(500, {
                error: {
                    reason: 'Some Elasticsearch error',
                    details: 'Some Elasticsearch error',
                    type: 'SomeElasticsearchException'
                },
                status: 500
            });

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
                content.should.have.property('error').and.contain('SomeElasticsearchException');
            } else {
                throw new Error(`Unexpected message type: ${content.type}`);
            }

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
        await deleteTestIndices();

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
