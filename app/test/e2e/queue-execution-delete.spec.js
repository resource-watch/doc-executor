/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
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

    it('Consume a EXECUTION_DELETE message, starts an Elasticsearch task to delete data and issues a STATUS_PERFORMED_DELETE_QUERY message (happy case)', async () => {
        const message = {
            id: '3051e9b8-30dc-424d-a4f7-d4f77bf08688',
            type: 'EXECUTION_DELETE',
            taskId: '1fe7839b-5e64-4301-9389-24d43ca6279b',
            query: `DELETE FROM index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926 WHERE 1 = 1`,
            index: `index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926`
        };

        nock(`http://${process.env.ELASTIC_URL}`)
            .post(`/_sql/_explain`, 'select *  FROM index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926 WHERE 1 = 1')
            .reply(200, {
                from: 0,
                size: 200,
                query: {
                    bool: {
                        filter: [
                            {
                                bool: {
                                    must: [
                                        {
                                            script: {
                                                script: {
                                                    inline: '1 == 1',
                                                    lang: 'painless'
                                                },
                                                boost: 1.0
                                            }
                                        }
                                    ],
                                    disable_coord: false,
                                    adjust_pure_negative: true,
                                    boost: 1.0
                                }
                            }
                        ],
                        disable_coord: false,
                        adjust_pure_negative: true,
                        boost: 1.0
                    }
                }
            });

        nock(`http://${process.env.ELASTIC_URL}`)
            .post(`/index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926/_delete_by_query?wait_for_completion=false`, {
                query: {
                    bool: {
                        filter: [{
                            bool: {
                                must: [{
                                    script: {
                                        script: {
                                            inline: '1 == 1',
                                            lang: 'painless'
                                        },
                                        boost: 1
                                    }
                                }],
                                disable_coord: false,
                                adjust_pure_negative: true,
                                boost: 1
                            }
                        }],
                        disable_coord: false,
                        adjust_pure_negative: true,
                        boost: 1
                    }
                }
            })
            .reply(200, {
                task: 'aBvJmOVzQzelHHefeBKrgg:78'
            });

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = resolve => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                if (content.type === docImporterMessages.status.MESSAGE_TYPES.STATUS_PERFORMED_DELETE_QUERY) {
                    content.should.have.property('id');
                    content.should.have.property('taskId').and.equal(message.taskId);
                    content.should.have.property('lastCheckedDate');
                    content.should.have.property('elasticTaskId');
                } else {
                    throw new Error(`Unexpected message type: ${content.type}`);
                }
            } catch (err) {
                throw err;
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
