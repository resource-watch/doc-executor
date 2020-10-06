/* eslint-disable no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
const chaiMatch = require('chai-match');
const sleep = require('sleep');

const { deleteTestIndices, getIndexSettings, deleteIndex } = require('./utils/helpers');
const { getTestServer } = require('./utils/test-server');

chai.use(chaiMatch);
chai.should();

let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('EXECUTION_CREATE_INDEX handling process', () => {

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

    it('Consume a EXECUTION_CREATE_INDEX message and create a new task and STATUS_INDEX_CREATED, STATUS_READ_DATA and STATUS_READ_FILE messages (happy case)', async () => {
        const timestamp = new Date().getTime();

        const message = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_CREATE_INDEX',
            taskId: '1128cf58-4cd7-4eab-b2db-118584d945b1',
            datasetId: `${timestamp}`,
            legend: {}
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            let indexSettings;
            switch (content.type) {

                case docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_CREATED:
                    content.should.have.property('id');
                    content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                    content.should.have.property('taskId').and.equal(message.taskId);

                    indexSettings = await getIndexSettings(content.index);

                    indexSettings.body[content.index].settings.index.refresh_interval.should.equal('-1');
                    indexSettings.body[content.index].settings.index.number_of_shards.should.equal('3');
                    indexSettings.body[content.index].settings.index.number_of_replicas.should.equal('0');

                    await deleteIndex(content.index);
                    break;
                default:
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

    it('Consume a EXECUTION_CREATE_INDEX message and create a new task and STATUS_INDEX_CREATED message (happy case)', async () => {
        const timestamp = new Date().getTime();

        const message = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_CREATE_INDEX',
            taskId: '1128cf58-4cd7-4eab-b2db-118584d945b2',
            datasetId: `${timestamp}`,
            legend: {}
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            let indexSettings;
            switch (content.type) {

                case docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_CREATED:
                    content.should.have.property('id');
                    content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                    content.should.have.property('taskId').and.equal(message.taskId);

                    indexSettings = await getIndexSettings(content.index);

                    indexSettings.body[content.index].settings.index.refresh_interval.should.equal('-1');
                    indexSettings.body[content.index].settings.index.number_of_shards.should.equal('3');
                    indexSettings.body[content.index].settings.index.number_of_replicas.should.equal('0');

                    await deleteIndex(content.index);
                    break;
                default:
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

    it('Consume a EXECUTION_CREATE_INDEX message with custom mappings and create a new task and STATUS_INDEX_CREATED message (happy case)', async () => {
        const timestamp = new Date().getTime();

        const message = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_CREATE_INDEX',
            taskId: '1128cf58-4cd7-4eab-b2db-118584d945bf',
            datasetId: `${timestamp}`,
            legend: {
                string: [
                    'iso', 'global_land_cover', 'tsc', 'erosion', 'wdpa', 'plantations',
                    'river_basin', 'ecozone', 'water_stress', 'rspo', 'idn_land_cover',
                    'mex_forest_zoning', 'per_forest_concession', 'bra_biomes'
                ],
                integer: ['adm1', 'adm2', 'threshold_2000', 'ifl', 'year_data.year'],
                boolean: [
                    'primary_forest', 'idn_primary_forest', 'biodiversity_significance',
                    'biodiversity_intactness', 'aze.year', 'urban_watershed', 'mangroves_1996',
                    'mangroves_2016', 'endemic_bird_area', 'tiger_cl', 'landmark', 'land_right',
                    'kba', 'mining', 'idn_mys_peatlands', 'oil_palm', 'idn_forest_moratorium',
                    'mex_protected_areas', 'mex_pes', 'per_production_forest', 'per_protected_area',
                    'wood_fiber', 'resource_right', 'managed_forests', 'oil_gas'
                ],
                double: [
                    'total_area', 'total_gain', 'total_biomass', 'total_co2', 'mean_biomass_per_ha',
                    'total_mangrove_biomass', 'total_mangrove_co2', 'mean_mangrove_biomass_per_ha',
                    'year_data.area_loss', 'year_data.biomass_loss', 'year_data.carbon_emissions',
                    'year_data.mangrove_biomass_loss', 'year_data.mangrove_carbon_emissions'
                ]
            }
        };

        const mappings = {
            properties: {
                adm1: {
                    type: 'integer'
                },
                adm2: {
                    type: 'integer'
                },
                aze: {
                    properties: {
                        year: {
                            type: 'boolean'
                        }
                    }
                },
                biodiversity_intactness: {
                    type: 'boolean'
                },
                biodiversity_significance: {
                    type: 'boolean'
                },
                endemic_bird_area: {
                    type: 'boolean'
                },
                idn_forest_moratorium: {
                    type: 'boolean'
                },
                idn_mys_peatlands: {
                    type: 'boolean'
                },
                idn_primary_forest: {
                    type: 'boolean'
                },
                ifl: {
                    type: 'integer'
                },
                kba: {
                    type: 'boolean'
                },
                land_right: {
                    type: 'boolean'
                },
                landmark: {
                    type: 'boolean'
                },
                managed_forests: {
                    type: 'boolean'
                },
                mangroves_1996: {
                    type: 'boolean'
                },
                mangroves_2016: {
                    type: 'boolean'
                },
                mean_biomass_per_ha: {
                    type: 'double'
                },
                mean_mangrove_biomass_per_ha: {
                    type: 'double'
                },
                mex_pes: {
                    type: 'boolean'
                },
                mex_protected_areas: {
                    type: 'boolean'
                },
                mining: {
                    type: 'boolean'
                },
                oil_gas: {
                    type: 'boolean'
                },
                oil_palm: {
                    type: 'boolean'
                },
                per_production_forest: {
                    type: 'boolean'
                },
                per_protected_area: {
                    type: 'boolean'
                },
                primary_forest: {
                    type: 'boolean'
                },
                resource_right: {
                    type: 'boolean'
                },
                threshold_2000: {
                    type: 'integer'
                },
                tiger_cl: {
                    type: 'boolean'
                },
                total_area: {
                    type: 'double'
                },
                total_biomass: {
                    type: 'double'
                },
                total_co2: {
                    type: 'double'
                },
                total_gain: {
                    type: 'double'
                },
                total_mangrove_biomass: {
                    type: 'double'
                },
                total_mangrove_co2: {
                    type: 'double'
                },
                urban_watershed: {
                    type: 'boolean'
                },
                wood_fiber: {
                    type: 'boolean'
                },
                year_data: {
                    properties: {
                        area_loss: {
                            type: 'double'
                        },
                        biomass_loss: {
                            type: 'double'
                        },
                        carbon_emissions: {
                            type: 'double'
                        },
                        mangrove_biomass_loss: {
                            type: 'double'
                        },
                        mangrove_carbon_emissions: {
                            type: 'double'
                        },
                        year: {
                            type: 'integer'
                        }
                    }
                }
            }
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());
            let indexSettings;
            switch (content.type) {

                case docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_CREATED:
                    content.should.have.property('id');
                    content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                    content.should.have.property('taskId').and.equal(message.taskId);

                    indexSettings = await getIndexSettings(content.index);

                    indexSettings.body[content.index].settings.index.refresh_interval.should.equal('-1');
                    indexSettings.body[content.index].settings.index.number_of_shards.should.equal('3');
                    indexSettings.body[content.index].settings.index.number_of_replicas.should.equal('0');
                    indexSettings.body[content.index].mappings.should.deep.equal(mappings);

                    await deleteIndex(content.index);
                    break;
                default:
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
