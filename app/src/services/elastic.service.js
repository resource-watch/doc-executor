/* eslint-disable no-underscore-dangle */
const logger = require('logger');
const { Client } = require('@elastic/elasticsearch');
const config = require('config');
const sleep = require('sleep');

const elasticUrl = config.get('elasticsearch.host');

class ElasticService {

    constructor() {
        logger.info(`Connecting to Elasticsearch at ${elasticUrl}`);

        const elasticSearchConfig = {
            node: elasticUrl
        };

        if (config.get('elasticsearch.user') && config.get('elasticsearch.password')) {
            elasticSearchConfig.auth = {
                username: config.get('elasticsearch.user'),
                password: config.get('elasticsearch.password')
            };
        }

        this.elasticClient = new Client(elasticSearchConfig);

        let retries = 10;

        const pingES = () => {
            this.elasticClient.ping({}, (error) => {
                if (error) {
                    if (retries >= 0) {
                        retries--;
                        logger.error(`Elasticsearch cluster is down, attempt #${10 - retries} ... - ${error.message}`);
                        sleep.sleep(5);
                        pingES();
                    } else {
                        logger.error(`Elasticsearch cluster is down, bailing! - ${error.message}`);
                        logger.error(error);
                        throw new Error(error);
                    }
                } else {
                    setInterval(() => {
                        this.elasticClient.ping({}, (pingError) => {
                            if (pingError) {
                                logger.error(`Elasticsearch cluster is down! - ${pingError.message}`);
                                process.exit(1);
                            }
                        });
                    }, 3000);
                }
            });
        };

        pingES();

        this.elasticClient.extend('opendistro.explain', ({ makeRequest, ConfigurationError }) => function explain(params, options = {}) {
            const {
                body,
                index,
                method,
                ...querystring
            } = params;

            // params validation
            if (body == null) {
                throw new ConfigurationError('Missing required parameter: body');
            }

            // build request object
            const request = {
                method: method || 'POST',
                path: `/_opendistro/_sql/_explain`,
                body,
                querystring
            };

            // build request options object
            const requestOptions = {
                ignore: options.ignore || null,
                requestTimeout: options.requestTimeout || null,
                maxRetries: options.maxRetries || null,
                asStream: options.asStream || false,
                headers: options.headers || null
            };

            return makeRequest(request, requestOptions);
        });
    }

    async createIndex(index, legend) {
        logger.debug(`Creating index ${index}  in elastic`);
        const body = {
            settings: {
                index: {
                    number_of_shards: 3
                }
            },
            mappings: {
                properties: {}
            }
        };
        if (legend && legend.lat && legend.long) {
            logger.debug('Adding geo column');
            body.mappings.properties.the_geom = {
                type: 'geo_shape',
                tree: 'geohash',
                precision: '1m',
                points_only: true
            };
            body.mappings.properties.the_geom_point = {
                type: 'geo_point'
            };
        }

        if (legend && legend.nested) {
            for (let i = 0, { length } = legend.nested; i < length; i++) {
                body.mappings.properties[legend.nested[i]] = {
                    type: 'nested',
                    include_in_parent: true
                };
            }
        }

        const fieldTypeList = [
            'integer',
            'short',
            'byte',
            'double',
            'float',
            'half_float',
            'scaled_float',
            'boolean',
            'binary',
            'text',
            'keyword'
        ];

        fieldTypeList.forEach((fieldType) => {
            if (legend && legend[fieldType]) {
                for (let i = 0, { length } = legend[fieldType]; i < length; i++) {
                    body.mappings.properties[legend[fieldType][i]] = {
                        type: fieldType
                    };
                }
            }
        });

        const response = await this.elasticClient.indices.create({
            index,
            body
        });

        return response.body;
    }

    async activateIndex(index) {
        const options = {
            index,
            body: {
                index: {
                    refresh_interval: '1s',
                    number_of_replicas: 2
                }
            }
        };

        const response = await this.elasticClient.indices.putSettings(options);

        return response.body;
    }

    async deactivateIndex(index) {
        const options = {
            index,
            body: {
                index: {
                    refresh_interval: '-1',
                    number_of_replicas: 0
                }
            }
        };
        const response = await this.elasticClient.indices.putSettings(options);

        return response.body;
    }

    async deleteIndex(index) {
        const response = await this.elasticClient.indices.delete({
            index
        });

        return response.body;
    }

    async reindex(sourceIndex, destIndex) {
        const response = await this.elasticClient.reindex({
            waitForCompletion: false,
            body: {
                source: {
                    index: sourceIndex
                },
                dest: {
                    index: destIndex
                }
            }
        });

        return response.body.task;
    }

    async deleteQuery(index, sql) {
        logger.debug('Doing explain of query');
        const resultQueryElastic = await this.elasticClient.opendistro.explain({
            body: {
                query: sql.replace(/delete/gi, 'select * ')
            }
        });
        delete resultQueryElastic.body.from;
        delete resultQueryElastic.body.size;
        logger.debug('Doing query');

        const response = await this.elasticClient.deleteByQuery({
            index,
            body: resultQueryElastic.body,
            waitForCompletion: false
        });

        return response.body.task;
    }

    async checkFinishTaskId(taskId) {
        const response = await this.elasticClient.tasks.get({
            taskId
        });

        if (response && response.body && response.body.completed) {
            logger.debug('Task completed');
            return true;
        }

        return false;
    }

}

module.exports = new ElasticService();
