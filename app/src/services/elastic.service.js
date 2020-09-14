/* eslint-disable no-underscore-dangle */
const logger = require('logger');
const { Client } = require('@elastic/elasticsearch');
const config = require('config');

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

        this.client = new Client(elasticSearchConfig);

        this.client.extend('opendistro.explain', ({ makeRequest, ConfigurationError }) => function explain(params, options = {}) {
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

        logger.debug(`Pinging Elasticsearch server at ${elasticUrl}`);

        this.client.ping({}, (error) => {
            if (error) {
                logger.error('Elasticsearch cluster is down!');
                process.exit(1);
            }
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
                _doc: {
                    properties: {}
                }
            }
        };
        if (legend && legend.lat && legend.long) {
            logger.debug('Adding geo column');
            body.mappings._doc.properties.the_geom = {
                type: 'geo_shape',
                tree: 'geohash',
                precision: '1m',
                points_only: true
            };
            body.mappings._doc.properties.the_geom_point = {
                type: 'geo_point'
            };
        }

        if (legend && legend.nested) {
            for (let i = 0, { length } = legend.nested; i < length; i++) {
                body.mappings._doc.properties[legend.nested[i]] = {
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
                    body.mappings._doc.properties[legend[fieldType][i]] = {
                        type: fieldType
                    };
                }
            }
        });

        const response = await this.client.indices.create({
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

        const response = await this.client.indices.putSettings(options);

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
        const response = await this.client.indices.putSettings(options);

        return response.body;
    }

    async deleteIndex(index) {
        const response = await this.client.indices.delete({
            index
        });

        return response.body;
    }

    async reindex(sourceIndex, destIndex) {
        const response = await this.client.reindex({
            waitForCompletion: false,
            body: {
                source: {
                    index: sourceIndex,
                    type: 'type',
                },
                dest: {
                    index: destIndex,
                    type: '_doc',
                }
            }
        });

        return response.body.task;
    }


    async deleteQuery(index, sql) {
        logger.debug('Doing explain of query');
        const resultQueryElastic = await this.client.opendistro.explain({
            body: {
                query: sql.replace(/delete/gi, 'select * ')
            }
        });
        delete resultQueryElastic.body.from;
        delete resultQueryElastic.body.size;
        logger.debug('Doing query');

        const response = await this.client.deleteByQuery({
            index,
            body: resultQueryElastic.body,
            waitForCompletion: false
        });

        return response.body.task;
    }

    async checkFinishTaskId(taskId) {
        const response = await this.client.tasks.get({
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
