const logger = require('logger');
const { Client } = require('@elastic/elasticsearch');
const config = require('config');
const ElasticError = require('errors/elastic.error');

const elasticUrl = config.get('elastic.url');

class ElasticService {

    constructor() {
        // const extendAPI = {
        //     explain(opts, cb) {
        //         const call = (err, data) => {
        //             if (data) {
        //                 try {
        //                     cb(err, data);
        //                     return;
        //                 } catch (e) {
        //                     cb(e, null);
        //                     return;
        //                 }
        //             }
        //             cb(err, data);
        //
        //         };
        //         logger.debug('Doing explain with', opts);
        //         this.transport.request({
        //             method: 'POST',
        //             path: encodeURI('/_sql/_explain'),
        //             body: opts.sql
        //         }, call);
        //     }
        // };
        // Client.apis.extendAPI = Object.assign({}, Client.apis['5.6'], extendAPI);

        this.client = new Client({
            node: `http://${elasticUrl}`
        });
        this.client.ping({
        }, (error) => {
            if (error) {
                logger.error('Elasticsearch cluster is down!');
                process.exit(1);
            }
        });
    }

    async createIndex(index, legend) {
        logger.debug(`Creating index ${index} in elastic`);
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

        return new Promise((resolve, reject) => {

            this.client.indices.create({
                index,
                body
            }, (err) => {
                if (err) {
                    reject(new ElasticError(err));
                    return;
                }
                resolve(index);
            });
        });
    }

    async activateIndex(index) {
        return new Promise((resolve, reject) => {
            const options = {
                index,
                body: {
                    index: {
                        refresh_interval: '1s',
                        number_of_replicas: 2
                    }
                }
            };
            this.client.indices.putSettings(options, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response);
            });
        });
    }

    async deactivateIndex(index) {
        return new Promise((resolve, reject) => {
            const options = {
                index,
                body: {
                    index: {
                        refresh_interval: '-1',
                        number_of_replicas: 0
                    }
                }
            };
            this.client.indices.putSettings(options, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response);
            });
        });
    }

    async deleteIndex(index) {
        return new Promise((resolve, reject) => {
            this.client.indices.delete({
                index
            }, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response);
            });
        });
    }

    async reindex(sourceIndex, destIndex) {
        return new Promise((resolve, reject) => {
            this.client.reindex({
                waitForCompletion: false,
                body: {
                    source: {
                        index: sourceIndex
                    },
                    dest: {
                        index: destIndex
                    }
                }
            }, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response.task);
            });
        });
    }


    async deleteQuery(index, sql) {
        return new Promise((resolve, reject) => {
            logger.debug('Doing explain of query');
            this.client.explain({
                sql: sql.replace(/delete/gi, 'select * ')
            }, (err, resultQueryElastic) => {
                if (err) {
                    logger.error(err);
                    if (err.statusCode === 500) {
                        reject(new ElasticError(err.message));
                    }
                    reject(err);
                    return;
                }
                delete resultQueryElastic.from;
                delete resultQueryElastic.size;
                logger.debug('Doing query');

                this.client.deleteByQuery({
                    index,
                    body: resultQueryElastic,
                    waitForCompletion: false
                }, (error, response) => {
                    if (error) {
                        reject(error);
                        return;
                    }
                    resolve(response.task);
                });
            });
        });
    }

    async checkFinishTaskId(taskId) {
        return new Promise((resolve, reject) => {
            this.client.tasks.get({
                taskId
            }, (err, data) => {
                if (err) {
                    reject(err);
                    return;
                }
                if (data && data.body && ((data.body.length > 0 && data.body[0].completed) || data.body.completed)) {
                    logger.debug('Task completed');
                    resolve(true);
                    return;
                }
                resolve(false);
            });
        });
    }

}

module.exports = new ElasticService();
