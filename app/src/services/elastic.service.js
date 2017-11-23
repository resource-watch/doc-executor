const logger = require('logger');
const elasticsearch = require('elasticsearch');
const config = require('config');
const ElasticError = require('errors/elastic.error');

const elasticUrl = config.get('elastic.url');

class ElasticService {

    constructor() {
        this.client = new elasticsearch.Client({
            host: elasticUrl,
            log: 'error'
        });
        setInterval(() => {
            // logger.debug('Doing ping to elastic');
            this.client.ping({
                // ping usually has a 3000ms timeout
                requestTimeout: 10000
            }, function (error) {
                if (error) {
                    logger.error('elasticsearch cluster is down!');
                    process.exit(1);
                }
            });
        }, 3000);
    }

    async createIndex(datasetId, legend) {
        logger.debug('Creating index in elastic');
        const index = `index_${datasetId.replace(/-/g, '')}`;

        const body = {
            mappings: {
                [index]: {
                    properties: {

                    }
                }
            }
        };
        if (legend && legend.lat && legend.long) {
            logger.debug('Adding geo column');
            body.mappings[index].properties.the_geom = {
                type: 'geo_shape',
                tree: 'geohash',
                precision: '1m',
                points_only: true
            };
            body.mappings[index].properties.the_geom_point = {
                type: 'geo_point'
            };
        }
        return new Promise((resolve, reject) => {
            
            this.client.indices.create({ index, body }, (err, res) => {
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
                        number_of_replicas: 1
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
            this.client.indices.delete({ index }, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response);
            });
        });
    }

}

module.exports = new ElasticService();
