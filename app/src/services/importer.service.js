const logger = require('logger');
const ConverterFactory = require('services/converters/converterFactory');
const _ = require('lodash');
const dataQueueService = require('services/data-queue.service');
const statusQueueService = require('services/status-queue.service');

const CONTAIN_SPACES = /\s/g;
const IS_NUMBER = /^\d+$/;

function isJSONObject(value) {
    if (isNaN(value) && /^[\],:{}\s]*$/.test(value.replace(/\\["\\\/bfnrtu]/g, '@').replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
        return true;
    }
    return false;
}

function convertPointToGeoJSON(lat, long) {
    return {
        type: 'point',
        coordinates: [
            long,
            lat
        ]
    };
}

class ImporterService {

    constructor(msg) {
        this.body = [];
        this.provider = msg.provider;
        this.url = msg.fileUrl;
        this.dataPath = msg.dataPath;
        this.verify = msg.verify;
        this.legend = msg.legend;
        this.taskId = msg.taskId;
        this.index = msg.index;
        this.indexObj = {
            index: {
                _index: msg.index,
                _type: msg.index
            }
        };
        this.numPacks = 0;
    }

    async start() {
        return new Promise(async(resolve, reject) => {
            try {
                logger.debug('Starting read file');
                const converter = ConverterFactory.getInstance(this.provider, this.url, this.dataPath, this.verify);
                
                await converter.init();
                const stream = converter.serialize();
                logger.debug('Starting process file');
                stream.on('data', this.processRow.bind(this, stream, reject));
                stream.on('error', (err) => {
                    logger.error('Error reading file', err);
                    reject(err);
                });
                stream.on('end', () => {
                    if (this.numPacks === 0 && this.body && this.body.length === 0) {
                        statusQueueService.sendErrorMessage(this.taskId, 'File empty');
                        resolve();
                        return;
                    }
                    logger.debug('Finishing reading file');
                    if (this.body && this.body.length > 0) {
                        // send last rows to data queue
                        logger.debug('Saving data');

                        dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                            this.body = [];
                            logger.debug('Pack saved successfully, num:');
                            resolve();
                        }, function (err) {
                            logger.error('Error saving ', err);
                            reject(err);
                        });
                    } else {
                        resolve();
                    }
                });
            } catch (err) {
                logger.error(err);
                reject(err);
            }
        });


    }

    async processRow(stream, reject, data) {
        stream.pause();

        if (_.isPlainObject(data)) {

            try {
                _.forEach(data, function (value, key) {
                    let newKey = key;
                    try {
                        if (newKey !== '_id') {
                            if (CONTAIN_SPACES.test(key)) {
                                delete data[key];
                                newKey = key.replace(CONTAIN_SPACES, '_');
                            }
                            if (IS_NUMBER.test(newKey)) {
                                if (data[newKey]) {
                                    delete data[key];
                                }
                                newKey = `col_${newKey}`;
                            }
                            if (!(value instanceof Object) && isJSONObject(value)) {
                                try {
                                    data[newKey] = JSON.parse(value);
                                } catch (e) {
                                    data[newKey] = value;
                                }
                            } else if (!isNaN(value)) {
                                data[newKey] = Number(value);
                            } else {
                                data[newKey] = value;
                            }
                        } else {
                            delete data[newKey];
                        }
                    } catch (e) {
                        logger.error(e);
                        throw new Error(e);
                    }
                });

                if (this.legend && (this.legend.lat || this.legend.long)) {
                    data.the_geom = convertPointToGeoJSON(data[this.legend.lat], data[this.legend.long]);
                    data.the_geom_point = {
                        lat: data[this.legend.lat],
                        lon: data[this.legend.long]
                    };
                }
                logger.trace('Adding new row');
                this.body.push(this.indexObj);
                this.body.push(data);

            } catch (e) {
                // continue
                logger.error('Error generating', e);
            }

        } else {
            logger.error('Data and/or options have no headers specified');
        }

        if (this.body && this.body.length >= 20000) {
            logger.debug('Sending data');

            dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                this.body = [];
                stream.resume();
                logger.debug('Pack saved successfully, num:', this.numPacks++);
            }, function (err) {
                logger.error('Error saving ', err);
                stream.end();
                reject(err);
            });
        } else {
            stream.resume();
        }
    }

}

module.exports = ImporterService;
