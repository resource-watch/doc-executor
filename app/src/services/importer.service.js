const logger = require('logger');
const ConverterFactory = require('services/converters/converterFactory');
const _ = require('lodash');
const fs = require('fs');
const dataQueueService = require('services/data-queue.service');
const statusQueueService = require('services/status-queue.service');
const StamperyService = require('services/stamperyService');
const config = require('config');
const InvalidFileFormat = require('errors/invalidFileFormat');

const CONTAIN_SPACES = /\s/g;
const IS_NUMBER = /^\d+$/;

function isJSONObject(value) {
    // eslint-disable-next-line no-restricted-globals,max-len,no-useless-escape
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
        this.datasetId = msg.datasetId;
        this.provider = msg.provider;
        this.url = msg.fileUrl;
        this.dataPath = msg.dataPath;
        this.verify = msg.verified;
        this.legend = msg.legend;
        this.taskId = msg.taskId;
        this.index = msg.index;
        this.type = msg.indexType;
        this.indexObj = {
            index: {
                _index: msg.index,
                _type: msg.indexType
            }
        };
        this.numPacks = 0;
    }

    async start() {
        return new Promise(async (resolve, reject) => {
            let converter;
            try {
                logger.debug(`Starting read file ${this.url}`);
                converter = ConverterFactory.getInstance(this.provider, this.url, this.dataPath, this.verify);
                // StamperyService
                if (this.verify) {
                    const blockchain = await StamperyService.stamp(this.datasetId, converter.sha256, converter.filePath, this.type);
                    statusQueueService.sendBlockChainGenerated(this.taskId, blockchain);
                }
                await converter.init();
                const stream = converter.serialize();
                logger.debug(`Starting process file ${this.url}`);
                stream.on('data', this.processRow.bind(this, stream, reject));
                stream.on('error', this.handleError.bind(this, reject));
                stream.on('end', () => {
                    if (this.numPacks === 0 && this.body && this.body.length === 0) {
                        let errorMessage = `File ${this.url} is empty.`;

                        if (fs.existsSync(converter.filePath)) {
                            const stats = fs.statSync(converter.filePath);
                            const fileSizeInBytes = stats.size;

                            errorMessage = `${errorMessage} Size in bytes of ${converter.filePath}: ${fileSizeInBytes}`;
                        } else {
                            errorMessage = `${errorMessage} Temporary file could not be found at ${converter.filePath}`;
                        }


                        statusQueueService.sendErrorMessage(this.taskId, errorMessage);
                        resolve();
                    }
                    logger.debug(`Finishing reading file ${this.url}`);
                    if (this.body && this.body.length > 0) {
                        // send last rows to data queue
                        dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                            this.body = [];
                            logger.debug('Pack saved successfully, num:', ++this.numPacks);
                            converter.close();
                            resolve();
                        }, (err) => {
                            logger.error('Error saving ', err);
                            converter.close();
                            reject(err);
                        });
                    } else {
                        logger.warn(`File from ${this.url} read but empty body found`);
                        converter.close();
                        resolve();
                    }
                });
            } catch (err) {
                if (converter) {
                    converter.close();
                }
                logger.error(err);
                reject(err);
            }
        });
    }

    handleError(reject, error) {
        logger.error('Error reading file', error);

        // JSON parsing error
        if (error.message.startsWith('Invalid JSON')) {
            reject(new InvalidFileFormat(`Error processing JSON file from ${this.url}. Error message: "${error.message}"`));
        }

        // CSV parsing error
        if (error.message.startsWith('Parse Error: ')) {
            reject(new InvalidFileFormat(`Error processing CSV file from ${this.url}. Error message: "${error.message}"`));
        }

        reject(error);
    }

    async processRow(stream, reject, data) {
        try {
            stream.pause();
            try {
                if (_.isPlainObject(data)) {

                    _.forEach(data, (value, key) => {
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
                                if (newKey.indexOf('.') >= 0) {
                                    if (data[newKey]) {
                                        delete data[newKey];
                                    }
                                    newKey = newKey.replace(/\./g, '_');
                                }
                                if (!(value instanceof Object) && isJSONObject(value)) {
                                    try {
                                        data[newKey] = JSON.parse(value);
                                    } catch (e) {
                                        data[newKey] = value;
                                    }
                                    // isNaN is NOT equivalent to Number.isNaN
                                    // eslint-disable-next-line no-restricted-globals
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
                        if (data[this.legend.lat] && data[this.legend.long]) {
                            data.the_geom = convertPointToGeoJSON(data[this.legend.lat], data[this.legend.long]);
                            data.the_geom_point = {
                                lat: data[this.legend.lat],
                                lon: data[this.legend.long]
                            };
                        }
                    }
                    logger.debug(`Adding new row from file ${this.url}`);
                    this.body.push(this.indexObj);
                    this.body.push(data);
                } else {
                    logger.error('Data and/or options have no headers specified');
                }
            } catch (e) {
                // continue
                logger.error('Error generating', e);
            }

            if (this.body && this.body.length >= config.get('elementPerPackage')) {
                logger.debug(`Sending data for file ${this.url}`);

                dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                    this.body = [];
                    stream.resume();
                    logger.debug(`Pack saved successfully for file ${this.url}, num:`, ++this.numPacks);
                }, (err) => {
                    logger.error(`Error sending data message for file ${this.url}:`, err);
                    stream.end();
                    reject(err);
                });
            } else {
                stream.resume();
            }
        } catch (err) {
            logger.error('Error saving', err);
        }
    }

}

module.exports = ImporterService;
