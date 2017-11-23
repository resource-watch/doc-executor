const logger = require('logger');
const ConverterFactory = require('services/converters/converterFactory');
const _ = require('underscore');
const CONTAIN_SPACES = /\s/g;
const IS_NUMBER = /^\d+$/;

function isJSONObject(value) {
    if (isNaN(value) && /^[\],:{}\s]*$/.test(value.replace(/\\["\\\/bfnrtu]/g, '@').replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
        return true;
    } 
    return false;
}

convertPointToGeoJSON(lat, long) {
    return {
        type: 'point',
        coordinates: [
            long,
            lat
        ]
    };
}

class ImporterService {

    static async start(msg) {
        return new Promise((resolve, reject) => {
            logger.debug('Starting read file');
            const converter = ConverterFactory.getInstance(msg.provider, msg.url, msg.dataPath, msg.verify);
            const stream = converter.serialize();
            stream.on('data', ImporterService.processRow.bind(this, stream, reject, msg.legend));
            stream.on('error', (err) => {
                logger.error('Error reading file', err);
                reject(err);
            });
            stream.on('end', () => {
                logger.debug('Finishing reading file');
                // send last rows to data queue
                resolve();
            });
        });
        
    }

    static async processRow(stream, reject, legend, data) {
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
                
                if (legend && (legend.lat || legend.long)) {
                    data.the_geom = convertPointToGeoJSON(data[legend.lat], data[legend.long]);
                    data.the_geom_point = {
                        lat: data[legend.lat],
                        lon: data[legend.long]
                    };
                }
                request.body.push(index);
                request.body.push(data);
                
            } catch (e) {
                //continue
                logger.error('Error generating', e);
            }

        } else {
            //stream.end();
            logger.error('Data and/or options have no headers specified');
            //reject(new Error('Data and/or options have no headers specified'));
        }

        if (request.body && request.body.length >= 80000) {
            logger.debug('Saving');
            this.saveData(request).then(function () {
                request.body = [];
                stream.resume();
                i++;
                logger.debug('Pack saved successfully, num:', i);
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
