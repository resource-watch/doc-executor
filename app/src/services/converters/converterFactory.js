const logger = require('logger');
const CSVConverter = require('services/converters/csvConverter');
const JSONConverter = require('services/converters/jsonConverter');
const XMLConverter = require('services/converters/xmlConverter');
const ConverterNotSupported = require('errors/converterNotSupported');

class Converter {

    static getInstance(type, url, dataPath, verify) {
        logger.info(`Getting converter of type ${type} and dataPath ${dataPath} and url ${url}`);
        switch (type) {

            case 'csv':
                return new CSVConverter(url, verify);
            case 'tsv':
                return new CSVConverter(url, verify, '\t');
            case 'json':
                return new JSONConverter(url, dataPath, verify);
            case 'xml':
                return new XMLConverter(url, dataPath, verify);
            default:
                throw new ConverterNotSupported(400, `Converter to ${type} not supported`);

        }
    }

}

module.exports = Converter;
