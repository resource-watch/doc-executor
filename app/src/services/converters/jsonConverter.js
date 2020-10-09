const logger = require('logger');
const JSONStream = require('JSONStream');
const fs = require('fs');
const randomstring = require('randomstring');
const DownloadService = require('services/downloadService');
const FileNotFound = require('errors/fileNotFound');

const isValidHttpUrl = (url) => {
    try {
        const parsedUrl = new URL(url);
        return ['http', 'https'].map((x) => `${x.toLowerCase()}:`).includes(parsedUrl.protocol);
    } catch (err) {
        return false;
    }
};

class JSONConverter {

    constructor(url, dataPath, verify) {
        logger.debug(`Creating jsonConverter with url ${url} and dataPath ${dataPath}`);
        this.dataPath = dataPath ? `${dataPath}.*` : '*';
        this.url = url;
        this.verify = verify;
    }

    async init() {
        if (isValidHttpUrl(this.url)) {
            logger.debug('Is a url. Downloading file in url ', this.url);
            const result = await DownloadService.downloadFile(this.url, `${randomstring.generate()}.json`, this.verify);
            this.filePath = result.path;
            this.sha256 = result.sha256;
            logger.debug('Temporal path ', this.filePath);
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {
        if (!fs.existsSync(this.filePath)) {
            throw new FileNotFound(`File ${this.filePath} does not exist`);
        }
        const readStream = fs.createReadStream(this.filePath)
            .pipe(JSONStream.parse(this.dataPath));

        return readStream;
    }

    close() {
        if (!fs.existsSync(this.filePath)) {
            throw new FileNotFound(`File ${this.filePath} does not exist`);
        }
        logger.info('Removing file', this.filePath);
        if (fs.existsSync(this.filePath) && !this.verify) {
            fs.unlinkSync(this.filePath);
        }
    }

}

module.exports = JSONConverter;
