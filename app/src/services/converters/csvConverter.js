const logger = require('logger');
const csv = require('fast-csv');
const fs = require('fs');
const UrlNotFound = require('errors/urlNotFound');
const randomstring = require('randomstring');
const DownloadService = require('services/downloadService');
const FileNotFound = require('errors/fileNotFound');

class CSVConverter {
    constructor(url, verify = false, delimiter = ',') {
        this.checkURL = new RegExp('^(https?:\\/\\/)?' + // protocol
            '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.?)+[a-z]{2,}|' + // domain name
            '((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
            '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // port and path
            '(\\?[;&a-z\\d%_.~+=-]*)?' + // query string
            '(\\#[-a-z\\d_]*)?$', 'i');
        this.delimiter = delimiter;
        this.url = url;
        this.verify = verify;
    }

    * init() {
        if (this.checkURL.test(this.url))  {
            logger.debug('Is a url. Downloading file');
            const exists = yield DownloadService.checkIfExists(this.url);
            if (!exists) {
                throw new UrlNotFound(400, 'Url not found');
            }
            let name = randomstring.generate();
            if (this.delimiter === '\t') {
                name += '.tsv';
            } else {
                name += '.csv';
            }
            const result = yield DownloadService.downloadFile(this.url, name, this.verify);
            this.filePath = result.path;
            this.sha256 = result.sha256;
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {
        if (!fs.existsSync(this.filePath)) {
            throw new FileNotFound(`File ${this.filePath} does not exist`);
        }
        const readStream = csv.fromPath(this.filePath, {
            headers: true,
            delimiter: this.delimiter,
            discardUnmappedColumns: true
        });
        readStream.on('end', () => {
            logger.info('Removing file', this.filePath);
                if (fs.existsSync(this.filePath) && !this.verify) {
                    fs.unlinkSync(this.filePath);
                }
        });

        return readStream;
    }
}

module.exports = CSVConverter;
