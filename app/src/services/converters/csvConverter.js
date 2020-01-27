const logger = require('logger');
const csv = require('@fast-csv/parse');
const fs = require('fs');
const UrlNotFound = require('errors/urlNotFound');
const randomstring = require('randomstring');

const DownloadService = require('services/downloadService');
const FileNotFound = require('errors/fileNotFound');

class CSVConverter {

    constructor(url, verify = false, delimiter = ',') {
        this.checkURL = new RegExp('^(https?:\\/\\/)?' // protocol
            + '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.?)+[a-z]{2,}|' // domain name
            + '((\\d{1,3}\\.){3}\\d{1,3}))' // OR ip (v4) address
            + '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' // port and path
            + '(\\?[:;&a-z\\d%_.~+=-]*)?' // query string
            + '(\\#[-a-z\\d_]*)?$', 'i');
        this.delimiter = delimiter;
        this.url = url;
        this.verify = verify;
    }

    async init() {
        if (this.checkURL.test(this.url)) {
            logger.debug('[CSVConverter] Is a url. Downloading file');
            const exists = await DownloadService.checkIfExists(this.url);
            if (!exists) {
                throw new UrlNotFound(400, `Url not found: ${this.url}`);
            }
            let name = randomstring.generate();
            if (this.delimiter === '\t') {
                name += '.tsv';
            } else {
                name += '.csv';
            }
            const result = await DownloadService.downloadFile(this.url, name, this.verify);
            this.filePath = result.path;
            this.sha256 = result.sha256;
        } else {
            this.filePath = this.url;
        }
    }

    serialize() {
        if (!fs.existsSync(this.filePath)) {
            throw new FileNotFound(`File ${this.filePath} does not exist`);
        } else {
            const stats = fs.statSync(this.filePath);
            const fileSizeInBytes = stats.size;
            logger.debug(`[CSVConverter] Opening stream to file ${this.filePath}. File size in bytes: ${fileSizeInBytes}`);
            if (fileSizeInBytes < 100000) {
                const fileContent = fs.readFileSync(this.filePath, 'utf8');
                logger.debug(`[CSVConverter] Content of file ${this.filePath}: ${fileContent}`);
            }

        }

        return csv.parseFile(this.filePath, {
            headers: true,
            delimiter: this.delimiter,
            discardUnmappedColumns: true
        });
    }

    close() {
        if (!fs.existsSync(this.filePath)) {
            throw new FileNotFound(`File ${this.filePath} does not exist`);
        }

        const stats = fs.statSync(this.filePath);
        const fileSizeInBytes = stats.size;

        logger.info('[CSVConverter] Removing file', this.filePath);

        if (fileSizeInBytes > 100000) {
            if (fs.existsSync(this.filePath) && !this.verify) {
                fs.unlinkSync(this.filePath);
            }
        }
    }

}

module.exports = CSVConverter;
