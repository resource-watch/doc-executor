const requestPromise = require('request-promise');
const fs = require('fs');
const logger = require('logger');
const Bluebird = require('bluebird');
const { http, https } = require('follow-redirects');
const crypto = require('crypto');

const algorithm = 'sha256';

function humanFileSize(bytes, si) {
    const thresh = si ? 1000 : 1024;
    if (Math.abs(bytes) < thresh) {
        return `${bytes} B`;
    }
    const units = si ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    do {
        // eslint-disable-next-line no-param-reassign
        bytes /= thresh;
        ++u;
    } while (Math.abs(bytes) >= thresh && u < units.length - 1);
    return `${bytes.toFixed(1)} ${units[u]}`;
}

const requestDownloadFile = (url, path, verify) => (

    new Bluebird((resolve, reject) => {
        logger.debug(`[DownloadService] Sending request to ${url}`);
        try {
            let dlprogress = 0;
            let oldProgress = 0;
            let requestServer = null;
            if (url.trim().startsWith('https')) {
                requestServer = https.request(url);
            } else {
                requestServer = http.request(url);
            }
            requestServer.addListener('response', (response) => {
                if (response.statusCode >= 400) {
                    logger.warn(`[DownloadService] File ${url} request failed to load, response code ${response.statusCode}`);
                }

                const downloadfile = fs.createWriteStream(path, {
                    flags: 'a'
                });
                logger.info(`[DownloadService] File ${url} size: ${humanFileSize(parseInt(response.headers['content-length'], 10))}`);
                let shasum = null;
                if (verify) {
                    shasum = crypto.createHash(algorithm);
                }
                response.addListener('data', (chunk) => {
                    logger.debug(`[DownloadService] Appending ${chunk.length} bytes to file ${path} from url ${url}.`);

                    if (verify) {
                        shasum.update(chunk);
                    }
                    dlprogress += chunk.length;
                    downloadfile.write(chunk, {
                        encoding: 'binary'
                    });
                    if (dlprogress - oldProgress > 100 * 1024 * 1024) {
                        logger.debug(`${humanFileSize(dlprogress)} download progress for file ${url}`);
                        oldProgress = dlprogress;
                    }
                });
                response.addListener('end', () => {
                    downloadfile.end(() => {
                        logger.info(`[DownloadService] ${humanFileSize(dlprogress)} downloaded for file ${url}. Ended from server`);
                        const stats = fs.statSync(path);
                        const fileSizeInBytes = stats.size;

                        logger.debug(`[DownloadService] File ${path} from url ${url} has size in bytes: ${fileSizeInBytes}`);

                        if (verify) {
                            const sha256 = shasum.digest('hex');
                            resolve(sha256);
                        } else {
                            resolve();
                        }
                    });

                });
                response.on('error', (e) => {
                    logger.error(`[DownloadService] Error downloading file ${url}`, e);
                    reject(e);
                });

            });
            requestServer.end();
        } catch (err) {
            logger.error(err);
            reject(err);
        }
    })

);

class DownloadService {

    static async checkIfExists(url) {
        logger.info(`[DownloadService] Checking if the url ${url} exists`);
        try {
            const result = await requestPromise.head({
                url,
                simple: false,
                resolveWithFullResponse: true
            });
            logger.debug('[DownloadService] Headers:', result.headers['content-type'], result.statusCode);

            return result.statusCode === 200;
        } catch (err) {
            logger.error(err);
            return false;
        }
    }

    static async downloadFile(url, name, verify) {
        const path = `/tmp/${name}`;
        logger.debug(`[DownloadService] Starting download of url ${url} to temporary path ${path}. Downloading...`);
        const sha256 = await requestDownloadFile(url, path, verify);
        logger.debug(`[DownloadService] File ${url} downloaded successfully.`);
        return {
            path,
            sha256
        };
    }

}

module.exports = DownloadService;
