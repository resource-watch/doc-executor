'use strict';

const coRequest = require('co-request');
const request = require('request');
const fs = require('fs');
const logger = require('logger');
const Bluebird = require('bluebird');
const https = require('https');
const http = require('http');
const crypto = require('crypto');
const algorithm = 'sha256';

function humanFileSize(bytes, si) {
    var thresh = si ? 1000 : 1024;
    if(Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }
    var units = si ? ['kB','MB','GB','TB','PB','EB','ZB','YB'] : ['KiB','MiB','GiB','TiB','PiB','EiB','ZiB','YiB'];
    var u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while(Math.abs(bytes) >= thresh && u < units.length - 1);
    return bytes.toFixed(1)+' '+units[u];
}

let requestDownloadFile = function(url, path, verify) {
    
    return new Bluebird(function(resolve, reject){
        logger.debug('Sending request');
        try {
            let dlprogress = 0;
            let oldProgress = 0;
            var requestserver = null;
            if (url.trim().startsWith('https')) {
                requestserver = https.request(url);
            } else {
                requestserver = http.request(url);
            }
            requestserver.addListener('response', function (response) {
                var downloadfile = fs.createWriteStream(path, {'flags': 'a'});
                logger.info('File size: ' + humanFileSize(parseInt(response.headers['content-length'], 10)) );
                let shasum = null;
                if (verify) {
                    shasum = crypto.createHash(algorithm);
                }
                response.addListener('data', function (chunk) {
                    if (verify) {
                        shasum.update(chunk);
                    }
                    dlprogress += chunk.length;
                    downloadfile.write(chunk, {encoding: 'binary'});
                    if(dlprogress-oldProgress > 100*1024*1024){
                        logger.debug(humanFileSize(dlprogress)+' progress');
                        oldProgress = dlprogress;
                    }
                });
                response.addListener('end', function() {
                    downloadfile.end();
                    logger.info(humanFileSize(dlprogress)+' downloaded. Ended from server');
                    if (verify) {
                        const sha256 = shasum.digest('hex');
                        resolve(sha256);
                    } else {
                        resolve();
                    }                    
                    
                });
                response.on('error',function(e){
                    logger.error('Error downloading file', e);
                    reject(e);
                });

            });
            requestserver.end();
        } catch(err){
            logger.error(err);
            reject(err);
        }
    });

};

class DownloadService {

    static * checkIfExists(url){
        logger.info('Checking if the url exists');
        let result = yield coRequest.head(url);
        logger.debug('Headers ', result.headers['content-type'], result.statusCode);
        
        return result.statusCode === 200;
    }

    static * downloadFile(url, name, verify) {
        logger.debug('Downloading....');
        let path = '/tmp/' + name;
        logger.debug('Temporal path', path, '. Downloading');
        let sha256 = yield requestDownloadFile(url, path, verify);
        logger.debug('Download file!!!');
        return {
            path,
            sha256
        };
    }


}

module.exports = DownloadService;
