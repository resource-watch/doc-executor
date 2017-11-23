const logger = require('logger');
const Stampery = require('stampery');
const config = require('config');
const S3Service = require('services/s3Service');
const ctRegisterMicroservice = require('ct-register-microservice-node');

class StamperyService {
    constructor() {
        this.stampery = new Stampery(config.get('stampery'));
        ctRegisterMicroservice.init({
            token: process.env.CT_TOKEN,
            ctUrl: process.env.CT_URL,
            logger
        });
    }

    * updateBlockChain(id, sha256, idStamp, time, url) {
        logger.debug('Updating dataset');
        let options = {
            uri: '/dataset/' + id,
            body: {
                blockchain: {
                    hash: sha256,
                    id: idStamp,
                    time,
                    backupUrl: url
                }
            },
            method: 'PATCH',
            json: true
        };
         try {
            let result = yield ctRegisterMicroservice.requestToMicroservice(options);
        } catch (e) {
            logger.error(e);
            throw new Error('Error to updating dataset');
        }
    }

    
    * stamp(datasetId, sha256, path, type) {
        logger.debug('Doing stamp with sha256 ', sha256);
        try {
            const promise = new Promise((resolve, reject) => {
                this.stampery.stamp(sha256, (err, stamp) => {
                    if (err) {
                        reject(err);
                        return;
                    }

                    resolve(stamp);
                });
            });
            const stampValue = yield promise;
            let url = yield S3Service.upload(datasetId, type, path);
            yield this.updateBlockChain(datasetId, sha256, stampValue.id, stampValue.time, url);
        } catch(err) {
            throw new Error('Error registering in blockchain: ' + err.message);
        }
    }
}

module.exports = new StamperyService();
