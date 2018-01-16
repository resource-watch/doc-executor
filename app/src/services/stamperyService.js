const logger = require('logger');
const Stampery = require('stampery');
const config = require('config');
const S3Service = require('services/s3Service');

class StamperyService {

    constructor() {
        this.stampery = new Stampery(config.get('stampery'));
    }

    async stamp(datasetId, sha256, path, type) {
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
            const stampValue = await promise;
            const url = await S3Service.upload(datasetId, type, path);
            const blockchain = {
                hash: sha256,
                id: stampValue.id,
                time: stampValue.time,
                backupUrl: url
            };
            return blockchain;
        } catch (err) {
            throw new Error(`Error registering in blockchain: ${err.message}`);
        }
    }

}

module.exports = new StamperyService();
