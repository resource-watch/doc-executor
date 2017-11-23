const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const docImporter = require('doc-importer-messages');
const StatusQueueService = require('services/status-queue.service');
const {
    promisify
} = require('util');
const {
    DATA_QUEUE
} = require('app.constants');


class DataQueueService {

    constructor() {
        logger.info(`Connecting to queue ${DATA_QUEUE}`);
        try {
            this.init().then(() => {
                logger.info('Connected');
            }, (err) => {
                logger.error(err);
                process.exit(1);
            });
        } catch (err) {
            logger.error(err);
        }
    }

    async init() {
        const conn = await amqp.connect(config.get('rabbitmq.url'));
        this.channel = await conn.createConfirmChannel();
    }

    async sendMessage(msg) {
        return new Promise((resolve, reject) => {
            const interval = setInterval(async () => {
                try {
                    logger.info('Sending message', msg);
                    const data = await this.channel.assertQueue(DATA_QUEUE, {
                        durable: true
                    });
                    if (data.count > 100) {
                        throw new Error('Full queue');
                    }
                    this.channel.sendToQueue(DATA_QUEUE, Buffer.from(JSON.stringify(msg)));
                    clearInterval(interval);
                    resolve();
                } catch (err) {
                    logger.error('Error sending message (try again in 2 second)', err);
                }
            }, 2000);
        });
    }

    async sendDataMessage(taskId, data) {
        logger.debug('Sending data message');
        await this.sendMessage({
            taskId,
            data
        });
        await StatusQueueService.sendReadData(taskId);
    }


}

module.exports = new DataQueueService();
