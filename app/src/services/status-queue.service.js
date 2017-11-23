const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const docImporter = require('doc-importer-messages');
const {
    promisify
} = require('util');
const {
    STATUS_QUEUE
} = require('app.constants');


class StatusQueueService {

    constructor() {
        logger.info(`Connecting to queue ${STATUS_QUEUE}`);
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
        this.channel.assertQueueAsync = promisify(this.channel.assertQueue);
    }

    async sendMessage(msg) {
        return new Promise((resolve, reject) => {
            let numTries = 0;
            const interval = setInterval(async () => {
                try {
                    numTries++;
                    logger.info('Sending message', msg);
                    const data = await this.channel.assertQueueAsync(STATUS_QUEUE, {
                        durable: true
                    });
                    this.channel.sendToQueue(STATUS_QUEUE, Buffer.from(JSON.stringify(msg)));
                    clearInterval(interval);
                    resolve();
                } catch (err) {
                    logger.error('Error sending message (try again in 2 second)', err);
                    if (numTries > 3) {
                        clearInterval(interval);
                        reject(err);
                    }
                }
            }, 2000);
        });
    }

    async sendIndexCreated(taskId) {
        logger.debug('Sending index created message of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_INDEX_CREATED, {
            taskId
        }));
    }

    async sendWriteCorrect(taskId) {
        logger.debug('Sending write correct message of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_WRITE, {
            taskId
        }));
    }

}

module.exports = new StatusQueueService();
