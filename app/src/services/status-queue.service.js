const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const docImporter = require('doc-importer-messages');
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
    }

    async sendMessage(msg) {
        return new Promise((resolve, reject) => {
            let numTries = 0;
            const interval = setInterval(async () => {
                try {
                    numTries++;
                    logger.info('Sending message', msg);
                    const data = await this.channel.assertQueue(STATUS_QUEUE, {
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

    async sendIndexCreated(taskId, index) {
        logger.debug('Sending index created message of taskId', taskId, 'and index', index);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_INDEX_CREATED, {
            taskId,
            index
        }));
    }

    async sendReadData(taskId) {
        logger.debug('Sending Read data of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_READ_DATA, {
            taskId
        }));
    }

    async sendReadFile(taskId) {
        logger.debug('Sending Read File of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_READ_FILE, {
            taskId
        }));
    }

    async sendImportConfirmed(taskId) {
        logger.debug('Sending Read File of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_IMPORT_CONFIRMED, {
            taskId
        }));
    }

    async sendIndexDeleted(taskId) {
        logger.debug('Sending index deleted of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_INDEX_DELETED, {
            taskId
        }));
    }

    async sendPerformedDeleteQuery(taskId, elasticTaskId) {
        logger.debug('Sending Perform delete query of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_PERFORMED_DELETE_QUERY, {
            taskId,
            elasticTaskId
        }));
    }

    async sendPerformedReindex(taskId, elasticTaskId) {
        logger.debug('Sending Perform reindex of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_PERFORMED_REINDEX, {
            taskId,
            elasticTaskId
        }));
    }

    async sendFinishedDeleteQuery(taskId) {
        logger.debug('Sending finished delete query of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_FINISHED_DELETE_QUERY, {
            taskId
        }));
    }

    async sendFinishedReindex(taskId) {
        logger.debug('Sending finished reindex of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_FINISHED_REINDEX, {
            taskId
        }));
    }

    async sendErrorMessage(taskId, error) {
        logger.debug('Sending error message of taskId', taskId);
        await this.sendMessage(docImporter.status.createMessage(docImporter.status.MESSAGE_TYPES.STATUS_ERROR, {
            taskId,
            error
        }));
    }

}

module.exports = new StatusQueueService();
