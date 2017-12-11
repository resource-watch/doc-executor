const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const { execution } = require('doc-importer-messages');
const ExecutorError = require('errors/executor.error');
const ExecutorService = require('services/executor.service');
const statusQueueService = require('services/status-queue.service');

const ExecutionMessages = execution.MESSAGE_TYPES;

const {
    EXECUTOR_TASK_QUEUE
} = require('app.constants');


class ExecutorQueueService {

    constructor() {
        this.q = EXECUTOR_TASK_QUEUE;
        logger.info(`Connecting to queue ${this.q}`);
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
        await this.channel.assertQueue(this.q, {
            durable: true
        });
        this.channel.prefetch(1);
        logger.info(` [*] Waiting for messages in ${this.q}`);
        this.channel.consume(this.q, this.consume.bind(this), {
            noAck: false
        });
    }

    async returnMsg(msg) {
        logger.info(`Sending message to ${this.q}`);
        try {
            // Sending to queue
            let count = msg.properties.headers['x-redelivered-count'] || 0;
            count += 1;
            this.channel.sendToQueue(this.q, msg.content, {
                headers: {
                    'x-redelivered-count': count
                }
            });
        } catch (err) {
            logger.error(`Error sending message to  ${this.q}`);
            throw err;
        }
    }

    async consume(msg) {
        let message = null;
        try {
            logger.debug('Message received', msg.content.toString());
            message = JSON.parse(msg.content.toString());
            logger.debug('message content', message);
            await ExecutorService.processMessage(message);
            this.channel.ack(msg);
            logger.info('Message processed successfully');
        } catch (err) {
            logger.error(err);
            this.channel.ack(msg);
            const retries = msg.properties.headers['x-redelivered-count'] || 0;
            if (retries < 10 || message.type === ExecutionMessages.EXECUTION_CONFIRM_DELETE || message.type === ExecutionMessages.EXECUTION_CONFIRM_REINDEX)  {
                this.returnMsg(msg);
            } else {
                await statusQueueService.sendErrorMessage(msg.taskId, 'Exceeded maximum number of attempts to process the message');
            }
        }

    }

}

module.exports = new ExecutorQueueService();
