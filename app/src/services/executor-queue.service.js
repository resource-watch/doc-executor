/* eslint-disable no-plusplus */
const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const sleep = require('sleep');
const { execution } = require('rw-doc-importer-messages');
const ExecutorService = require('services/executor.service');
const statusQueueService = require('services/status-queue.service');
const ReindexingInProgress = require('errors/reindexingInProgress');

const ExecutionMessages = execution.MESSAGE_TYPES;

let retries = 10;

class ExecutorQueueService {

    constructor() {
        this.q = config.get('queues.executorTasks');
        logger.info(`[Executor Queue] Connecting to queue ${this.q}`);
        try {
            this.init().then(() => {
                logger.info('[Executor Queue] Connected');
            }, (err) => {
                this.retryConnection(err);
            });
        } catch (err) {
            logger.error(err);
        }
    }

    retryConnection(err) {
        if (retries >= 0) {
            retries--;
            logger.error(`Failed to connect to RabbitMQ uri ${config.get('rabbitmq.url')} with error message "${err.message}", retrying...`);
            sleep.sleep(2);
            this.init().then(() => {
                logger.info('Connected');
            }, (initError) => {
                this.retryConnection(initError);
            });
        } else {
            logger.error(err);
            process.exit(1);
        }
    }

    async init() {
        const conn = await amqp.connect(config.get('rabbitmq.url'));
        this.channel = await conn.createConfirmChannel();
        await this.channel.assertQueue(this.q, {
            durable: true
        });
        this.channel.prefetch(1);
        logger.info(`[Executor Queue] [*] Waiting for messages in ${this.q}`);
        this.channel.consume(this.q, this.consume.bind(this), {
            noAck: false
        });
    }

    async returnMsg(message) {
        logger.info(`[Executor Queue] Returning message to ${this.q}`);
        try {
            // Sending to queue
            let count = message.properties.headers['x-redelivered-count'] || 0;
            count += 1;
            this.channel.sendToQueue(this.q, message.content, {
                headers: {
                    'x-redelivered-count': count
                }
            });
        } catch (err) {
            logger.error(`[Executor Queue] Error sending message to ${this.q}`);
            throw err;
        }
    }

    async consume(msg) {
        let message = null;
        try {
            logger.debug('[Executor Queue] Message received', msg.content.toString());
            message = JSON.parse(msg.content.toString());
            logger.debug('message content', message);
            await ExecutorService.processMessage(message, this);
            // this.channel.ack(msg);
            logger.info('[Executor Queue] Message processed successfully', msg.content.toString());
        } catch (err) {
            logger.error(err);

            let delayMultiplier = 1;

            if (err instanceof ReindexingInProgress && message.fileCount) {
                delayMultiplier = message.fileCount || 1;
            }

            const messageRetries = msg.properties.headers['x-redelivered-count'] || 0;
            if (messageRetries < parseInt(config.get('messageRetries'), 10) || message.type === ExecutionMessages.EXECUTION_CONFIRM_DELETE) {
                logger.warn(`Failed to process message with type ${message.type}, requeuing after ${(config.get('retryDelay') / 1000) * delayMultiplier * (messageRetries + 1)} seconds`);
                setTimeout(this.returnMsg.bind(this), config.get('retryDelay') * (messageRetries + 1), msg);
            } else {
                await statusQueueService.sendErrorMessage(
                    message.taskId,
                    `Exceeded maximum number of attempts to process message of type "${message.type}". Error message: "${err.message}"`
                );
            }
        } finally {
            this.channel.ack(msg);
        }

    }

    async sendMessage(msg) {
        logger.info(`[Executor Queue] Sending message to ${this.q}`, msg);
        try {
            // Sending to queue
            await this.channel.sendToQueue(this.q, Buffer.from(JSON.stringify(msg)));
        } catch (err) {
            logger.error(`[Executor Queue] Error sending message to ${this.q}`);
            throw err;
        }
    }

}

module.exports = new ExecutorQueueService();
