const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const ExecutorError = require('errors/executor.error');
const ExecutorService = require('services/executor.service');


const {
    EXECUTOR_TASK_QUEUE
} = require('app.constants');


class DataQueueService {

    constructor() {
        logger.info(`Connecting to queue ${EXECUTOR_TASK_QUEUE}`);
        amqp.connect(config.get('rabbitmq.url'), (err, conn) => {
            if (err) {
                logger.error(err);
                process.exit(1);
            }
            conn.createChannel((err, ch) => {
                if (err) {
                    logger.error(err);
                    process.exit(1);
                }
                const q = EXECUTOR_TASK_QUEUE;
                this.channel = ch;
                ch.assertQueue(q, {
                    durable: true,
                    maxLength: 10
                });
                ch.prefetch(1);

                logger.info(` [*] Waiting for messages in ${q}`);
                ch.consume(q, this.consume.bind(this), {
                    noAck: false
                });
            });
        });
    }

    async consume(msg) {
        try {
            logger.debug('Message received', msg.content.toString());
            const message = JSON.parse(msg.content.toString());
            logger.debug('message content', message);
            await ExecutorService.processMessage(message);
            this.channel.ack(msg);
            logger.info('Message processed successfully');
        } catch (err) {
            logger.error(err);
            if (err instanceof ExecutorError) {
                logger.error('Error processing message', err);
                this.channel.nack(msg);
                return;
            }
            const retries = msg.fields.deliveryTag;
            if (retries < 10) {
                this.channel.nack(msg);
            } else {
                this.channel.ack(msg);
            }
        }

    }

}

module.exports = new DataQueueService();
