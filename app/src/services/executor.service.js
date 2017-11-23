const logger = require('logger');
const StatusQueueService = require('services/status-queue.service');
const { execution } = require('doc-importer-messages');
const ExecutionMessages = execution.MESSAGE_TYPES;

class ExecutorService {

    static async processMessage(msg) {
        logger.debug('Processing message', msg);
        switch (msg.type) {

        case ExecutionMessages.EXECUTION_CREATE:
            await ExecutorService.create(msg);
            break;

        case ExecutionMessages.EXECUTION_CONCAT:
            await ExecutorService.concat(msg);
            break;

        case ExecutionMessages.EXECUTION_DELETE:
            await ExecutorService.deleteQuery(msg);
            break;

        case ExecutionMessages.EXECUTION_CONFIRM_DELETE:
            await ExecutorService.confirmDelete(msg);
            break;

        case ExecutionMessages.EXECUTION_DELETE_INDEX:
            await ExecutorService.deleteIndex(msg);
            break;

        case ExecutionMessages.EXECUTION_CONFIRM_IMPORT:
            await ExecutorService.confirmImport(msg);
            break;

        default:
            logger.error('Message not supported');

        }
    }

    static async create(msg) {
        // Create the index
        // ElasticService.createIndex(msg.datasetId);
        // Now send a STATUS_INDEX_CREATED to StatusQueue
        await StatusQueueService.sendIndexCreated(msg.taskId);
        // ElasticService.readFile();
        // Simulating open and read file
        for (let i = 0; i < 10; i++) {
            logger.debug('Reading data');
            // Emitting STATUS_READ_DATA events
        }
    }

    static async concat(msg) {
        // The Index is already craeted when concatenating
        const index = msg.index;
        // ElasticService.readFile();
        // Simulating open and read file
        for (let i = 0; i < 10; i++) {
            logger.debug('a');
            // Emitting STATUS_READ_DATA events
        }
    }


}

module.exports = ExecutorService;
