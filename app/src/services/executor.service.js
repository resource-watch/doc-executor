const logger = require('logger');
const StatusQueueService = require('services/status-queue.service');
const { execution } = require('doc-importer-messages');
const ImporterService = require('services/importer.service');
const elasticService = require('services/elastic.service');

const ExecutionMessages = execution.MESSAGE_TYPES;

const sleep = time => new Promise(resolve => setTimeout(resolve, time));

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
        logger.debug('Create task');
        logger.debug('Creating index');
        const index = `index_${msg.datasetId.replace(/-/g, '')}`;
        try {
            logger.debug('Deleting index ', index);
            await elasticService.deleteIndex(index);
        } catch (err) {}

        await elasticService.createIndex(index, msg.legend);
        await elasticService.deactivateIndex(index);
        msg.index = index;
        // Now send a STATUS_INDEX_CREATED to StatusQueue
        await StatusQueueService.sendIndexCreated(msg.taskId, index);
        logger.debug('Starting importing service');
        const importerService = new ImporterService(msg);
        await importerService.start();
        // ElasticService.readFile();
        // // Simulating open and read file
        // for (let i = 0; i < 10; i++) {
        //     logger.debug('Reading data');
        //     Emitting STATUS_READ_DATA events
        //     await StatusQueueService.sendReadData(msg.taskId);
        // }
        // File finished
        logger.debug('Sending read file message');
        await StatusQueueService.sendReadFile(msg.taskId);
    }

    static async concat(msg) {
        // The Index is already craeted when concatenating
        logger.debug('Starting importing service');
        const importerService = new ImporterService(msg);
        await importerService.start();
        logger.debug('Sending read file message');
        await StatusQueueService.sendReadFile(msg.taskId);
    }

    static async deleteQuery(msg) {
        logger.debug('Delete data of index with query ', msg.query);
        const elasticTaskId = elasticService.deleteQuery(msg.query);
        // Generate Performed Delete Query event
        await StatusQueueService.sendPerformedDeleteQuery(msg.taskId, elasticTaskId);
    }

    static async confirmDelete(msg) {
        logger.debug('Confirm Delete data with elastictaskid ', msg.elasticTaskId);
        const finished = elasticService.checkFinishTaskId(msg.elasticTaskId);
        if (!finished) {
            await sleep(2000);
            throw new Error('Task not finished');
        }
        // try {check elasticTask } catch (err) throw new Error
        // throwing an error here implies that the msg is going to
        // be "nacked"
        // set a timeout before throw the error
        // if not an error
        await StatusQueueService.sendFinishedDeleteQuery(msg.taskId);
    }

    static async deleteIndex(msg) {
        logger.debug('Deleting index', msg.index);
        await elasticService.deleteIndex(msg.index);
        await StatusQueueService.sendIndexDeleted(msg.taskId);
    }

    static async confirmImport(msg) {
        logger.debug('Confirming index', msg.index);
        await elasticService.activateIndex(msg.index);
        await StatusQueueService.sendImportConfirmed(msg.taskId);
    }

}

module.exports = ExecutorService;
