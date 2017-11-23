const logger = require('logger');
const ExecutionMessages = require('doc-importer-messages').execution.MESSAGE_TYPES;


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

            default:
            logger.error('Message not supported');

        }
    }

}

module.exports = ExecutorService;
