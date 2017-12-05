const logger = require('logger');

logger.debug('Initializing doc-executor');
require('services/executor-queue.service');
require('services/status-queue.service');
