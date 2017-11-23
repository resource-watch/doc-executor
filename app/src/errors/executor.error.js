class ExecutorError extends Error {

    constructor(message) {
        super(message);
        this.name = 'ExecutorError';
        this.message = message;
    }

}

module.exports = ExecutorError;
