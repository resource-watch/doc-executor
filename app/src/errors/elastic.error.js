class ElasticError extends Error {

    constructor(message) {
        super(message);
        this.name = 'ElasticError';
        this.message = message;
    }

}

module.exports = ElasticError;
