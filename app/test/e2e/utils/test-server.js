const nock = require('nock');
const chai = require('chai');
const config = require('config');
const sleep = require('sleep');
const logger = require('logger');
const chaiHttp = require('chai-http');
const { Client } = require('@elastic/elasticsearch');

let requester;

chai.use(chaiHttp);

exports.getTestServer = async function getTestServer() {
    if (requester) {
        return requester;
    }

    const elasticUri = config.get('elasticsearch.host');

    nock(elasticUri, { allowUnmocked: true });

    const elasticSearchConfig = {
        node: elasticUri
    };

    if (config.get('elasticsearch.user') && config.get('elasticsearch.password')) {
        elasticSearchConfig.auth = {
            username: config.get('elasticsearch.user'),
            password: config.get('elasticsearch.password')
        };
    }

    this.elasticClient = new Client(elasticSearchConfig);

    let retries = 10;

    const pingES = () => new Promise((resolve, reject) => {
        this.elasticClient.ping({}, (error) => {
            if (error) {

                if (retries >= 0) {
                    retries--;
                    logger.error(`Elasticsearch cluster is down, attempt #${10 - retries} ... - ${error.message}`);
                    sleep.sleep(5);
                    resolve(pingES());
                } else {
                    logger.error(`Elasticsearch cluster is down, bailing! - ${error.message}`);
                    logger.error(error);
                    reject(error);
                }
            } else {
                const server = require('../../../src/app');
                requester = chai.request(server).keepOpen();

                resolve(requester);
            }
        });
    });

    await pingES();

    const server = require('../../../src/app');
    requester = chai.request(server).keepOpen();

    return requester;
};
