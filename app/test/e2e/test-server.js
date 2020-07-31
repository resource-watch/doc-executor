const nock = require('nock');
const chai = require('chai');
const config = require('config');
const chaiHttp = require('chai-http');

let requester;

chai.use(chaiHttp);

exports.getTestServer = function getTestServer() {
    if (requester) {
        return requester;
    }

    const elasticUri = process.env.ELASTIC_URI || config.get('elastic.url');

    nock(elasticUri)
        .head('/')
        .times(999999)
        .reply(200);

    const server = require('../../src/app');
    requester = chai.request(server).keepOpen();

    return requester;
};
