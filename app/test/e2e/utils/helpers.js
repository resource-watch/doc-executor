const config = require('config');
const { Client } = require('@elastic/elasticsearch');

const elasticSearchConfig = {
    node: config.get('elasticsearch.host')
};

if (config.get('elasticsearch.user') && config.get('elasticsearch.password')) {
    elasticSearchConfig.auth = {
        username: config.get('elasticsearch.user'),
        password: config.get('elasticsearch.password')
    };
}

const createIndex = async (index, mappings) => {
    const body = {
        settings: {
            index: {
                number_of_shards: 1
            }
        },
        mappings: {
            properties: mappings
        }
    };

    const ESClient = new Client(elasticSearchConfig);

    const response = await ESClient.indices.create({
        index,
        body
    });

    return response.body;
};

const insertData = async (index, data) => {
    const ESClient = new Client(elasticSearchConfig);

    const body = data.flatMap((doc) => [{ index: { _index: index } }, doc]);

    return ESClient.bulk({ body, timeout: '90s', refresh: 'wait_for' });
};

const getIndexSettings = async (index) => {
    const ESClient = new Client(elasticSearchConfig);

    return ESClient.indices.get({
        index
    });
};

const getData = async (index) => {
    const ESClient = new Client(elasticSearchConfig);

    return ESClient.search({
        index,
        from: 0,
        size: 100
    });
};

const getIndices = async () => {
    const ESClient = new Client(elasticSearchConfig);

    return ESClient.cat.indices({
        format: 'json'
    });
};

const deleteTestIndices = async () => {
    const ESClient = new Client(elasticSearchConfig);

    const response = await ESClient.cat.indices({
        format: 'json'
    });

    const promises = response.body.map((index) => {
        if (index.index.startsWith('test_')) {
            return ESClient.indices.delete({
                index: index.index
            });
        }

        return new Promise(((resolve) => resolve()));
    });

    return Promise.all(promises);
};

const deleteIndex = async (index) => {
    const ESClient = new Client(elasticSearchConfig);

    return ESClient.indices.delete({
        index
    });
};

module.exports = {
    createIndex,
    deleteTestIndices,
    deleteIndex,
    insertData,
    getData,
    getIndexSettings,
    getIndices,
};
