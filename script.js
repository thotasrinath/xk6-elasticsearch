import xk6_elasticsearch from 'k6/x/elasticsearch';

//ES 7.5.2
const client = xk6_elasticsearch.newBasicClient(['http://localhost:9200/']);

client.setBatchCount(2000)

export const options = {
    discardResponseBodies: true,
    scenarios: {
        contacts: {
            executor: 'per-vu-iterations',
            vus: 10,
            iterations: 400000,
            // maxDuration: '30s',
            gracefulStop: '5s',
        },
    },
};

export default () => {

    let doc = {
        correlationId: `test--couchbase`,
        title: 'Perf test experiment',
        url: 'example.com',
        locale: 'en',
        time: `${new Date(Date.now()).toISOString()}`
    };
    client.addDocumentToBatch("test", makeId(15), doc);
}

function makeId(length) {
    let result = '';
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    let counter = 0;
    while (counter < length) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
        counter += 1;
    }
    return result;
}

