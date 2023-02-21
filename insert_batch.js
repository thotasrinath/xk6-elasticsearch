import xk6_elasticsearch from 'k6/x/elasticsearch';

//ES 7.5.2
const client = xk6_elasticsearch.newBasicClient(['http://localhost:9200/']);

const batchsize = 50;

export default () => {

    var docobjs = {}

    for (var i = 0; i < batchsize; i++) {
        docobjs[makeId(15)] = getRecord();
    }

    client.addBatchDocuments("test", docobjs);
}

function getRecord() {
    return {
        correlationId: `test--couchbase`,
        title: 'Perf test experiment',
        url: 'example.com',
        locale: 'en',
        time: `${new Date(Date.now()).toISOString()}`
    };


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


