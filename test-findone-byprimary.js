import xk6_elasticsearch from 'k6/x/elasticsearch';

//ES 7.5.2
const client = xk6_elasticsearch.newBasicClient(['http://localhost:9200/']);
export default () => {
    // syntax :: client.findOne("<db>", "<scope>", "<keyspace>", "<docId>");
    var res = client.findOne("test", randomIntFromInterval(0, 99999).toString());

    console.log(res);
}

function randomIntFromInterval(min, max) { // min and max included
    return Math.floor(Math.random() * (max - min + 1) + min)
}
