import xk6_elasticsearch from 'k6/x/elasticsearch';
import { SharedArray } from 'k6/data';
/*
{
    "query": {
        "bool": {
            "must": [],
            "must_not": [],
            "should": [
                {
                    "match": {
                        "text_sent_150": "pseudocolumellar"
                    }
                },
                {
                    "match": {
                        "text_sent_300": "pseudocolumellar"
                    }
                },
                {
                    "match": {
                        "text_sent_450": "pseudocolumellar"
                    }
                }
            ]
        }
    },
    "from": 0,
    "size": 10,
    "sort": [],
    "aggs": {}
}
*/

const client = xk6_elasticsearch.newBasicClient(['http://ec2-18-138-235-11.ap-southeast-1.compute.amazonaws.com:9200/']);

const data = new SharedArray('words', function () {
    // All heavy work (opening and processing big files for example) should be done inside here.
    // This way it will happen only once and the result will be shared between all VUs, saving time and memory.
    const f = JSON.parse(open('./words_dictionary.json'));
    return Object.keys(f); // f must be an array
});

function sentenceGenerator(words, size) {
    var sentence = '';
    for (var i = 0; i < size; i++) {
        sentence += words[Math.floor(Math.random() * words.length)] + ' ';
    }

    return sentence;
}

export default () => {

    var sentlen = sentenceGenerator(data, 5);

    var query = '"bool":{"must":[],"must_not":[],"should":[{"match":{"text_sent_150":"' + sentlen + '"}},{"match":{"text_sent_300":"' + sentlen + '"}},{"match":{"text_sent_450":"' + sentlen + '"}}]}';

    var res = client.find("test", query, 2);

    console.log(res);
}
