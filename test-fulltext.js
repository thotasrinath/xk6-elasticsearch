import xk6_elasticsearch from 'k6/x/elasticsearch';
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

const client = xk6_elasticsearch.newBasicClient(['http://172.17.0.2:9200/']);
export default () => {

    var startDate = randomDate(new Date(2000, 0, 1), new Date(2022, 0, 1), 0, 24);

    var endDate = randomDate(startDate, new Date(2022, 0, 1), 0, 24);


    var query = '"bool":{"must":[],"must_not":[],"should":[{"match":{"text_sent_150":"pseudocolumellar"}},{"match":{"text_sent_300":"pseudocolumellar"}},{"match":{"text_sent_450":"pseudocolumellar"}}]}';

    var res = client.find("test", query, 2);

    console.log(res);
}

function randomDate(start, end, startHour, endHour) {
    var date = new Date(+start + Math.random() * (end - start));
    var hour = startHour + Math.random() * (endHour - startHour) | 0;
    date.setHours(hour);
    return date;
}