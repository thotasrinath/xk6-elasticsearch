import xk6_elasticsearch from 'k6/x/elasticsearch';

/**
 * Index creation on tradeDate
 *
 * CREATE INDEX `global_myDateIdx`
 *     ON `test` (STR_TO_MILLIS(`tradeDate`))
 *     USING GSI;
 */
const client = xk6_elasticsearch.newBasicClient(['http://localhost:9200/']);
export default () => {

    var startDate = randomDate(new Date(2000, 0, 1), new Date(2022, 0, 1), 0, 24);

    var endDate = randomDate(startDate, new Date(2022, 0, 1), 0, 24);


    var query = '{"query":{"bool":{"must":[{"match":{"tradeDate":"Wed Jun 20 2002 14:41:56 GMT+0800 (+08)"}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}}'

    var res = client.find("test", query, 2);

    console.log(res);
}

function randomDate(start, end, startHour, endHour) {
    var date = new Date(+start + Math.random() * (end - start));
    var hour = startHour + Math.random() * (endHour - startHour) | 0;
    date.setHours(hour);
    return date;
}

