import xk6_elasticsearch from 'k6/x/elasticsearch';
/**
 * Index creation on partyTradeDate
 *
 * curl -X PUT "localhost:9200/test?pretty" -H 'Content-Type: application/json' -d'
 * {
 *   "properties": {
 *     "partyTradeDate": {
 *       "type": "date"
 *     }
 *   }
 * }
 * '
 *
 * Range Query for partyTradeDate secondlevel
 *
 * {
 * 	"query": {
 * 		"bool": {
 * 			"must": [{
 * 				"range": {
 * 					"trade.party.meta.partyTradeDate": {
 * 						"gt": "2000-05-17T07:54:49.139Z",
 * 						"lt": "2010-05-30T07:54:49.139Z"
 * 					}
 * 				}
 * 			}],
 * 			"must_not": [],
 * 			"should": []
 * 		}
 * 	},
 * 	"from": 0,
 * 	"size": 10,
 * 	"sort": [],
 * 	"aggs": {}
 * }
 */

const client = xk6_elasticsearch.newBasicClient(['http://localhost:9200/']);
export default () => {

    var startDate = randomDate(new Date(2000, 0, 1), new Date(2022, 0, 1), 0, 24);

    var endDate = randomDate(startDate, new Date(2022, 0, 1), 0, 24);


    var query = '"bool": { "must": [{ "range": { "trade.party.meta.partyTradeDate": { "gt": "' + startDate.toISOString() + '", "lt": "' + endDate.toISOString() + '" } } }], "must_not": [], "should": [] }';

    var res = client.find("test", query, 2);

    console.log(res);
}

function randomDate(start, end, startHour, endHour) {
    var date = new Date(+start + Math.random() * (end - start));
    var hour = startHour + Math.random() * (endHour - startHour) | 0;
    date.setHours(hour);
    return date;
}
