import http from 'k6/http';
import { sleep, check } from 'k6';
import { Counter } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.0.0/index.js';

let VUsCount = __ENV.VUS ? __ENV.VUS : 5;
const vuInitTimeoutSecs = 5; // adjust this if you have a longer init!
const loadTestDurationSecs = 30;
const loadTestGracefulStopSecs = 5;

let vuSetupsDone = new Counter('vu_setups_done');

export let options = {
    scenarios: {
        // This is the per-VU setup/init equivalent:
        vu_setup: {
            executor: 'per-vu-iterations',
            vus: VUsCount,
            iterations: 1,
            maxDuration: `${vuInitTimeoutSecs}s`,
            gracefulStop: '0s',

            exec: 'vuSetup',
        },

        // You can have any type of executor here, or multiple ones, as long as
        // the number of VUs is the pre-initialized VUsCount above.
        my_api_test: {
            executor: 'constant-arrival-rate',
            startTime: `${vuInitTimeoutSecs}s`, // start only after the init is done
            preAllocatedVUs: VUsCount,

            rate: 5,
            timeUnit: '1s',
            duration: `${loadTestDurationSecs}s`,
            gracefulStop: `${loadTestGracefulStopSecs}s`,

            // Add extra tags to emitted metrics from this scenario. This way
            // our thresholds below can only be for them. We can also filter by
            // the `scenario:my_api_test` tag for that, but setting a custom tag
            // here allows us to set common thresholds for multi-scenario tests.
            tags: { type: 'loadtest' },
            exec: 'apiTest',
        },

        // This is the per-VU teardown/cleanup equivalent:
        vu_teardown: {
            executor: 'per-vu-iterations',
            startTime: `${vuInitTimeoutSecs + loadTestDurationSecs + loadTestGracefulStopSecs}s`,
            vus: VUsCount,
            iterations: 1,
            maxDuration: `${vuInitTimeoutSecs}s`,
            exec: 'vuTeardown',
        },
    },
    thresholds: {
        // Make sure all of the VUs finished their setup successfully, so we can
        // ensure that the load test won't continue with broken VU "setup" data
        'vu_setups_done': [{
            threshold: `count==${VUsCount}`,
            abortOnFail: true,
            delayAbortEval: `${vuInitTimeoutSecs}s`,
        }],
        // Also make sure all of the VU teardown calls finished uninterrupted:
        'iterations{scenario:vu_teardown}': [`count==${VUsCount}`],

        // Ignore HTTP requests from the VU setup or teardown here
        'http_req_duration{type:loadtest}': ['p(99)<300', 'p(99.9)<500', 'max<1000'],
    },
    summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)', 'max'],
};

let vuCrocName = uuidv4();
let httpReqParams = { headers: {} }; // token is set in init()

export function vuSetup() {
    vuSetupsDone.add(0); // workaround for https://github.com/loadimpact/k6/issues/1346

    let user = `croco${vuCrocName}`
    let pass = `pass${__VU}`

    let res = http.post('https://test-api.k6.io/user/register/', {
        first_name: 'Crocodile',
        last_name: vuCrocName,
        username: user,
        password: pass,
    });
    check(res, { 'Created user': (r) => r.status === 201 });

    // Add some bogus wait time to see how VU setup "timeouts" are handled, and
    // how these requests are not included in the http_req_duration threshold.
    let randDelay = Math.floor(Math.random() * 4)
    http.get(`https://httpbin.test.k6.io/delay/${randDelay}`);

    let loginRes = http.post(`https://test-api.k6.io/auth/token/login/`, {
        username: user,
        password: pass
    });

    let vuAuthToken = loginRes.json('access');
    if (check(vuAuthToken, { 'Logged in user': (t) => t !== '' })) {
        console.log(`VU ${__VU} was logged in with username ${user} and token ${vuAuthToken}`);

        // Set the data back in the global VU context:
        httpReqParams.headers['Authorization'] = `Bearer ${vuAuthToken}`;
        vuSetupsDone.add(1);
    }
}


export function apiTest() {
    const url = 'https://test-api.k6.io/my/crocodiles/';
    const payload = {
        name: `Name ${uuidv4()}`,
        sex: 'M',
        date_of_birth: '2001-01-01',
    };

    let newCrocResp = http.post(url, payload, httpReqParams);
    if (check(newCrocResp, { 'Croc created correctly': (r) => r.status === 201 })) {
        console.log(`[${__VU}] Created a new croc with id ${newCrocResp.json('id')}`);
    }

    let resp = http.get(url, httpReqParams);
    if (resp.status == 200 && resp.json().length > 3) {
        let data = resp.json();
        if (data.length > 3) {
            let id = data[0].id;
            console.log(`[${__VU}] We have ${data.length} crocs, so deleting the oldest one ${id}`);
            let r = http.del(`${url}/${id}/`, null, httpReqParams);
            check(newCrocResp, { 'Croc deleted correctly': (r) => r.status === 201 })
        }
    }
}


export function vuTeardown() {
    console.log(`VU ${__VU} (${vuCrocName}) is tearing itself down...`);

    // In the real world, that will be actual clean up code and fancy error
    // catching like in vuSetup() above. For the demo, you can increase the
    // bogus wait time below to see how VU teardown "timeouts" are handled.
    sleep(Math.random() * 5);

    console.log(`VU ${__VU} (${vuCrocName}) was torn down!`);
}