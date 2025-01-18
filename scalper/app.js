const yargs = require('yargs')
const {hideBin} = require('yargs/helpers')
const dayjs = require("dayjs");
const knex = require('./my-knex')

const CoinAPI = require('axios').default.create({
    baseURL: 'https://api.coincap.io',
    headers: {
        'Authorization': 'Bearer 8a323214-044e-44fc-9319-631e77bbcf71'
    }
})

const intervalsToDayjs = {
    m1: { value: 1, unit: 'minutes' },
    m5: { value: 5, unit: 'minutes' },
    m15: { value: 15, unit: 'minutes' },
    m30: { value: 30, unit: 'minutes' },
    h1: { value: 1, unit: 'hours' },
    h2: { value: 2, unit: 'hours' },
    h6: { value: 6, unit: 'hours' },
    h12: { value: 12, unit: 'hours' },
    d1: { value: 1, unit: 'days' },
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const createTableIfNotExists = async (table) => {
    if (!await knex.schema.hasTable(table)) {
        console.log(`Creating ${table} table`)
        await knex.schema.createTable(table, (table) => {
            table.increments('id')
            table.float('price')
            table.datetime('date')
        })
    }
}

const run = async (asset, interval, start) => {
    const intervalData = intervalsToDayjs[interval]

    let _start = dayjs(start)
    let _end = _start.add(intervalData.value * 100, intervalData.unit)
    let apiCallCount = 0
    let startApiCall = dayjs();

    const table = `prices_${asset}_${interval}`

    await createTableIfNotExists(table)

    do {
        if (apiCallCount >= 500) {
            let now = dayjs();
            if (now.isBefore(startApiCall.add(1, 'minute'))) {
                const sleepTime = startApiCall.add(1, 'minute').diff(now);
                console.log(`Sleeping for ${sleepTime / 1000} seconds`);
                await sleep(sleepTime);
            }
            apiCallCount = 0;
            startApiCall = dayjs();
        }

        const res = await CoinAPI.get(`/v2/assets/${asset}/history`, {
            params: {
                interval: interval,
                start: _start.unix() * 1000,
                end: _end.unix() * 1000
            }
        })
        apiCallCount++

        console.log('Inserting data', {
            start: _start.format('YYYY-MM-DD HH:mm:ss'),
            end: _end.format('YYYY-MM-DD HH:mm:ss'),
            apiCallCount
        })

        _start = _end
        _end = _start.add(intervalData.value * 100, intervalData.unit)

        const insertData = res.data.data.map(item => ({
            price: item.priceUsd,
            date: dayjs(item.time).format('YYYY-MM-DD HH:mm:ss')
        }));

        await knex(table).insert(insertData).onConflict('date').ignore();
    } while (_start.isBefore(dayjs()))
}

const argv = yargs(hideBin(process.argv))
    .option('asset', { alias: 'a', type: 'string' })
    .option('interval', { alias: 'i', type: 'string' })
    .option('start', { alias: 's', type: 'string' })
    .parse()

run(argv.asset, argv.interval, argv.start).then(() => console.log('Scalped!'))