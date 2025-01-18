const knex = require('./my-knex')

const run = async () => {
    console.log('Running migrations')
    console.log('Checking if prices table exists')

    if (!await knex.schema.hasTable('prices_bitcoin_d1')) {
        console.log('Creating prices table')
        await knex.schema.createTable('prices_bitcoin_d1', (table) => {
            table.increments('id')
            table.float('price')
            table.datetime('date')
        })
    }
}

run().then()