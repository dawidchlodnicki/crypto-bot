const myKnex = require('knex')({
    client: 'mysql',
    connection: {
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'root',
        database: 'crypto_bot',
    },
});

module.exports = myKnex;