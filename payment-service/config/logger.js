// payment-service/config/logger.js
const winston = require('winston');
const LokiTransport = require('winston-loki');

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    transports: [
        new winston.transports.Console(),
        new LokiTransport({
            host: 'http://localhost:3100',
            labels: { job: 'payment-service' },
            json: true,
            format: winston.format.json(),
            onConnectionError: (err) => console.error('[Loki] Connection error:', err)
        })
    ]
});

module.exports = logger;
