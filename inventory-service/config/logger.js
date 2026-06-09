// inventory-service/config/logger.js
const winston = require('winston');
const LokiTransport = require('winston-loki');

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    defaultMeta: { service: 'inventory-service' },
    transports: [
        new winston.transports.Console(),
        new LokiTransport({
            host: 'http://localhost:3100', // Loki local port
            labels: { app: 'inventory-service' },
            json: true
        })
    ]
});

module.exports = logger;