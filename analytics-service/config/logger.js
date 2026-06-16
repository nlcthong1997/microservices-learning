// analytics-service/config/logger.js
const winston = require('winston');
const LokiTransport = require('winston-loki');

const consoleFormat = winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
);

const lokiFormat = winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf((info) => {
        const { [Symbol.for('splat')]: _splat, ...rest } = info;
        return JSON.stringify(rest);
    })
);

const logger = winston.createLogger({
    level: 'info',
    defaultMeta: { service: 'analytics-service' },
    transports: [
        new winston.transports.Console({ format: consoleFormat }),
        new LokiTransport({
            host: 'http://localhost:3100',
            labels: { app: 'analytics-service' },
            batching: false,
            replaceTimestamp: true,
            format: lokiFormat,
            onConnectionError: (err) => console.error('[Loki] Connection error:', err),
        }),
    ],
});

module.exports = logger;
