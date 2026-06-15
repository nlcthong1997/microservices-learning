// config/logger.js
const winston = require('winston');
const LokiTransport = require('winston-loki');

// Format cho Console — JSON đẹp, dễ đọc trong terminal
const consoleFormat = winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
);

// Format cho Loki — toàn bộ log entry phải là 1 JSON string hoàn chỉnh
// Lý do: Loki dùng `| json` để parse log line thành fields có thể filter
// Nếu log line là "message {rest}" (text + object) → JSONParserErr
// Dùng printf để stringify toàn bộ info object → Loki nhận được valid JSON
const lokiFormat = winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf((info) => {
        // eslint-disable-next-line no-unused-vars
        const { [Symbol.for('splat')]: _splat, ...rest } = info;
        return JSON.stringify(rest);
    })
);

const orderLogger = winston.createLogger({
    level: 'info',
    defaultMeta: { service: 'order-service' },
    transports: [
        new winston.transports.Console({ format: consoleFormat }),
        new LokiTransport({
            host: 'http://localhost:3100',
            labels: { app: 'order-service' },
            batching: false,
            replaceTimestamp: true,
            format: lokiFormat
        })
    ]
});

module.exports = orderLogger;