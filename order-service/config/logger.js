// config/logger.js
const winston = require('winston');
const LokiTransport = require('winston-loki');

// Tạo instance logger cố định cho order-service
const orderLogger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    // Thêm service name vào meta mặc định
    defaultMeta: { service: 'order-service' }, 
    transports: [
        new winston.transports.Console(),
        new LokiTransport({
            host: 'http://localhost:3100',
            labels: { app: 'order-service' }, // Nhãn app cố định
            json: true
        })
    ]
});

// Xuất cái instance này ra ngoài
module.exports = orderLogger;