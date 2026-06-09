// config/rabbit.js
const amqplib = require('amqplib');
const logger = require('./logger');

const RABBITMQ_URL = 'amqp://localhost:5672';
let channel;

async function connectRabbit() {
    try {
        const connection = await amqplib.connect(RABBITMQ_URL);
        channel = await connection.createChannel();
        
        // Định nghĩa Exchange kiểu Fanout
        await channel.assertExchange('order_events', 'fanout', { durable: false });
        
        logger.info({ trace_id: 'SYSTEM', message: 'Hạ tầng RabbitMQ sẵn sàng (config).' });
        return channel;
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `Lỗi kết nối RabbitMQ: ${error.message}` });
        throw error; // Quăng lỗi để file main biết
    }
}

// Export một function để lấy channel đã connect
module.exports = { connectRabbit, getRabbitChannel: () => channel };