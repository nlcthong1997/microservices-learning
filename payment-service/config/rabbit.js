// payment-service/config/rabbit.js
//
// RabbitMQ cho RabbitMQ SAGA flow (song song với Kafka SAGA):
//
//   inventory_events (fanout) → payment_order_reserved_queue → [file này consume]
//         ↓ success → payment_completed_events (fanout) → inventory confirm
//         ↓ fail    → saga_events (fanout)              → inventory rollback
//
const amqplib = require('amqplib');
const logger  = require('./logger');

const RABBITMQ_URL = 'amqp://localhost:5672';
let channel;

async function connectRabbit() {
    try {
        const connection = await amqplib.connect(RABBITMQ_URL);
        channel = await connection.createChannel();

        // 1. Assert exchange nhận từ inventory (inventory đã publish sau khi reserve)
        await channel.assertExchange('inventory_events', 'fanout', { durable: false });

        // 2. Queue của payment-service — nhận khi inventory reserve thành công
        const RESERVED_QUEUE = 'payment_order_reserved_queue';
        await channel.assertQueue(RESERVED_QUEUE, { durable: true });
        await channel.bindQueue(RESERVED_QUEUE, 'inventory_events', '');
        logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Queue bound ${RESERVED_QUEUE} ok.` });

        // 3. Exchange publish kết quả thanh toán (thành công)
        await channel.assertExchange('payment_completed_events', 'fanout', { durable: false });

        // 4. Exchange SAGA rollback (đã có sẵn, inventory-service consume)
        await channel.assertExchange('saga_events', 'fanout', { durable: false });

        logger.info({ trace_id: 'SYSTEM', message: 'RabbitMQ infrastructure ready (payment config).' });
        return channel;
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `RabbitMQ connection error: ${error.message}` });
        throw error;
    }
}

module.exports = { connectRabbit, getRabbitChannel: () => channel };
