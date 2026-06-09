// inventory-service/config/rabbit.js
const amqplib = require('amqplib');
const logger = require('./logger');

const RABBITMQ_URL = 'amqp://localhost:5672';
let channel;

async function connectRabbit() {
    try {
        const connection = await amqplib.connect(RABBITMQ_URL);
        channel = await connection.createChannel();

        // --- SETUP CHO LUỒNG CHÍNH (ORDER CREATED) ---
        // 1. Assert Fanout Exchange (nơi nhận tin từ Order Service)
        const ORDER_EXCHANGE = 'order_events';
        await channel.assertExchange(ORDER_EXCHANGE, 'fanout', { durable: false });

        // 2. Assert Queue riêng cho Inventory
        const ORDER_QUEUE = 'inventory_order_created_queue';
        await channel.assertQueue(ORDER_QUEUE, { durable: true }); // Bền vững

        // 3. Bind Queue vào Exchange
        await channel.bindQueue(ORDER_QUEUE, ORDER_EXCHANGE, '');
        logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Bind Queue ${ORDER_QUEUE} thành công.` });


        // --- SETUP CHO LUỒNG SAGA BÙ (PAYMENT FAILED) ---
        // 1. Assert SAGA Fanout Exchange (nơi nhận tin fail từ Payment Service)
        const SAGA_EXCHANGE = 'saga_events';
        await channel.assertExchange(SAGA_EXCHANGE, 'fanout', { durable: false });

        // 2. Assert Queue riêng cho SAGA Rollback
        const SAGA_ROLLBACK_QUEUE = 'inventory_saga_rollback_queue';
        await channel.assertQueue(SAGA_ROLLBACK_QUEUE, { durable: true });

        // 3. Bind Queue SAGA vào SAGA Exchange
        await channel.bindQueue(SAGA_ROLLBACK_QUEUE, SAGA_EXCHANGE, '');
        logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Bind Queue SAGA ${SAGA_ROLLBACK_QUEUE} thành công.` });

        logger.info({ trace_id: 'SYSTEM', message: 'Hạ tầng RabbitMQ sẵn sàng (Inventory config).' });
        return channel;
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `Lỗi kết nối RabbitMQ: ${error.message}` });
        throw error;
    }
}

module.exports = { connectRabbit, getRabbitChannel: () => channel };