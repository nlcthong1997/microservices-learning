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
        //
        // Dead Letter Exchange (DLX) — xử lý message lỗi
        //
        // Bình thường:
        //   message → ORDER_QUEUE → consumer xử lý → ack → xóa
        //
        // Khi consumer nack(msg, false, false) — tức là xử lý thất bại:
        //   message → ORDER_QUEUE → nack → DLX → DLQ (dead letter queue)
        //                                          ↑ có thể retry sau, alert, hoặc audit
        //
        // Nếu KHÔNG có DLX: message nack → mất luôn (không trace được)
        // Nếu CÓ DLX:       message nack → lưu ở DLQ → có thể xử lý sau

        // 1. Tạo DLX trước (phải tồn tại trước khi queue chính reference nó)
        const DLX_EXCHANGE = 'order_events_dlx';
        await channel.assertExchange(DLX_EXCHANGE, 'fanout', { durable: true });

        // 2. Tạo Dead Letter Queue — nơi hứng message thất bại
        const DLQ = 'inventory_order_failed_queue';
        await channel.assertQueue(DLQ, { durable: true });
        await channel.bindQueue(DLQ, DLX_EXCHANGE, '');
        logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: DLX ready -> ${DLQ}` });

        // 3. Assert Fanout Exchange chính
        const ORDER_EXCHANGE = 'order_events';
        await channel.assertExchange(ORDER_EXCHANGE, 'fanout', { durable: false });

        // 4. Assert Queue chính — khai báo x-dead-letter-exchange
        //    Khi consumer nack → RabbitMQ tự động route message sang DLX
        //
        //    ⚠️ Nếu queue này đã tồn tại mà không có DLX args:
        //       RabbitMQ sẽ báo lỗi PRECONDITION_FAILED
        //       → Phải xóa queue cũ trong RabbitMQ UI (localhost:15672) rồi restart
        const ORDER_QUEUE = 'inventory_order_created_queue';
        await channel.assertQueue(ORDER_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
            },
        });

        // 5. Bind Queue vào Exchange
        await channel.bindQueue(ORDER_QUEUE, ORDER_EXCHANGE, '');
        logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Queue bound ${ORDER_QUEUE} ok.` });


        // --- SETUP CHO LUỒNG SAGA BÙ (PAYMENT FAILED) ---
        // 1. Assert SAGA Fanout Exchange (nơi nhận tin fail từ Payment Service)
        const SAGA_EXCHANGE = 'saga_events';
        await channel.assertExchange(SAGA_EXCHANGE, 'fanout', { durable: false });

        // 2. Assert Queue riêng cho SAGA Rollback
        const SAGA_ROLLBACK_QUEUE = 'inventory_saga_rollback_queue';
        await channel.assertQueue(SAGA_ROLLBACK_QUEUE, { durable: true });

        // 3. Bind Queue SAGA vào SAGA Exchange
        await channel.bindQueue(SAGA_ROLLBACK_QUEUE, SAGA_EXCHANGE, '');
        logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Queue bound ${SAGA_ROLLBACK_QUEUE} ok.` });

        // --- SETUP CHO LUỒNG PUBLISH SAU RESERVE (→ payment-service) ---
        // inventory-service publish vào đây sau khi reserve thành công
        // payment-service consume từ 'payment_order_reserved_queue' (bind vào exchange này)
        await channel.assertExchange('inventory_events', 'fanout', { durable: false });
        logger.info({ trace_id: 'SYSTEM', message: 'RabbitMQ: Exchange inventory_events ready.' });

        // --- SETUP CHO CONFIRM SALE (payment thành công → trừ stock thật) ---
        // payment-service publish vào 'payment_completed_events' sau khi charge thành công
        // inventory-service consume ở đây để gọi confirmSale()
        await channel.assertExchange('payment_completed_events', 'fanout', { durable: false });
        const PAYMENT_COMPLETED_QUEUE = 'inventory_payment_completed_queue';
        await channel.assertQueue(PAYMENT_COMPLETED_QUEUE, { durable: true });
        await channel.bindQueue(PAYMENT_COMPLETED_QUEUE, 'payment_completed_events', '');
        logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Queue bound ${PAYMENT_COMPLETED_QUEUE} ok.` });

        logger.info({ trace_id: 'SYSTEM', message: 'RabbitMQ infrastructure ready (inventory config).' });
        return channel;
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `RabbitMQ connection error: ${error.message}` });
        throw error;
    }
}

module.exports = { connectRabbit, getRabbitChannel: () => channel };