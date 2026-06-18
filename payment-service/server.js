// payment-service/server.js
const express = require('express');
const { randomUUID } = require('crypto');
const logger = require('./config/logger');
const { connectKafka, getKafkaProducer, getKafkaConsumer } = require('./config/kafka');
const { connectRabbit, getRabbitChannel } = require('./config/rabbit');

const app = express();
const PORT = 3003;

app.use(express.json());

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', service: 'payment-service', port: PORT });
});

// =========================================================================
// KAFKA CONSUMER — Choreography SAGA
//
// Luồng:
//   order-service    → publish "order-events"   { type: "order.created", ... }
//   inventory-service → publish "inventory-events" { type: "inventory.reserved" | "inventory.failed", ... }
//   payment-service  → (file này) consume "inventory-events"
//                     → nếu inventory.reserved → xử lý thanh toán
//                     → publish "payment-events" { type: "payment.completed" | "payment.failed" }
//   inventory-service → consume "payment-events"
//                     → nếu payment.failed → rollback kho (compensating transaction)
//
// Test scenarios:
//   Thành công:    POST /orders/kafka-saga {"productId":"IPHONE-15","quantity":1}
//   Hết hàng:      POST /orders/kafka-saga {"productId":"FAKE-SKU","quantity":1}
//   Thanh toán lỗi: POST /orders/kafka-saga {"productId":"FAIL-PAYMENT","quantity":1}
// =========================================================================
async function startInventoryEventsConsumer() {
    const consumer = getKafkaConsumer();
    const producer = getKafkaProducer();

    await consumer.subscribe({ topic: 'inventory-events', fromBeginning: false });

    logger.info({ trace_id: 'SYSTEM', message: 'Kafka: Consumer "inventory-events" is listening...' });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value.toString());
            const { type, orderId, productId, quantity, traceId } = event;

            // Chỉ xử lý khi kho đã reserve thành công
            if (type !== 'inventory.reserved') {
                logger.info({
                    trace_id: traceId,
                    message: `[Payment] Ignoring event type: ${type}`,
                });
                return;
            }

            logger.info({
                trace_id: traceId,
                message: `[Payment] Processing payment for orderId=${orderId}, productId=${productId}...`,
            });

            // =========================================================
            // Mock thanh toán
            // Dùng productId === 'FAIL-PAYMENT' để simulate thẻ bị từ chối
            // Trong thực tế: gọi Stripe/VNPay API ở đây
            // =========================================================
            const isPaymentSuccess = productId !== 'FAIL-PAYMENT';

            // Simulate processing time (500ms)
            await new Promise((resolve) => setTimeout(resolve, 500));

            if (isPaymentSuccess) {
                const amount = Math.floor(Math.random() * 5000000) + 100000; // 100k - 5.1M VND

                logger.info({
                    trace_id: traceId,
                    message: `[Payment] SUCCESS — orderId=${orderId} charged ${amount.toLocaleString('vi-VN')} VND.`,
                });

                await producer.send({
                    topic: 'payment-events',
                    messages: [{
                        key: orderId,
                        value: JSON.stringify({
                            type: 'payment.completed',
                            orderId,
                            productId,
                            quantity,
                            traceId,
                            amount,
                            completedAt: new Date().toISOString(),
                        }),
                        headers: { 'x-trace-id': traceId },
                    }],
                });

            } else {
                logger.error({
                    trace_id: traceId,
                    message: `[Payment] FAILED — card declined for orderId=${orderId}. SAGA rollback triggered.`,
                });

                await producer.send({
                    topic: 'payment-events',
                    messages: [{
                        key: orderId,
                        value: JSON.stringify({
                            type: 'payment.failed',
                            orderId,
                            productId,
                            quantity,
                            traceId,
                            reason: 'card_declined',
                            failedAt: new Date().toISOString(),
                        }),
                        headers: { 'x-trace-id': traceId },
                    }],
                });
            }
        },
    });
}

// =========================================================================
// RABBITMQ CONSUMER — RabbitMQ SAGA flow (song song với Kafka SAGA)
//
// Luồng:
//   inventory-service publish "inventory.reserved" → inventory_events (fanout)
//   [file này] consume payment_order_reserved_queue → xử lý thanh toán
//     success → publish payment_completed_events → inventory confirm sale
//     fail    → publish saga_events              → inventory rollback
//
// Test: POST /orders/async {"productId":"FAIL-PAYMENT","quantity":1}
// =========================================================================
async function startRabbitPaymentConsumer() {
    const channel = getRabbitChannel();
    if (!channel) return;

    const QUEUE = 'payment_order_reserved_queue';
    logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Consumer 'inventory.reserved' is listening on ${QUEUE}...` });

    channel.consume(QUEUE, async (msg) => {
        if (!msg) return;

        const traceId = msg.properties.headers['x-trace-id'] || 'SYSTEM-RMQ-PMT';
        const { type, orderId, productId, quantity } = JSON.parse(msg.content.toString());

        if (type !== 'inventory.reserved') {
            channel.ack(msg);
            return;
        }

        logger.info({
            trace_id: traceId,
            message: `[RabbitMQ SAGA] Processing payment for orderId=${orderId}, productId=${productId}...`,
        });

        // Mock payment — FAIL-PAYMENT luôn bị từ chối
        const isSuccess = productId !== 'FAIL-PAYMENT';
        await new Promise(r => setTimeout(r, 300));

        if (isSuccess) {
            const amount = Math.floor(Math.random() * 5000000) + 100000;
            logger.info({
                trace_id: traceId,
                message: `[RabbitMQ SAGA] Payment SUCCESS — orderId=${orderId}, amount=${amount.toLocaleString('vi-VN')} VND`,
            });
            channel.publish(
                'payment_completed_events', '',
                Buffer.from(JSON.stringify({ type: 'payment.completed', orderId, productId, quantity, traceId, amount, completedAt: new Date().toISOString() })),
                { headers: { 'x-trace-id': traceId } }
            );
        } else {
            logger.error({
                trace_id: traceId,
                message: `[RabbitMQ SAGA] Payment FAILED — card declined for orderId=${orderId}. SAGA rollback triggered.`,
            });
            channel.publish(
                'saga_events', '',
                Buffer.from(JSON.stringify({ type: 'payment.failed', orderId, productId, quantity, traceId, reason: 'card_declined', failedAt: new Date().toISOString() })),
                { headers: { 'x-trace-id': traceId } }
            );
        }

        channel.ack(msg);
    });
}

// =========================================================================
// STARTUP — kết nối Kafka trước, chỉ mở port khi sẵn sàng
// =========================================================================
async function start() {
    try {
        logger.info({ trace_id: 'SYSTEM', message: 'Connecting to infrastructure...' });

        await connectKafka();
        await startInventoryEventsConsumer();

        await connectRabbit();
        await startRabbitPaymentConsumer();

        app.listen(PORT, () => {
            logger.info({
                trace_id: 'SYSTEM',
                message: `Payment Service ready on port ${PORT}`,
            });
        });

    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: `Failed to start: ${error.message}` });
        process.exit(1);
    }
}

start();
