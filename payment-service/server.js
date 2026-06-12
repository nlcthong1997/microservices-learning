// payment-service/server.js
const express = require('express');
const { randomUUID } = require('crypto');
const logger = require('./config/logger');
const { connectKafka, getKafkaProducer, getKafkaConsumer } = require('./config/kafka');

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
// STARTUP — kết nối Kafka trước, chỉ mở port khi sẵn sàng
// =========================================================================
async function start() {
    try {
        logger.info({ trace_id: 'SYSTEM', message: 'Connecting to infrastructure...' });

        await connectKafka();
        await startInventoryEventsConsumer();

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
