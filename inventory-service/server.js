// inventory-service/src/server.js
const express = require('express');
const logger = require('./config/logger');

// Import hạ tầng config
const { connectRabbit, getRabbitChannel } = require('./config/rabbit');
const {
    connectKafka,
    getKafkaProducer,
    getOrderEventsConsumer,
    getPaymentEventsConsumer,
} = require('./config/kafka');
// Import dữ liệu dùng chung (cho consumers sử dụng)
const mockInventory = require('./models/inventory');

// Import route file mới (TÁCH BIỆT)
const inventoryRoutes = require('./routes/inventoryRoutes');

// Import gRPC server
const { startGrpcServer } = require('./config/grpcServer');

// =========================================================================
// TÊN QUEUE — khai báo 1 chỗ, dùng chung cho setup và consumer
// =========================================================================
const ORDER_QUEUE          = 'inventory_order_created_queue';
const SAGA_ROLLBACK_QUEUE  = 'inventory_saga_rollback_queue';
const DLQ                  = 'inventory_order_failed_queue';

const app = express();
const PORT = 3002;

app.use(express.json());

// --- MOUNT ROUTES ---
// Mọi route trong file inventoryRoutes.js sẽ bắt đầu bằng '/inventory'
app.use('/inventory', inventoryRoutes);

// =========================================================================
// CÁC CONSUMERS BẤT ĐỒNG BỘ (ASYNC - RabbitMQ)
// (Logic giữ nguyên, nhưng dùng mockInventory từ models file)
// =========================================================================

// LUỒNG 2A: Consumer cho Luồng Chính (Nhận chốt đơn)
async function startOrderCreatedConsumer() {
    const channel = getRabbitChannel();

    if (!channel) return;

    logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Consumer 'order.created' is listening...` });

    channel.consume(ORDER_QUEUE, (msg) => {
        if (msg !== null) {
            const traceId = msg.properties.headers['x-trace-id'] || 'SYSTEM-ASYNC-ORDER';
            const orderCreatedEvent = JSON.parse(msg.content.toString());
            const { productId, quantity } = orderCreatedEvent;

            // --- NGHIỆP VỤ TRỪ KHO TẠM THỜI (Dùng nguồn dữ liệu shared) ---
            const product = mockInventory[productId];
            
            if (product && product.stock >= quantity) {
                product.stock -= quantity;
                product.reserved += quantity;
                logger.info({ trace_id: traceId, message: `[RabbitMQ Async] RESERVE success.` });
                channel.ack(msg);
            } else {
                logger.error({ trace_id: traceId, message: `[RabbitMQ Async] RESERVE failed - out of stock.` });
                channel.nack(msg, false, false);
            }
        }
    });
}

// LUỒNG 2B: Consumer cho SAGA BÙ (Nhận thanh toán fail)
async function startSagaRollbackConsumer() {
    const channel = getRabbitChannel();

    if (!channel) return;

    logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Consumer SAGA 'payment.failed' is listening...` });

    channel.consume(SAGA_ROLLBACK_QUEUE, (msg) => {
        if (msg !== null) {
            const traceId = msg.properties.headers['x-trace-id'] || 'SYSTEM-SAGA-BU';
            const sagaFailedEvent = JSON.parse(msg.content.toString());
            const { productId, quantity } = sagaFailedEvent;

            // --- NGHIỆP VỤ ROLLBACK (Dùng nguồn dữ liệu shared) ---
            const product = mockInventory[productId];
            
            if (product) {
                product.reserved -= quantity;
                product.stock += quantity;
                logger.info({ trace_id: traceId, message: `[SAGA] ROLLBACK success.` });
            }
            channel.ack(msg);
        }
    });
}


// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'ok', service: 'inventory-service', port: PORT });
});

// =========================================================================
// CONSUMER CHO DEAD LETTER QUEUE
//
// Message vào đây khi:
//   1. Consumer nack(msg, false, false) — hết hàng, sản phẩm không tồn tại
//   2. Message hết TTL (nếu có cấu hình x-message-ttl trên queue)
//
// Trong production, consumer này sẽ:
//   - Gửi alert (Slack, PagerDuty)
//   - Ghi vào audit log
//   - Retry với delay (schedule lại sau N phút)
//   - Notify user "đơn hàng của bạn bị hủy vì hết hàng"
//
// Trong demo này: log cảnh báo để thấy message không bị mất
// =========================================================================
async function startDLQConsumer() {
    const channel = getRabbitChannel();
    if (!channel) return;

    logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: DLQ Consumer is listening on ${DLQ}...` });

    channel.consume(DLQ, (msg) => {
        if (msg !== null) {
            const traceId = msg.properties.headers['x-trace-id'] || 'SYSTEM-DLQ';
            const event = JSON.parse(msg.content.toString());

            // x-death header do RabbitMQ tự gắn — chứa thông tin tại sao message chết
            // VD: queue tên gì, lý do (rejected/expired), timestamp
            const deathInfo = msg.properties.headers['x-death']?.[0];

            logger.error({
                trace_id: traceId,
                message: `[DLQ] Message failed - routed to Dead Letter Queue`,
                event,
                reason: deathInfo?.reason || 'unknown',
                original_queue: deathInfo?.queue || 'unknown',
                death_count: deathInfo?.count || 1,
            });

            // Ack message trong DLQ — tránh nó bị loop lại
            // (DLQ thường không có DLX tiếp theo)
            channel.ack(msg);
        }
    });
}

// =========================================================================
// KAFKA CONSUMERS — Choreography SAGA
//
// Consumer 1: "order-events" topic
//   - Nhận event order.created từ order-service (route POST /orders/kafka-saga)
//   - Reserve kho
//   - Publish "inventory.reserved" hoặc "inventory.failed" lên "inventory-events"
//
// Consumer 2: "payment-events" topic
//   - Nhận event payment.failed từ payment-service
//   - Compensating transaction: hoàn trả kho đã reserve (SAGA rollback)
// =========================================================================
async function startKafkaOrderEventsConsumer() {
    const consumer = getOrderEventsConsumer();
    const producer = getKafkaProducer();

    await consumer.subscribe({ topic: 'order-events', fromBeginning: false });

    logger.info({ trace_id: 'SYSTEM', message: 'Kafka: Consumer "order-events" is listening...' });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value.toString());
            const { type, orderId, productId, quantity, traceId } = event;

            if (type !== 'order.created') return;

            logger.info({
                trace_id: traceId,
                message: `[Kafka SAGA] Received order.created — orderId=${orderId}, productId=${productId}, qty=${quantity}`,
            });

            const product = mockInventory[productId];

            if (product && product.stock >= quantity) {
                // Reserve kho
                product.stock    -= quantity;
                product.reserved += quantity;

                logger.info({
                    trace_id: traceId,
                    message: `[Kafka SAGA] Stock RESERVED for orderId=${orderId}. Stock remaining: ${product.stock}`,
                });

                await producer.send({
                    topic: 'inventory-events',
                    messages: [{
                        key: orderId,
                        value: JSON.stringify({
                            type: 'inventory.reserved',
                            orderId, productId, quantity, traceId,
                            reservedAt: new Date().toISOString(),
                        }),
                        headers: { 'x-trace-id': traceId },
                    }],
                });

            } else {
                logger.error({
                    trace_id: traceId,
                    message: `[Kafka SAGA] Stock FAILED for orderId=${orderId} — product not found or out of stock.`,
                });

                await producer.send({
                    topic: 'inventory-events',
                    messages: [{
                        key: orderId,
                        value: JSON.stringify({
                            type: 'inventory.failed',
                            orderId, productId, quantity, traceId,
                            reason: product ? 'out_of_stock' : 'product_not_found',
                            failedAt: new Date().toISOString(),
                        }),
                        headers: { 'x-trace-id': traceId },
                    }],
                });
            }
        },
    });
}

async function startKafkaPaymentEventsConsumer() {
    const consumer = getPaymentEventsConsumer();

    await consumer.subscribe({ topic: 'payment-events', fromBeginning: false });

    logger.info({ trace_id: 'SYSTEM', message: 'Kafka: Consumer "payment-events" is listening (SAGA rollback)...' });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value.toString());
            const { type, orderId, productId, quantity, traceId } = event;

            if (type !== 'payment.failed') return;

            logger.warn({
                trace_id: traceId,
                message: `[Kafka SAGA] payment.failed received — rolling back stock for orderId=${orderId}`,
            });

            // Compensating transaction — hoàn lại kho đã reserve
            const product = mockInventory[productId];
            if (product) {
                product.reserved -= quantity;
                product.stock    += quantity;

                logger.info({
                    trace_id: traceId,
                    message: `[Kafka SAGA] ROLLBACK complete for orderId=${orderId}. Stock restored: ${product.stock}`,
                });
            }
        },
    });
}

// Fix startup bug — xem giải thích chi tiết trong order-service/server.js
async function start() {
    try {
        logger.info({ trace_id: 'SYSTEM', message: 'Connecting to infrastructure...' });

        await connectRabbit();
        await startOrderCreatedConsumer();
        await startSagaRollbackConsumer();
        await startDLQConsumer();

        await connectKafka();
        await startKafkaOrderEventsConsumer();
        await startKafkaPaymentEventsConsumer();

        // Khởi động gRPC Server (port 50051) SONG SONG với Express (port 3002)
        // Hai server độc lập — gRPC phục vụ các service nội bộ (binary, nhanh)
        //                    — Express phục vụ REST API (browser, curl, Postman)
        startGrpcServer();

        app.listen(PORT, () => {
            logger.info({
                trace_id: 'SYSTEM',
                message: `Inventory Service ready on port ${PORT}`
            });
        });

    } catch (error) {
        logger.error({
            trace_id: 'SYSTEM',
            message: `Failed to start: ${error.message}`
        });
        process.exit(1);
    }
}

start();