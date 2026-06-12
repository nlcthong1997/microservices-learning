// routes/orderRoutes.js
const express = require('express');
const router = express.Router();
const { randomUUID } = require('crypto');

const logger = require('../config/logger');
const { getRabbitChannel } = require('../config/rabbit');
const { getKafkaProducer } = require('../config/kafka');
const { httpClient, requestWithRetry } = require('../config/httpClient');
const { inventoryBreaker } = require('../config/circuitBreaker');

const INVENTORY_SERVICE_URL = 'http://localhost:3002';

// =========================================================================
// GIAI ĐOẠN 1 — HTTP SYNC (đơn giản, không có resilience)
//
// Dùng khi: học cơ bản, test nhanh
// Vấn đề:   không có timeout → treo nếu inventory-service không phản hồi
//           không có retry   → fail ngay nếu inventory-service momentarily down
//           không có circuit breaker → flood requests khi service down
// =========================================================================
router.post('/sync', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Sync] Order received: ${productId} x${quantity}` });

    try {
        const stockResponse = await axios_plain_for_comparison().get(
            `${INVENTORY_SERVICE_URL}/inventory/${productId}`,
            { headers: { 'x-trace-id': traceId } }
        );

        if (!stockResponse.data.available) {
            return res.status(400).json({ message: 'Out of stock', trace_id: traceId });
        }

        res.status(200).json({ message: 'Order placed successfully [sync-basic]', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Sync] Error: ${error.message}` });
        res.status(500).json({ message: 'Internal server error', trace_id: traceId });
    }
});

// =========================================================================
// GIAI ĐOẠN 2 — HTTP SYNC với Timeout + Retry + Circuit Breaker
//
// Thứ tự bảo vệ:
//   Circuit Breaker (ngoài cùng) → kiểm tra trạng thái trước
//   Retry (giữa)                 → thử lại khi gặp lỗi tạm thời
//   Timeout (trong cùng)         → giới hạn thời gian chờ mỗi lần gọi
//
// Test các kịch bản:
//   Bình thường:  POST /orders/sync-resilient {"productId":"IPHONE-15","quantity":1}
//   Timeout:      POST /orders/sync-resilient {"productId":"TRIGGER-SLOW","quantity":1}
//   Lỗi + Retry:  POST /orders/sync-resilient {"productId":"TRIGGER-ERROR","quantity":1}
//   Circuit open: Gọi TRIGGER-ERROR 3 lần liên tiếp → lần 4 fail ngay
// =========================================================================
router.post('/sync-resilient', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Sync-Resilient] Order received: ${productId} x${quantity}` });

    try {
        // Lớp 1 (ngoài): Circuit Breaker kiểm tra trạng thái inventory-service
        const stockResponse = await inventoryBreaker.execute(
            // Lớp 2 (giữa): Retry tự động khi gặp lỗi tạm thời
            () => requestWithRetry(
                // Lớp 3 (trong): HTTP call với timeout 3 giây
                () => httpClient.get(
                    `${INVENTORY_SERVICE_URL}/inventory/${productId}`,
                    { headers: { 'x-trace-id': traceId } }
                ),
                { maxRetries: 3, traceId }
            ),
            traceId
        );

        if (!stockResponse.data.available) {
            logger.warn({ trace_id: traceId, message: `[Sync-Resilient] Out of stock: ${productId}` });
            return res.status(400).json({ message: 'Out of stock', trace_id: traceId });
        }

        logger.info({ trace_id: traceId, message: `[Sync-Resilient] Order placed successfully.` });
        res.status(200).json({ message: 'Order placed successfully', trace_id: traceId });

    } catch (error) {
        // Phân biệt loại lỗi để trả status code phù hợp

        if (error.circuitOpen) {
            // Circuit breaker đang mở — inventory-service đang down
            // Không retry, không chờ — fail ngay với thông báo rõ ràng
            logger.warn({ trace_id: traceId, message: `[Sync-Resilient] Circuit OPEN, rejecting request.` });
            return res.status(503).json({
                message: 'Inventory service temporarily unavailable. Please try again later.',
                trace_id: traceId
            });
        }

        if (error.code === 'ECONNABORTED') {
            // Timeout — inventory-service quá chậm
            return res.status(504).json({
                message: 'Inventory service response timed out.',
                trace_id: traceId
            });
        }

        if (error.code === 'ECONNREFUSED') {
            // Service không chạy
            return res.status(503).json({
                message: 'Cannot connect to inventory service.',
                trace_id: traceId
            });
        }

        logger.error({ trace_id: traceId, message: `[Sync-Resilient] Error: ${error.message}` });
        res.status(500).json({ message: 'Internal server error', trace_id: traceId });
    }
});

// Xem trạng thái circuit breaker real-time
// curl http://localhost:3001/orders/circuit-status
router.get('/circuit-status', (req, res) => {
    res.json(inventoryBreaker.status());
});

// =========================================================================
// GIAI ĐOẠN 2 — RABBITMQ ASYNC
// =========================================================================
router.post('/async', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Async] Order received: ${productId} x${quantity}` });

    const rabbitChannel = getRabbitChannel();
    if (!rabbitChannel) {
        logger.error({ trace_id: traceId, message: `[Async] RabbitMQ not ready.` });
        return res.status(503).json({ message: 'Message broker not ready', trace_id: traceId });
    }

    try {
        const event = { productId, quantity, traceId, createdAt: new Date() };

        rabbitChannel.publish(
            'order_events',
            '',
            Buffer.from(JSON.stringify(event)),
            { headers: { 'x-trace-id': traceId } }
        );

        logger.info({ trace_id: traceId, message: `[Async] Event published to RabbitMQ.` });
        res.status(202).json({ message: 'Order accepted, processing in background', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Async] Publish error: ${error.message}` });
        res.status(500).json({ message: 'Internal server error', trace_id: traceId });
    }
});

// =========================================================================
// GIAI ĐOẠN 3 — KAFKA STREAM
// =========================================================================
router.post('/stream', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Stream] Event received: ${productId} x${quantity}` });

    const kafkaProducer = getKafkaProducer();
    if (!kafkaProducer) {
        logger.error({ trace_id: traceId, message: `[Stream] Kafka not ready.` });
        return res.status(503).json({ message: 'Kafka not ready', trace_id: traceId });
    }

    try {
        await kafkaProducer.send({
            topic: 'user-behavior-logs',
            messages: [{
                key: productId,
                value: JSON.stringify({ action: 'purchase', productId, quantity, traceId }),
                headers: { 'x-trace-id': traceId }
            }],
        });

        logger.info({ trace_id: traceId, message: `[Stream] Event published to Kafka.` });
        res.status(202).json({ message: 'Behavior tracked', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Stream] Error: ${error.message}` });
        res.status(500).json({ message: 'Internal server error', trace_id: traceId });
    }
});

// Placeholder để tránh lỗi require — /sync basic dùng axios trực tiếp (chưa cài)
// Trong thực tế bạn sẽ dùng: const axios = require('axios')
function axios_plain_for_comparison() {
    return require('axios');
}

// =========================================================================
// GIAI ĐOẠN 4 — gRPC SYNC
//
// Tại sao gRPC khác REST?
//
//   REST (/sync):
//     - Gửi: HTTP/1.1 text  "GET /inventory/IPHONE-15"
//     - Nhận: JSON string   '{"available":true,"stock":10}'
//     - Phải parse JSON string → object mỗi lần
//     - Không biết trước response có fields gì (phải đọc docs)
//
//   gRPC (/sync-grpc):
//     - Gửi: HTTP/2 binary  <protobuf bytes>
//     - Nhận: object JS     { available: true, stock: 10, message: '...' }
//     - Proto-loader tự xử lý serialize/deserialize
//     - IDE biết chính xác type của từng field ngay lúc viết code
//
// Khi nào dùng gRPC?
//   ✅ Service-to-service internal calls (không cần browser đọc được)
//   ✅ Cần type safety giữa các team khác nhau
//   ✅ High-throughput, low-latency (binary nhỏ hơn JSON ~3-5x)
//   ✅ Streaming bi-directional (gRPC streaming — REST không làm được)
//
// Khi nào KHÔNG dùng gRPC?
//   ❌ Public API (browser không hỗ trợ gRPC trực tiếp)
//   ❌ Cần human-readable request/response (debug dễ)
//   ❌ Team chưa quen với Protobuf
//
// Test: curl -X POST http://localhost:3001/orders/sync-grpc \
//         -H "Content-Type: application/json" \
//         -d "{\"productId\":\"IPHONE-15\",\"quantity\":1}"
// =========================================================================
router.post('/sync-grpc', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[gRPC] Order received: ${productId} x${quantity}` });

    // Import lazy để tránh lỗi nếu gRPC chưa kết nối lúc module load
    const { checkStock } = require('../config/grpcClient');

    try {
        // Gọi gRPC — cú pháp giống gọi function thường
        // Nhưng thực ra đang gọi qua mạng tới inventory-service:50051
        //
        // So sánh với REST:
        //   REST:  axios.get(`/inventory/${productId}`)  → stockResponse.data.available
        //   gRPC:  checkStock({ product_id, quantity })  → stockResult.available
        const stockResult = await checkStock({
            product_id: productId,
            quantity: quantity || 1,
        });

        logger.info({
            trace_id: traceId,
            message: `[gRPC] Result: ${stockResult.message}`,
            available: stockResult.available,
            stock: stockResult.stock,
            protocol: 'gRPC/HTTP2',  // phân biệt với REST logs
        });

        if (!stockResult.available) {
            return res.status(400).json({
                message: stockResult.message,
                trace_id: traceId,
                protocol: 'gRPC',
            });
        }

        res.status(200).json({
            message: 'Order placed successfully [gRPC]',
            stock_remaining: stockResult.stock,
            trace_id: traceId,
            protocol: 'gRPC',  // cho thấy rõ đây là gRPC call
        });

    } catch (error) {
        // gRPC errors có thêm `error.grpcCode` (set trong grpcClient.js)
        // Chuyển gRPC status code → HTTP status code phù hợp
        const grpcStatus = error.grpcCode;

        if (grpcStatus === 5) {
            // grpc.status.NOT_FOUND (5) → HTTP 404
            return res.status(404).json({
                message: `Product not found: ${productId}`,
                trace_id: traceId,
            });
        }

        if (grpcStatus === 14) {
            // grpc.status.UNAVAILABLE (14) → HTTP 503
            // Xảy ra khi inventory-service gRPC server không chạy
            return res.status(503).json({
                message: 'Inventory service gRPC unavailable',
                trace_id: traceId,
            });
        }

        logger.error({ trace_id: traceId, message: `[gRPC] Error: ${error.message}`, grpcCode: grpcStatus });
        res.status(500).json({ message: 'Internal server error', trace_id: traceId });
    }
});

// =========================================================================
// GIAI ĐOẠN 5 — KAFKA CHOREOGRAPHY SAGA
//
// Luồng đầy đủ (3 service phối hợp qua Kafka, không service nào gọi nhau trực tiếp):
//
//  [order-service]     POST /orders/kafka-saga
//        │ publish "order.created"
//        ▼
//  [Kafka topic: order-events]
//        │
//        ▼
//  [inventory-service] consume → reserve stock
//        │ publish "inventory.reserved" hoặc "inventory.failed"
//        ▼
//  [Kafka topic: inventory-events]
//        │
//        ▼
//  [payment-service]   consume → charge card
//        │ publish "payment.completed" hoặc "payment.failed"
//        ▼
//  [Kafka topic: payment-events]
//        │
//        ▼
//  [inventory-service] consume → nếu payment.failed → rollback kho (SAGA compensation)
//
// Test scenarios:
//   Thành công:     curl -X POST http://localhost:3001/orders/kafka-saga \
//                    -H "Content-Type: application/json" \
//                    -d '{"productId":"IPHONE-15","quantity":1}'
//
//   Hết hàng:       -d '{"productId":"FAKE-SKU","quantity":1}'
//                   → inventory.failed, payment-service không xử lý
//
//   Thanh toán lỗi: -d '{"productId":"FAIL-PAYMENT","quantity":1}'
//                   → inventory.reserved → payment.failed → kho bị rollback
//
// Theo dõi log: Kafka UI http://localhost:8080 → topics: order-events, inventory-events, payment-events
// =========================================================================
router.post('/kafka-saga', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Kafka SAGA] Order received: ${productId} x${quantity}` });

    const kafkaProducer = getKafkaProducer();
    if (!kafkaProducer) {
        logger.error({ trace_id: traceId, message: `[Kafka SAGA] Kafka not ready.` });
        return res.status(503).json({ message: 'Kafka not ready', trace_id: traceId });
    }

    try {
        const orderId = randomUUID();

        await kafkaProducer.send({
            topic: 'order-events',
            messages: [{
                key: orderId,       // Cùng orderId → cùng partition → đúng thứ tự event
                value: JSON.stringify({
                    type: 'order.created',
                    orderId,
                    productId,
                    quantity,
                    traceId,
                    createdAt: new Date().toISOString(),
                }),
                headers: { 'x-trace-id': traceId },
            }],
        });

        logger.info({ trace_id: traceId, message: `[Kafka SAGA] "order.created" published. orderId=${orderId}` });

        // Trả về ngay — không chờ inventory hay payment
        // Đây là bản chất của async/event-driven: fire and forget
        res.status(202).json({
            message: 'Order accepted — SAGA in progress (check logs for status)',
            orderId,
            trace_id: traceId,
        });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Kafka SAGA] Publish error: ${error.message}` });
        res.status(500).json({ message: 'Internal server error', trace_id: traceId });
    }
});

module.exports = router;
