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

    logger.info({ trace_id: traceId, message: `[Sync] Nhận đặt hàng: ${productId} x${quantity}` });

    try {
        const stockResponse = await axios_plain_for_comparison().get(
            `${INVENTORY_SERVICE_URL}/inventory/${productId}`,
            { headers: { 'x-trace-id': traceId } }
        );

        if (!stockResponse.data.available) {
            return res.status(400).json({ message: 'Hết hàng', trace_id: traceId });
        }

        res.status(200).json({ message: 'Đặt hàng thành công [sync-basic]', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Sync] Lỗi: ${error.message}` });
        res.status(500).json({ message: 'Lỗi hệ thống', trace_id: traceId });
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

    logger.info({ trace_id: traceId, message: `[Sync-Resilient] Nhận đặt hàng: ${productId} x${quantity}` });

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
            logger.warn({ trace_id: traceId, message: `[Sync-Resilient] Hết hàng: ${productId}` });
            return res.status(400).json({ message: 'Hết hàng', trace_id: traceId });
        }

        logger.info({ trace_id: traceId, message: `[Sync-Resilient] Đặt hàng thành công.` });
        res.status(200).json({ message: 'Đặt hàng thành công', trace_id: traceId });

    } catch (error) {
        // Phân biệt loại lỗi để trả status code phù hợp

        if (error.circuitOpen) {
            // Circuit breaker đang mở — inventory-service đang down
            // Không retry, không chờ — fail ngay với thông báo rõ ràng
            logger.warn({ trace_id: traceId, message: `[Sync-Resilient] Circuit OPEN, từ chối request.` });
            return res.status(503).json({
                message: 'Dịch vụ kho tạm thời không khả dụng. Vui lòng thử lại sau.',
                trace_id: traceId
            });
        }

        if (error.code === 'ECONNABORTED') {
            // Timeout — inventory-service quá chậm
            return res.status(504).json({
                message: 'Dịch vụ kho phản hồi quá chậm.',
                trace_id: traceId
            });
        }

        if (error.code === 'ECONNREFUSED') {
            // Service không chạy
            return res.status(503).json({
                message: 'Không thể kết nối đến dịch vụ kho.',
                trace_id: traceId
            });
        }

        logger.error({ trace_id: traceId, message: `[Sync-Resilient] Lỗi: ${error.message}` });
        res.status(500).json({ message: 'Lỗi hệ thống', trace_id: traceId });
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

    logger.info({ trace_id: traceId, message: `[Async] Nhận đặt hàng: ${productId} x${quantity}` });

    const rabbitChannel = getRabbitChannel();
    if (!rabbitChannel) {
        logger.error({ trace_id: traceId, message: `[Async] RabbitMQ chưa sẵn sàng.` });
        return res.status(503).json({ message: 'Message broker chưa sẵn sàng', trace_id: traceId });
    }

    try {
        const event = { productId, quantity, traceId, createdAt: new Date() };

        rabbitChannel.publish(
            'order_events',
            '',
            Buffer.from(JSON.stringify(event)),
            { headers: { 'x-trace-id': traceId } }
        );

        logger.info({ trace_id: traceId, message: `[Async] Đã publish event lên RabbitMQ.` });
        res.status(202).json({ message: 'Đơn hàng đang được xử lý', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Async] Lỗi publish: ${error.message}` });
        res.status(500).json({ message: 'Lỗi hệ thống', trace_id: traceId });
    }
});

// =========================================================================
// GIAI ĐOẠN 3 — KAFKA STREAM
// =========================================================================
router.post('/stream', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Stream] Nhận đặt hàng: ${productId} x${quantity}` });

    const kafkaProducer = getKafkaProducer();
    if (!kafkaProducer) {
        logger.error({ trace_id: traceId, message: `[Stream] Kafka chưa sẵn sàng.` });
        return res.status(503).json({ message: 'Kafka chưa sẵn sàng', trace_id: traceId });
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

        logger.info({ trace_id: traceId, message: `[Stream] Đã publish lên Kafka.` });
        res.status(202).json({ message: 'Đã ghi nhận hành vi', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Stream] Lỗi: ${error.message}` });
        res.status(500).json({ message: 'Lỗi hệ thống', trace_id: traceId });
    }
});

// Placeholder để tránh lỗi require — /sync basic dùng axios trực tiếp (chưa cài)
// Trong thực tế bạn sẽ dùng: const axios = require('axios')
function axios_plain_for_comparison() {
    return require('axios');
}

module.exports = router;

// =========================================================================
// GIAI ĐOẠN 1 — HTTP SYNC
// Gọi thẳng sang inventory-service, chờ trả lời, rồi mới response về client.
// Dùng khi: cần biết kết quả ngay, đơn giản, ít service.
// =========================================================================
router.post('/sync', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Sync] Nhận đặt hàng: ${productId} x${quantity}` });

    try {
        // Gọi HTTP sang inventory-service — chờ kết quả ở đây (blocking)
        const stockResponse = await axios.get(`${INVENTORY_SERVICE_URL}/inventory/${productId}`, {
            headers: { 'x-trace-id': traceId }
        });

        if (!stockResponse.data.available) {
            logger.warn({ trace_id: traceId, message: `[Sync] Hết hàng: ${productId}` });
            return res.status(400).json({ message: 'Hết hàng', trace_id: traceId });
        }

        logger.info({ trace_id: traceId, message: `[Sync] Còn hàng, đặt thành công.` });
        res.status(200).json({ message: 'Đặt hàng thành công', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Sync] Lỗi: ${error.message}` });
        res.status(500).json({ message: 'Lỗi hệ thống', trace_id: traceId });
    }
});

// =========================================================================
// GIAI ĐOẠN 2 — RABBITMQ ASYNC
// Ném event vào RabbitMQ rồi trả 202 ngay, không chờ inventory xử lý xong.
// inventory-service đang lắng nghe ngầm, tự nhận và trừ kho.
// Dùng khi: không cần kết quả ngay, muốn tách biệt service, chịu tải tốt hơn.
// =========================================================================
router.post('/async', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Async] Nhận đặt hàng: ${productId} x${quantity}` });

    const rabbitChannel = getRabbitChannel();
    if (!rabbitChannel) {
        logger.error({ trace_id: traceId, message: `[Async] RabbitMQ chưa sẵn sàng.` });
        return res.status(503).json({ message: 'Message broker chưa sẵn sàng', trace_id: traceId });
    }

    try {
        const event = { productId, quantity, traceId, createdAt: new Date() };

        // Publish vào exchange "order_events" (fanout) — inventory-service tự nhận
        rabbitChannel.publish(
            'order_events',
            '',                                      // routing key để trống vì fanout
            Buffer.from(JSON.stringify(event)),
            { headers: { 'x-trace-id': traceId } }
        );

        logger.info({ trace_id: traceId, message: `[Async] Đã publish event lên RabbitMQ.` });

        // Trả về ngay, không đợi inventory xử lý xong
        res.status(202).json({ message: 'Đơn hàng đang được xử lý', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Async] Lỗi publish: ${error.message}` });
        res.status(500).json({ message: 'Lỗi hệ thống', trace_id: traceId });
    }
});

// =========================================================================
// GIAI ĐOẠN 3 — KAFKA STREAM
// Publish hành vi user lên Kafka. Nhiều service khác nhau (analytics, ML,
// audit log) có thể đọc độc lập mà không ảnh hưởng lẫn nhau.
// Dùng khi: cần nhiều consumer đọc cùng 1 event, cần replay lại lịch sử.
// =========================================================================
router.post('/stream', async (req, res) => {
    const traceId = randomUUID();
    const { productId, quantity } = req.body;

    logger.info({ trace_id: traceId, message: `[Stream] Nhận đặt hàng: ${productId} x${quantity}` });

    const kafkaProducer = getKafkaProducer();
    if (!kafkaProducer) {
        logger.error({ trace_id: traceId, message: `[Stream] Kafka chưa sẵn sàng.` });
        return res.status(503).json({ message: 'Kafka chưa sẵn sàng', trace_id: traceId });
    }

    try {
        await kafkaProducer.send({
            topic: 'user-behavior-logs',
            messages: [{
                key: productId,   // cùng productId → cùng partition → ordering đảm bảo
                value: JSON.stringify({ action: 'purchase', productId, quantity, traceId }),
                headers: { 'x-trace-id': traceId }
            }],
        });

        logger.info({ trace_id: traceId, message: `[Stream] Đã publish lên Kafka.` });
        res.status(202).json({ message: 'Đã ghi nhận hành vi', trace_id: traceId });

    } catch (error) {
        logger.error({ trace_id: traceId, message: `[Stream] Lỗi publish Kafka: ${error.message}` });
        res.status(500).json({ message: 'Lỗi hệ thống', trace_id: traceId });
    }
});

module.exports = router;