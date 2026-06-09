// routes/orderRoutes.js
const express = require('express');
const router = express.Router();
const axios = require('axios');
const { randomUUID } = require('crypto'); // built-in Node.js, không cần cài package

const logger = require('../config/logger');
const { getRabbitChannel } = require('../config/rabbit');
const { getKafkaProducer } = require('../config/kafka');

const INVENTORY_SERVICE_URL = 'http://localhost:3002';

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