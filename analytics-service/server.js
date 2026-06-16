// analytics-service/server.js
//
// Mục đích: Demo Kafka Consumer Group
//
// KEY CONCEPT — Tại sao analytics-service nhận được ĐẦY ĐỦ message dù ui-service cũng đang đọc?
//
//   user-behavior-logs topic (Kafka giữ data, không xóa sau khi đọc)
//       │
//       ├──► groupId: 'ui-service-group'      → nhận 100% message  (ui-service)
//       │
//       └──► groupId: 'analytics-group'        → nhận 100% message  (file này)
//                                                ← CÙNG DATA, ĐỘC LẬP HOÀN TOÀN
//
//   Nếu dùng RabbitMQ queue thay thế:
//       Queue → consumer A nhận 50%, consumer B nhận 50% → tranh nhau
//
// Test:
//   1. Start service này: node server.js
//   2. Gửi vài request: POST http://localhost:3001/orders/stream
//   3. Xem summary:     GET  http://localhost:3005/analytics/summary
//   4. Confirm ui-service CŨNG nhận đầy đủ qua SSE (không bị mất message)

const express = require('express');
const { Kafka } = require('kafkajs');
const logger   = require('./config/logger');

const app  = express();
const PORT = 3005;

// ── In-memory store (thay cho database) ──────────────────────────────────
//
// Trong production: dùng Redis, TimescaleDB, ClickHouse, v.v.
// Ở đây dùng object đơn giản để tập trung vào Kafka concept
const stats = {
    totalEvents:  0,
    totalQuantity: 0,
    byProduct:    {},   // { 'IPHONE-15': { count: 5, quantity: 7 }, ... }
    byHour:       {},   // { '14': 3, '15': 12, ... }  — phân bố theo giờ
    firstEventAt: null,
    lastEventAt:  null,
};

function recordEvent({ productId, quantity }) {
    const qty  = parseInt(quantity, 10) || 1;
    const hour = new Date().getHours().toString();

    stats.totalEvents   += 1;
    stats.totalQuantity += qty;
    stats.lastEventAt    = new Date().toISOString();
    if (!stats.firstEventAt) stats.firstEventAt = stats.lastEventAt;

    // Đếm theo sản phẩm
    if (!stats.byProduct[productId]) stats.byProduct[productId] = { count: 0, quantity: 0 };
    stats.byProduct[productId].count    += 1;
    stats.byProduct[productId].quantity += qty;

    // Đếm theo giờ trong ngày
    stats.byHour[hour] = (stats.byHour[hour] || 0) + 1;
}

// ── Kafka Consumer ────────────────────────────────────────────────────────
//
// groupId: 'analytics-group' ← KHÁC với 'ui-service-group' của ui-service
// → Kafka gửi TẤT CẢ message tới CẢ HAI group độc lập
// → Không tranh, không mất message
async function startConsumer() {
    const kafka = new Kafka({
        clientId: 'analytics-service',
        brokers:  ['localhost:9092'],
        logLevel: 1, // ERROR only
    });

    // Đảm bảo topic tồn tại (analytics-service có thể start trước order-service)
    const admin = kafka.admin();
    await admin.connect();
    try {
        await admin.createTopics({
            topics: [{ topic: 'user-behavior-logs', numPartitions: 1, replicationFactor: 1 }],
            waitForLeaders: true,
        });
    } catch { /* topic already exists */ }
    await admin.disconnect();

    // ────────────────────────────────────────────────────────────────────
    // Đây là điểm mấu chốt:
    //   ui-service  dùng groupId: 'ui-service-group'
    //   analytics   dùng groupId: 'analytics-group'      ← khác nhau
    //
    // Kafka track OFFSET riêng cho mỗi groupId:
    //   analytics-group offset = bao nhiêu message group này đã đọc
    //   ui-service-group offset = bao nhiêu message group kia đã đọc
    //   → hoàn toàn độc lập, không ảnh hưởng nhau
    // ────────────────────────────────────────────────────────────────────
    const consumer = kafka.consumer({ groupId: 'analytics-group' });
    await consumer.connect();
    await consumer.subscribe({ topic: 'user-behavior-logs', fromBeginning: false });

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const event = JSON.parse(message.value.toString());
                recordEvent(event);
                logger.info({
                    trace_id: event.traceId || 'no-trace',
                    message: `[Analytics] Recorded: ${event.productId} x${event.quantity} | total=${stats.totalEvents}`,
                });
            } catch (err) {
                logger.error({ trace_id: 'SYSTEM', message: `[Analytics] Parse error: ${err.message}` });
            }
        },
    });

    logger.info({ trace_id: 'SYSTEM', message: 'Kafka consumer ready (groupId: analytics-group, topic: user-behavior-logs)' });
}

// ── REST API ──────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
    res.json({ status: 'ok', service: 'analytics-service', port: PORT });
});

// GET /analytics/summary — snapshot tổng hợp
app.get('/analytics/summary', (req, res) => {
    // Tính top 5 sản phẩm bán chạy nhất
    const topProducts = Object.entries(stats.byProduct)
        .sort(([, a], [, b]) => b.count - a.count)
        .slice(0, 5)
        .map(([productId, data]) => ({ productId, ...data }));

    // Phân bố theo giờ (sort key)
    const hourlyDistribution = Object.entries(stats.byHour)
        .sort(([a], [b]) => Number(a) - Number(b))
        .map(([hour, count]) => ({ hour: `${hour.padStart(2,'0')}:00`, count }));

    res.json({
        consumerGroup: 'analytics-group',    // ← nhắc nhớ đây là consumer group riêng
        totalEvents:   stats.totalEvents,
        totalQuantity: stats.totalQuantity,
        topProducts,
        hourlyDistribution,
        firstEventAt:  stats.firstEventAt,
        lastEventAt:   stats.lastEventAt,
    });
});

// GET /analytics/products/:productId — chi tiết 1 sản phẩm
app.get('/analytics/products/:productId', (req, res) => {
    const data = stats.byProduct[req.params.productId];
    if (!data) return res.status(404).json({ message: 'No data for this product' });
    res.json({ productId: req.params.productId, ...data });
});

// ── Start ─────────────────────────────────────────────────────────────────
async function start() {
    try {
        logger.info({ trace_id: 'SYSTEM', message: 'Starting analytics-service...' });
        await startConsumer();
        app.listen(PORT, () => {
            logger.info({ trace_id: 'SYSTEM', message: `Analytics Service ready on port ${PORT}` });
            console.log(`\n[Analytics] Ready → http://localhost:${PORT}`);
            console.log(`[Analytics] Summary → http://localhost:${PORT}/analytics/summary\n`);
        });
    } catch (err) {
        logger.error({ trace_id: 'SYSTEM', message: `Failed to start: ${err.message}` });
        process.exit(1);
    }
}

start();
