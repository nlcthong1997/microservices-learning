// ui-service/server.js
//
// Visual Live Dashboard — nhận event từ Kafka + RabbitMQ, push xuống browser qua SSE
//
// Cách hoạt động (SSE — Server-Sent Events):
//   Browser mở 1 HTTP connection tới GET /stream (EventSource)
//   Server GIỮ connection đó mở mãi
//   Mỗi khi có Kafka/RabbitMQ event → server ghi "data: {...}\n\n" vào connection
//   Browser nhận ngay lập tức — không cần polling, không cần WebSocket
//
// Start: node server.js
// Open:  http://localhost:3004

const express  = require('express');
const path     = require('path');
const { Kafka } = require('kafkajs');
const amqplib  = require('amqplib');

const app  = express();
const PORT = 3004;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ── Proxy: Visual Dashboard → order-service (avoid CORS: browser is :3004, order-service is :3001)
app.post('/api/orders/:route', async (req, res) => {
    try {
        const upstream = await fetch(`http://localhost:3001/orders/${req.params.route}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(req.body),
        });
        const data = await upstream.json().catch(() => ({}));
        res.status(upstream.status).json(data);
    } catch (err) {
        res.status(503).json({ error: err.message, code: 'UPSTREAM_ERROR' });
    }
});

app.get('/api/circuit-status', async (req, res) => {
    try {
        const upstream = await fetch('http://localhost:3001/orders/circuit-status');
        const data = await upstream.json();
        res.json(data);
    } catch (err) {
        res.status(503).json({ state: 'UNKNOWN', error: err.message });
    }
});

// ── Proxy: health endpoints ───────────────────────────────────────────────
app.get('/api/health/order', async (req, res) => {
    try {
        const upstream = await fetch('http://localhost:3001/health');
        const data = await upstream.json().catch(() => ({}));
        res.status(upstream.status).json(data);
    } catch (err) {
        res.status(503).json({ error: err.message });
    }
});

app.get('/api/health/inventory', async (req, res) => {
    try {
        const upstream = await fetch('http://localhost:3002/health');
        const data = await upstream.json().catch(() => ({}));
        res.status(upstream.status).json(data);
    } catch (err) {
        res.status(503).json({ error: err.message });
    }
});

// ── Proxy: inventory-service ──────────────────────────────────────────────
app.get('/api/inventory/:productId', async (req, res) => {
    try {
        const upstream = await fetch(`http://localhost:3002/inventory/${req.params.productId}`);
        const data = await upstream.json().catch(() => ({}));
        res.status(upstream.status).json(data);
    } catch (err) {
        res.status(503).json({ error: err.message });
    }
});

// ── Proxy: analytics-service ──────────────────────────────────────────────
app.get('/api/analytics/summary', async (req, res) => {
    try {
        const upstream = await fetch('http://localhost:3005/analytics/summary');
        const data = await upstream.json().catch(() => ({}));
        res.status(upstream.status).json(data);
    } catch (err) {
        res.status(503).json({ error: err.message });
    }
});

app.get('/api/analytics/products/:productId', async (req, res) => {
    try {
        const upstream = await fetch(`http://localhost:3005/analytics/products/${req.params.productId}`);
        const data = await upstream.json().catch(() => ({}));
        res.status(upstream.status).json(data);
    } catch (err) {
        res.status(503).json({ error: err.message });
    }
});

// ── SSE client registry ───────────────────────────────────────────────────
const clients = new Set();

app.get('/stream', (req, res) => {
    res.setHeader('Content-Type',  'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection',    'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.flushHeaders();

    clients.add(res);
    console.log('[UI] SSE client connected. Total: ' + clients.size);

    const kv = setInterval(() => res.write(':keepalive\n\n'), 20000);
    req.on('close', () => {
        clearInterval(kv);
        clients.delete(res);
        console.log('[UI] SSE client disconnected. Total: ' + clients.size);
    });
});

function broadcast(payload) {
    const msg = 'data: ' + JSON.stringify(payload) + '\n\n';
    clients.forEach(res => res.write(msg));
}

// ── Kafka Consumer ────────────────────────────────────────────────────────
async function startKafkaConsumer() {
    const kafka = new Kafka({
        clientId: 'ui-service',
        brokers: ['localhost:9092'],
        logLevel: 1,
    });

    const admin = kafka.admin();
    await admin.connect();
    try {
        await admin.createTopics({
            topics: [
                { topic: 'order-events',        numPartitions: 1, replicationFactor: 1 },
                { topic: 'inventory-events',     numPartitions: 1, replicationFactor: 1 },
                { topic: 'payment-events',       numPartitions: 1, replicationFactor: 1 },
                { topic: 'user-behavior-logs',   numPartitions: 1, replicationFactor: 1 },
            ],
            waitForLeaders: true,
        });
    } catch { /* topics already exist */ }
    await admin.disconnect();

    const consumer = kafka.consumer({ groupId: 'ui-service-group' });
    await consumer.connect();
    await consumer.subscribe({
        topics: ['order-events', 'inventory-events', 'payment-events', 'user-behavior-logs'],
        fromBeginning: false,
    });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                const event = JSON.parse(message.value.toString());
                // user-behavior-logs không có field `type` — thêm vào để browser nhận diện
                const type = event.type || (topic === 'user-behavior-logs' ? 'user.behavior' : undefined);
                broadcast({ source: 'kafka', topic, ...event, type, receivedAt: new Date().toISOString() });
            } catch { }
        },
    });

    console.log('[UI] Kafka consumer ready — watching order-events, inventory-events, payment-events, user-behavior-logs');
}

// ── RabbitMQ Observer ─────────────────────────────────────────────────────
// Bind exclusive queues vào các fanout exchanges để nhận BẢN SAO messages
// exclusive: true → queue tự xóa khi connection đóng, không ảnh hưởng consumer thật
async function startRabbitObserver() {
    try {
        const conn = await amqplib.connect('amqp://localhost:5672');
        const ch   = await conn.createChannel();

        const targets = [
            ['order_events',             'fanout', false, 'rmq.order.created'       ],
            ['order_events_dlx',         'fanout', true,  'rmq.order.dlq'           ],
            ['inventory_events',         'fanout', false, 'rmq.inventory.reserved'  ],
            ['payment_completed_events', 'fanout', false, 'rmq.payment.completed'   ],
            ['saga_events',              'fanout', false, 'rmq.saga.rollback'       ],
        ];

        for (const [name, type, durable, eventType] of targets) {
            await ch.assertExchange(name, type, { durable });
            const { queue } = await ch.assertQueue('', { exclusive: true });
            await ch.bindQueue(queue, name, '');

            ch.consume(queue, (msg) => {
                if (!msg) return;
                try {
                    const content = JSON.parse(msg.content.toString());
                    broadcast({ source: 'rabbitmq', type: eventType, exchange: name, ...content, receivedAt: new Date().toISOString() });
                } catch {
                    broadcast({ source: 'rabbitmq', type: eventType, exchange: name, receivedAt: new Date().toISOString() });
                }
                ch.ack(msg);
            }, { noAck: false });
        }

        console.log('[UI] RabbitMQ observer ready (order_events, order_events_dlx, inventory_events, payment_completed_events, saga_events)');
    } catch (err) {
        console.warn('[UI] RabbitMQ observer skipped: ' + err.message);
    }
}

// ── Start ─────────────────────────────────────────────────────────────────
async function start() {
    try {
        await startKafkaConsumer();
        await startRabbitObserver();
        app.listen(PORT, () => {
            console.log('\n[UI] Dashboard ready → http://localhost:' + PORT + '\n');
        });
    } catch (err) {
        console.error('[UI] Failed to start:', err.message);
        process.exit(1);
    }
}

start();
