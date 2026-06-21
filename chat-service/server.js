// chat-service/server.js
//
// GIAI ĐOẠN 6 — WEBSOCKET / SOCKET.IO
//
// Tại sao cần WebSocket khi đã có SSE (ui-service:3004)?
//
//   SSE — Server-Sent Events (ONE-WAY):
//     Browser mở 1 HTTP connection → server ghi data xuống mãi mãi
//     Browser KHÔNG THỂ gửi data ngược lại qua connection đó
//     ✅ Phù hợp: order status, live metrics, notifications
//     ❌ Không làm được: chat, typing indicator, presence, interactive
//
//   WebSocket (service này — TWO-WAY):
//     Browser và server đồng ý UPGRADE từ HTTP sang WebSocket protocol
//     Sau upgrade: cả 2 bên đều có thể GỬI bất cứ lúc nào qua 1 TCP connection
//     ✅ Chat, typing indicators, read receipts, collaborative features
//     ✅ Kafka → Socket.IO bridge (admin nhận event VÀ có thể ACK lại)
//
// Architecture của service này:
//
//   [Kafka topics]
//     order-events ──┐
//   inventory-events─┼──► chat-service consumer ──► adminNS.emit('notification')
//    payment-events ─┘           │                        ↕ (bidirectional)
//                                │                   Admin browsers
//                          [Socket.IO]
//                         /chat namespace ◄──► User browsers (live support)
//                         /admin namespace ◄──► Admin browsers (order feed)
//
// Namespaces (Socket.IO concept — như route trong Express):
//   /chat   → hệ thống support chat (user ↔ nhân viên CSKH)
//   /admin  → feed sự kiện Kafka cho admin (đọc + có thể ACK)
//
// Start: node server.js
// Open:  http://localhost:3006
//
// Test typing indicator (không làm được với SSE):
//   Mở 2 tab, cùng join room "support-001"
//   Gõ text ở tab 1 → tab 2 thấy "X đang gõ..."
//   Với SSE: tab 1 không thể gửi "tôi đang gõ" lên server qua SSE connection

const express = require('express');
const http    = require('http');
const path    = require('path');
const { Server } = require('socket.io');
const { Kafka }  = require('kafkajs');
const logger     = require('./config/logger');

const app        = express();
const httpServer = http.createServer(app);

// Socket.IO chạy trên cùng HTTP server với Express
// Khi browser connect tới ws://localhost:3006/socket.io/... → Socket.IO xử lý
// Khi browser connect tới http://localhost:3006/... → Express xử lý
const io = new Server(httpServer, {
    cors: { origin: '*' },
});

const PORT = 3006;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ── In-memory state (production: dùng Redis để share state giữa nhiều instance) ──
const rooms               = new Map(); // roomId → Set<socketId>
const users               = new Map(); // socketId → { username, room }
const recentNotifications = [];        // buffer 200 Kafka events gần nhất

// ── REST endpoints ────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        service: 'chat-service',
        port: PORT,
        connectedUsers: users.size,
        activeRooms: rooms.size,
    });
});

// Xem danh sách rooms đang active
// curl http://localhost:3006/rooms
app.get('/rooms', (req, res) => {
    const list = [...rooms.entries()].map(([id, members]) => ({
        id,
        memberCount: members.size,
    }));
    res.json({ rooms: list, connectedUsers: users.size });
});

// ─────────────────────────────────────────────────────────────────────────
// NAMESPACE /chat — Live Support Chat
//
// Luồng demo:
//   1. User A (tab 1) join room "support-001" với username "khach"
//   2. User B (tab 2) join room "support-001" với username "agent"
//   3. User A gõ → User B thấy "khach đang gõ..." (typing indicator)
//   4. User A gửi tin → cả 2 nhận (bidirectional broadcast)
//
// So với SSE: tất cả bước 3 và 4 đều KHÔNG THỂ làm với SSE thuần vì
//   SSE không cho browser gửi data qua connection đó
// ─────────────────────────────────────────────────────────────────────────
const chatNS = io.of('/chat');

chatNS.on('connection', (socket) => {
    logger.info({ message: `[WS/chat] Connected: ${socket.id}` });

    // ── join ──────────────────────────────────────────────────────────────
    // Client gửi:  socket.emit('join', { username, roomId })
    // Server làm:  socket.join(roomId), thông báo cả room
    socket.on('join', ({ username, roomId }) => {
        if (!username?.trim() || !roomId?.trim()) return;

        // Rời room cũ nếu đang ở
        const prev = users.get(socket.id);
        if (prev?.room) {
            socket.leave(prev.room);
            const members = rooms.get(prev.room);
            if (members) members.delete(socket.id);
            if (members?.size === 0) rooms.delete(prev.room);
            chatNS.to(prev.room).emit('user:left', { username: prev.username });
        }

        // Vào room mới
        const safeUser = username.trim().slice(0, 30);
        const safeRoom = roomId.trim().slice(0, 50);

        socket.join(safeRoom);
        users.set(socket.id, { username: safeUser, room: safeRoom });

        if (!rooms.has(safeRoom)) rooms.set(safeRoom, new Set());
        rooms.get(safeRoom).add(socket.id);

        // Thông báo cả room: có người mới — kể cả người vừa join
        chatNS.to(safeRoom).emit('user:joined', {
            username: safeUser,
            roomId: safeRoom,
            memberCount: rooms.get(safeRoom).size,
        });

        logger.info({ message: `[WS/chat] ${safeUser} joined room ${safeRoom}` });
    });

    // ── message ───────────────────────────────────────────────────────────
    // Luồng: client gõ → socket.emit('message', { text })
    //        → server broadcast chatNS.to(room).emit('message', msg)
    //        → TẤT CẢ client trong room nhận ngay (kể cả người gửi → UI confirm)
    //
    // Với SSE: browser không thể gửi text lên qua SSE connection
    //          phải dùng 1 fetch POST riêng → server emit SSE → 1 chiều, phức tạp hơn
    socket.on('message', ({ text }) => {
        const user = users.get(socket.id);
        if (!user || !text?.trim()) return;

        const msg = {
            id:        Date.now().toString(),
            username:  user.username,
            text:      text.trim().slice(0, 500),
            roomId:    user.room,
            timestamp: new Date().toISOString(),
        };

        chatNS.to(user.room).emit('message', msg);
        logger.info({ message: `[WS/chat] Message in ${user.room} from ${user.username}` });
    });

    // ── typing ────────────────────────────────────────────────────────────
    // Pure WebSocket feature — không thể replicate với SSE
    //
    // Client gửi khi bắt đầu/dừng gõ:  socket.emit('typing', { isTyping: true/false })
    // Server relay tới các client khác trong room (không relay lại cho chính mình)
    socket.on('typing', ({ isTyping }) => {
        const user = users.get(socket.id);
        if (!user) return;

        // socket.to(...) = broadcast tới room TRỪ người gửi
        socket.to(user.room).emit('typing', {
            username: user.username,
            isTyping: Boolean(isTyping),
        });
    });

    // ── disconnect ────────────────────────────────────────────────────────
    socket.on('disconnect', () => {
        const user = users.get(socket.id);
        if (user?.room) {
            const members = rooms.get(user.room);
            if (members) {
                members.delete(socket.id);
                if (members.size === 0) rooms.delete(user.room);
            }
            chatNS.to(user.room).emit('user:left', { username: user.username });
        }
        users.delete(socket.id);
        logger.info({ message: `[WS/chat] Disconnected: ${socket.id}` });
    });
});

// ─────────────────────────────────────────────────────────────────────────
// NAMESPACE /admin — Kafka Event Feed (bidirectional)
//
// Luồng:
//   Kafka event arrive → adminNS.emit('notification', event)
//   Admin browser nhận ngay qua WebSocket
//   Admin có thể gửi lại 'notification:ack' → server log/process (bidirectional)
//
// Điểm khác so với SSE (ui-service):
//   SSE: admin chỉ nhận (one-way)
//   WS:  admin có thể acknowledge, filter, trigger action (two-way)
//
// Catch-up: admin reconnect sau 10 phút → nhận 50 event gần nhất
//   SSE có thể dùng Last-Event-ID nhưng cần server lưu và lookup
//   Socket.IO đơn giản hơn: emit history khi client connect
// ─────────────────────────────────────────────────────────────────────────
const adminNS = io.of('/admin');

adminNS.on('connection', (socket) => {
    logger.info({ message: `[WS/admin] Admin connected: ${socket.id}` });

    // Gửi lịch sử gần nhất cho admin vừa kết nối (catch-up)
    socket.emit('notifications:history', recentNotifications.slice(-50));

    // Admin có thể ACK event — bidirectional feature
    socket.on('notification:ack', ({ id, action }) => {
        logger.info({ message: `[WS/admin] ACK from ${socket.id}: event ${id}, action=${action}` });
        // Production: update DB, trigger workflow, v.v.
    });

    socket.on('disconnect', () => {
        logger.info({ message: `[WS/admin] Admin disconnected: ${socket.id}` });
    });
});

// ─────────────────────────────────────────────────────────────────────────
// KAFKA BRIDGE — Kafka topics → Socket.IO /admin namespace
//
// Subscribe: order-events, inventory-events, payment-events
//   (cùng topic với Kafka SAGA ở order-service/routes/orderRoutes.js)
//
// Đây là pattern phổ biến: "WebSocket Gateway"
//   Kafka: internal message bus giữa các microservice
//   Socket.IO: bridge đưa event ra browser
//   chat-service đóng vai "edge layer" — convert internal event sang WS
// ─────────────────────────────────────────────────────────────────────────
const kafka = new Kafka({
    clientId: 'chat-service',
    brokers: ['localhost:9092'],
    retry: { retries: 5, initialRetryTime: 1000 },
});

async function startKafkaBridge() {
    const consumer = kafka.consumer({ groupId: 'chat-service-admin-group' });

    try {
        await consumer.connect();
        logger.info({ message: '[Kafka] Connected — bridging to Socket.IO /admin' });

        await consumer.subscribe({
            topics: ['order-events', 'inventory-events', 'payment-events'],
            fromBeginning: false,
        });

        await consumer.run({
            eachMessage: async ({ topic, message }) => {
                let payload;
                try {
                    payload = JSON.parse(message.value.toString());
                } catch {
                    return; // bỏ qua message không parse được
                }

                const notification = {
                    id:         Date.now().toString(),
                    topic,
                    type:       payload.type || topic,
                    ...payload,
                    receivedAt: new Date().toISOString(),
                };

                // Buffer cho catch-up
                recentNotifications.push(notification);
                if (recentNotifications.length > 200) recentNotifications.shift();

                // Broadcast tới tất cả admin clients
                adminNS.emit('notification', notification);

                logger.info({
                    message: `[Kafka→WS] ${notification.type} (${topic})`,
                    type:    notification.type,
                    topic,
                });
            },
        });

    } catch (err) {
        // Kafka không sẵn sàng — service vẫn chạy, chỉ không có Kafka bridge
        logger.warn({ message: `[Kafka] Unavailable — chat still works standalone: ${err.message}` });
    }
}

// ── Demo endpoint ─────────────────────────────────────────────────────────
// Không cần Kafka chạy — nhấn nút trên UI để xem luồng order saga giả lập
// POST /demo/events → push 4 notification vào adminNS với delay
app.post('/demo/events', (req, res) => {
    const orderId   = 'ORD-' + Math.random().toString(36).slice(2, 8).toUpperCase();
    const productId = ['IPHONE-15', 'MACBOOK-M3', 'LAPTOP-MODULAR-TEST'][Math.floor(Math.random() * 3)];
    const quantity  = Math.floor(Math.random() * 3) + 1;

    const events = [
        { delay: 0,    topic: 'order-events',     type: 'order.created',         orderId, productId, quantity,  status: 'created' },
        { delay: 800,  topic: 'inventory-events',  type: 'inventory.reserved',    orderId, productId, quantity,  reserved: true },
        { delay: 1600, topic: 'payment-events',    type: 'payment.completed',     orderId, productId, amount: quantity * 999, currency: 'USD' },
        { delay: 2200, topic: 'order-events',      type: 'order.fulfilled',       orderId, status: 'fulfilled' },
    ];

    events.forEach(({ delay, topic, type, ...rest }) => {
        setTimeout(() => {
            const notification = {
                id:         Date.now().toString() + Math.random(),
                topic,
                type,
                ...rest,
                receivedAt: new Date().toISOString(),
                _demo:      true,
            };
            recentNotifications.push(notification);
            if (recentNotifications.length > 200) recentNotifications.shift();
            adminNS.emit('notification', notification);
        }, delay);
    });

    res.json({ ok: true, orderId, productId, quantity, message: '4 events sẽ xuất hiện trong ~2.2s' });
});

// ── Start ─────────────────────────────────────────────────────────────────
startKafkaBridge().catch(() => {});

httpServer.listen(PORT, () => {
    logger.info({ message: `[chat-service] Running on http://localhost:${PORT}` });
    logger.info({ message: `[chat-service] /chat namespace  → live support chat` });
    logger.info({ message: `[chat-service] /admin namespace → Kafka event feed` });
});
