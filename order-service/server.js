// order-service/server.js
const express = require('express');
const logger = require('./config/logger');
const { connectRabbit } = require('./config/rabbit');
const { connectKafka } = require('./config/kafka');
const orderRoutes = require('./routes/orderRoutes');

const app = express();
const PORT = 3001;

app.use(express.json());
app.use('/orders', orderRoutes);

// Health check — dùng để kiểm tra service còn sống không
// Kubernetes, load balancer, và các service khác gọi endpoint này
// để biết có nên gửi traffic vào không
app.get('/health', (req, res) => {
    res.json({ status: 'ok', service: 'order-service', port: PORT });
});

// ============================================================
// GIAI ĐOẠN 1 — Fix startup bug
// ============================================================
//
// BUG trong code cũ:
//   app.listen(PORT, async () => {
//       await connectRabbit();  // ← chạy SAU KHI server đã nhận request
//   })
//
// Vấn đề:
//   app.listen() bắt đầu nhận TCP connection NGAY LẬP TỨC
//   Callback async chỉ chạy SAU KHI server đã lắng nghe
//   → Nếu request đến trong 1-2 giây đầu khi RabbitMQ chưa connect
//   → getRabbitChannel() trả về undefined → publish lỗi → mất message
//
// Fix: kết nối hạ tầng TRƯỚC, chỉ mở cổng KHI mọi thứ sẵn sàng

async function start() {
    try {
        logger.info({ trace_id: 'SYSTEM', message: 'Connecting to infrastructure...' });

        await connectRabbit();
        await connectKafka();

        // Chỉ bắt đầu nhận request SAU KHI hạ tầng sẵn sàng
        app.listen(PORT, () => {
            logger.info({
                trace_id: 'SYSTEM',
                message: `Order Service ready on port ${PORT}`
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