// order-service/server.js
const express = require('express');
const logger = require('./config/logger'); // Import từ config mới

// Import hạ tầng config
const { connectRabbit } = require('./config/rabbit');
const { connectKafka } = require('./config/kafka');

// Import route file mới
const orderRoutes = require('./routes/orderRoutes');

const app = express();
const PORT = 3001;

app.use(express.json());

// --- MOUNT ROUTES ---
// Mọi route trong file orderRoutes.js sẽ bắt đầu bằng /orders
app.use('/orders', orderRoutes);

// --- KHỞI ĐỘNG SERVER ---
app.listen(PORT, async () => {
    logger.info({ trace_id: 'SYSTEM', message: `Order Service (Modular) chạy tại cổng ${PORT}` });
    
    // Đợi kết nối hạ tầng OK thì mới bắt đầu nhận request (best practice)
    try {
        await connectRabbit();
        await connectKafka();
        logger.info({ trace_id: 'SYSTEM', message: 'Hạ tầng OK. Sẵn sàng nhận request đặt hàng.' });
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: 'Lỗi khởi tạo hạ tầng. Server dừng.' });
        process.exit(1);
    }
});