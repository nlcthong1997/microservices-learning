// inventory-service/src/server.js
const express = require('express');
const logger = require('./config/logger');

// Import hạ tầng config
const { connectRabbit, getRabbitChannel } = require('./config/rabbit');
// Import dữ liệu dùng chung (cho consumers sử dụng)
const mockInventory = require('./models/inventory');

// Import route file mới (TÁCH BIỆT)
const inventoryRoutes = require('./routes/inventoryRoutes');

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
    const ORDER_QUEUE = 'inventory_order_created_queue';

    if (!channel) return;

    logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Consumer 'order.created' bắt đầu lắng nghe...` });

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
                logger.info({ trace_id: traceId, message: `[RabbitMQ Async] RESERVE thành công.` });
                channel.ack(msg);
            } else {
                logger.error({ trace_id: traceId, message: `[RabbitMQ Async] RESERVE thất bại.` });
                channel.nack(msg, false, false);
            }
        }
    });
}

// LUỒNG 2B: Consumer cho SAGA BÙ (Nhận thanh toán fail)
async function startSagaRollbackConsumer() {
    const channel = getRabbitChannel();
    const SAGA_ROLLBACK_QUEUE = 'inventory_saga_rollback_queue';

    if (!channel) return;

    logger.info({ trace_id: 'SYSTEM', message: `RabbitMQ: Consumer SAGA 'payment.failed' bắt đầu lắng nghe...` });

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
                logger.info({ trace_id: traceId, message: `[SAGA BU] ROLLBACK thành công.` });
            }
            channel.ack(msg);
        }
    });
}


// --- KHỞI ĐỘNG SERVER ---
app.listen(PORT, async () => {
    logger.info({ trace_id: 'SYSTEM', message: `Inventory Service (Modular) chạy tại cổng ${PORT}` });
    
    // Đợi kết nối hạ tầng OK
    try {
        await connectRabbit();
        // Bật các Consumer lên lắng nghe ngầm
        await startOrderCreatedConsumer();
        await startSagaRollbackConsumer();
        logger.info({ trace_id: 'SYSTEM', message: 'Hạ tầng RabbitMQ sẵn sàng.' });
    } catch (error) {
        logger.error({ trace_id: 'SYSTEM', message: 'Lỗi khởi tạo RabbitMQ. Server dừng.' });
        process.exit(1);
    }
});