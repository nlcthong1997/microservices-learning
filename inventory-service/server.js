// inventory-service/src/server.js
const express = require('express');
const logger = require('./config/logger');

// Import hạ tầng config
const { connectRabbit, getRabbitChannel } = require('./config/rabbit');
// Import dữ liệu dùng chung (cho consumers sử dụng)
const mockInventory = require('./models/inventory');

// Import route file mới (TÁCH BIỆT)
const inventoryRoutes = require('./routes/inventoryRoutes');

// Import gRPC server
const { startGrpcServer } = require('./config/grpcServer');

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


// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'ok', service: 'inventory-service', port: PORT });
});

// Fix startup bug — xem giải thích chi tiết trong order-service/server.js
async function start() {
    try {
        logger.info({ trace_id: 'SYSTEM', message: 'Đang kết nối RabbitMQ...' });

        await connectRabbit();
        await startOrderCreatedConsumer();
        await startSagaRollbackConsumer();

        // Khởi động gRPC Server (port 50051) SONG SONG với Express (port 3002)
        // Hai server độc lập — gRPC phục vụ các service nội bộ (binary, nhanh)
        //                    — Express phục vụ REST API (browser, curl, Postman)
        startGrpcServer();

        app.listen(PORT, () => {
            logger.info({
                trace_id: 'SYSTEM',
                message: `Inventory Service sẵn sàng tại cổng ${PORT} ✅`
            });
        });

    } catch (error) {
        logger.error({
            trace_id: 'SYSTEM',
            message: `Không thể khởi động: ${error.message}`
        });
        process.exit(1);
    }
}

start();