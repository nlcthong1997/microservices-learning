// inventory-service/src/routes/inventoryRoutes.js
const express = require('express');
const router = express.Router();

const logger = require('../config/logger');
const mockInventory = require('../models/inventory');

// =========================================================================
// ROUTE CHÍNH — check kho (HTTP Sync)
// =========================================================================
router.get('/:productId', (req, res) => {
    const traceId = req.headers['x-trace-id'] || 'SYSTEM-GEN';
    const { productId } = req.params;

    // =========================================================
    // TEST SCENARIOS — xóa hoặc bảo vệ bằng auth khi deploy production
    // =========================================================
    //
    // Các productId đặc biệt để test resilience trong order-service:
    //
    //   TRIGGER-SLOW  → giả lập service chậm (5 giây)
    //                   Test: timeout trong order-service
    //
    //   TRIGGER-ERROR → giả lập lỗi 500 mỗi lần
    //                   Test: retry logic và circuit breaker
    //
    // Cách test:
    //   # Test timeout (order-service abort sau 3s, inventory trả sau 5s)
    //   curl -X POST http://localhost:3001/orders/sync-resilient \
    //     -H "Content-Type: application/json" \
    //     -d '{"productId":"TRIGGER-SLOW","quantity":1}'
    //   # → 504 Gateway Timeout
    //
    //   # Test retry + circuit breaker (gọi 4 lần liên tiếp)
    //   for i in 1 2 3 4; do
    //     curl -s -X POST http://localhost:3001/orders/sync-resilient \
    //       -H "Content-Type: application/json" \
    //       -d '{"productId":"TRIGGER-ERROR","quantity":1}' | python -m json.tool
    //   done
    //   # Lần 1-3: retry rồi fail → circuit breaker đếm lỗi
    //   # Lần 4: circuit OPEN → fail ngay (< 100ms)
    //
    //   # Xem trạng thái circuit breaker sau khi test
    //   curl http://localhost:3001/orders/circuit-status

    if (productId === 'TRIGGER-SLOW') {
        logger.warn({ trace_id: traceId, message: '[TEST] Giả lập slow response 5 giây...' });
        return setTimeout(() => {
            res.json({ productId, available: true, note: 'slow response' });
        }, 5000);
    }

    if (productId === 'TRIGGER-ERROR') {
        logger.error({ trace_id: traceId, message: '[TEST] Giả lập lỗi 500.' });
        return res.status(500).json({ message: 'Internal Server Error (giả lập để test)' });
    }
    // =========================================================
    // END TEST SCENARIOS
    // =========================================================

    logger.info({
        trace_id: traceId,
        message: `[HTTP Sync] Nhận yêu cầu check kho: ${productId}.`
    });

    const product = mockInventory[productId];

    if (!product) {
        logger.warn({ trace_id: traceId, message: `[HTTP Sync] Sản phẩm ${productId} không tồn tại.` });
        return res.json({ productId, available: false });
    }

    const isAvailable = product.stock > 0;

    if (isAvailable) {
        logger.info({ trace_id: traceId, message: `[HTTP Sync] ${productId} còn hàng (stock: ${product.stock}).` });
    } else {
        logger.warn({ trace_id: traceId, message: `[HTTP Sync] ${productId} HẾT HÀNG.` });
    }

    res.json({ productId, available: isAvailable, stock: product.stock });
});

module.exports = router;