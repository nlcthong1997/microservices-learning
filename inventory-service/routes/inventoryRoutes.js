// inventory-service/src/routes/inventoryRoutes.js
const express = require('express');
const router = express.Router();

const logger = require('../config/logger');
const { checkStock } = require('../services/inventoryService');

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
        logger.warn({ trace_id: traceId, message: '[TEST] Simulating slow response (5s)...' });
        return setTimeout(() => {
            res.json({ productId, available: true, note: 'slow response simulated' });
        }, 5000);
    }

    if (productId === 'TRIGGER-ERROR') {
        logger.error({ trace_id: traceId, message: '[TEST] Simulating 500 error.' });
        return res.status(500).json({ message: 'Internal Server Error (simulated for testing)' });
    }
    // =========================================================
    // END TEST SCENARIOS
    // =========================================================

    logger.info({
        trace_id: traceId,
        message: `[HTTP Sync] Check stock request: ${productId}.`
    });

    const result = checkStock(productId, 1);

    if (!result.found) {
        logger.warn({ trace_id: traceId, message: `[HTTP Sync] Product ${productId} not found.` });
        return res.status(404).json({ productId, available: false });
    }

    if (result.available) {
        logger.info({ trace_id: traceId, message: `[HTTP Sync] ${productId} in stock (available: ${result.stock}).` });
    } else {
        logger.warn({ trace_id: traceId, message: `[HTTP Sync] ${productId} OUT OF STOCK.` });
    }

    res.json({ productId, available: result.available, stock: result.stock });
});

module.exports = router;