// inventory-service/src/routes/inventoryRoutes.js
const express = require('express');
const router = express.Router(); // Khởi tạo Router (Mini-app)

// Import các hạ tầng cần thiết
const logger = require('../config/logger');
// Import dữ liệu dùng chung
const mockInventory = require('../models/inventory');

// =========================================================================
// GIAO TIẾP ĐỒNG BỘ (HTTP SYNC - Phục vụ Order Service check kho)
// =========================================================================
// Chú ý: Route path ở đây là '/:productId' vì nó sẽ được mount vào '/inventory' ở file server.js
router.get('/:productId', (req, res) => {
    // 🔥 QUAN TRỌNG: Lấy Trace ID từ HTTP Header do Order Service gửi sang
    const traceId = req.headers['x-trace-id'] || 'SYSTEM-GEN';
    const { productId } = req.params;

    logger.info({ 
        trace_id: traceId, 
        message: `[HTTP Sync] Nhận yêu cầu check kho (route): ${productId}.` 
    });

    const product = mockInventory[productId];

    if (!product) {
        logger.warn({ trace_id: traceId, message: `[HTTP Sync] Sản phẩm ${productId} không tồn tại.` });
        return res.json({ productId, available: false });
    }

    // Kiểm tra kho thực tế (stock) có đủ hàng ko
    const isAvailable = product.stock > 0;

    if (isAvailable) {
        logger.info({ trace_id: traceId, message: `[HTTP Sync] Sản phẩm ${productId} còn hàng.` });
    } else {
        logger.warn({ trace_id: traceId, message: `[HTTP Sync] Sản phẩm ${productId} HẾT HÀNG.` });
    }

    res.json({ productId, available: isAvailable });
});

// Export Router để file server.js sử dụng
module.exports = router;