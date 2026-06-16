// inventory-service/services/inventoryService.js
//
// BUSINESS LOGIC LAYER — nguồn sự thật duy nhất cho nghiệp vụ kho
//
// ── Thay đổi so với phiên bản cũ ────────────────────────────────────────────
//   Trước: đọc từ mockInventory object (in-memory, mất khi restart)
//   Sau:   đọc/ghi từ PostgreSQL (persistent, concurrent-safe)
//
// ── Race Condition & SELECT FOR UPDATE ──────────────────────────────────────
//
//   Vấn đề race condition khi 2 request đồng thời trừ kho:
//
//     Request A                      Request B
//     SELECT stock → 1               SELECT stock → 1   ← cùng thấy stock=1
//     UPDATE stock = 1-1 = 0
//                                    UPDATE stock = 1-1 = 0  ← stock âm!
//     → 2 đơn confirmed nhưng chỉ còn 1 sản phẩm → oversold
//
//   Giải pháp: SELECT ... FOR UPDATE
//     Request A: SELECT ... FOR UPDATE → lock row
//     Request B: SELECT ... FOR UPDATE → BỊ CHẶN, chờ A xong
//     Request A: UPDATE stock = 0, COMMIT → release lock
//     Request B: tiếp tục → thấy stock = 0 → trả out of stock
//
//   Kết quả: không bao giờ oversold dù 1000 request đồng thời

const { query, withTransaction } = require('../config/db');

/**
 * Check kho — READ ONLY, không lock
 * Dùng cho: GET /inventory/:productId (không trừ kho)
 */
async function checkStock(productId, quantity = 1) {
    const result = await query(
        'SELECT stock, reserved FROM inventory WHERE product_id = $1',
        [productId]
    );

    if (result.rows.length === 0) {
        return { found: false, available: false, stock: 0,
                 message: `Product not found: ${productId}` };
    }

    const { stock, reserved }  = result.rows[0];
    const availableStock       = stock - reserved;
    const available            = availableStock >= quantity;

    return {
        found: true, available,
        stock: availableStock,
        message: available
            ? `In stock (${availableStock} units available)`
            : `Out of stock (only ${availableStock} left, requested ${quantity})`,
    };
}

/**
 * Reserve kho — WRITE, dùng SELECT FOR UPDATE để tránh race condition
 * Dùng cho: Kafka SAGA & RabbitMQ async consumer
 *
 * @returns {{ success: boolean, stock: number, reason?: string }}
 */
async function reserveStock(productId, quantity) {
    return withTransaction(async (client) => {
        // SELECT FOR UPDATE: lock row này lại cho tới khi transaction COMMIT
        // Nếu có request khác đang chạy cùng → chờ lock được giải phóng
        const result = await client.query(
            `SELECT stock, reserved
             FROM inventory
             WHERE product_id = $1
             FOR UPDATE`,          // ← đây là chìa khoá tránh race condition
            [productId]
        );

        if (result.rows.length === 0) {
            return { success: false, stock: 0, reason: 'product_not_found' };
        }

        const { stock, reserved } = result.rows[0];
        const available           = stock - reserved;

        if (available < quantity) {
            return { success: false, stock: available, reason: 'out_of_stock' };
        }

        // Tăng reserved (không xoá stock thật cho đến khi payment confirm)
        await client.query(
            `UPDATE inventory
             SET reserved = reserved + $1
             WHERE product_id = $2`,
            [quantity, productId]
        );

        return { success: true, stock: available - quantity };
    });
}

/**
 * Release reserve — hoàn lại kho khi SAGA rollback (payment.failed)
 * Dùng trong compensating transaction
 */
async function releaseReserve(productId, quantity) {
    await query(
        `UPDATE inventory
         SET reserved = GREATEST(reserved - $1, 0)
         WHERE product_id = $2`,
        [quantity, productId]
    );
}

/**
 * Confirm sale — trừ kho thật khi payment thành công
 * (tuỳ thiết kế — hiện tại project không dùng nhưng để sẵn)
 */
async function confirmSale(productId, quantity) {
    await query(
        `UPDATE inventory
         SET stock    = stock    - $1,
             reserved = GREATEST(reserved - $1, 0)
         WHERE product_id = $2`,
        [quantity, productId]
    );
}

module.exports = { checkStock, reserveStock, releaseReserve, confirmSale };

