// inventory-service/config/db.js
//
// Connection Pool tới PostgreSQL
//
// Tại sao dùng Pool thay vì tạo connection mới mỗi request?
//
//   Tạo connection mới mỗi lần:
//     - TCP handshake + TLS + PostgreSQL auth = ~20-50ms mỗi request
//     - 1000 req/s = 1000 connection = PostgreSQL quá tải
//
//   Connection Pool:
//     - Tạo sẵn N connection khi khởi động (default: 10)
//     - Mỗi query mượn connection từ pool, trả lại sau khi xong
//     - 1000 req/s nhưng chỉ dùng tối đa 10 connection
//
// Cách dùng:
//   const { query, pool } = require('./db');
//   const result = await query('SELECT * FROM inventory WHERE product_id = $1', ['IPHONE-15']);

const { Pool } = require('pg');

const pool = new Pool({
    host:     process.env.PG_HOST     || 'localhost',
    port:     parseInt(process.env.PG_PORT || '5432', 10),
    database: process.env.PG_DATABASE || 'inventory_db',
    user:     process.env.PG_USER     || 'inventory_user',
    password: process.env.PG_PASSWORD || 'inventory_pass',
    max:      10,          // tối đa 10 connection trong pool
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

// Kiểm tra kết nối khi khởi động
pool.on('error', (err) => {
    console.error('[DB] Unexpected error on idle client:', err.message);
});

/**
 * Wrapper tiện dụng — không cần acquire/release client thủ công cho queries đơn
 */
async function query(text, params) {
    const start = Date.now();
    const res   = await pool.query(text, params);
    const dur   = Date.now() - start;
    if (dur > 200) console.warn(`[DB] Slow query (${dur}ms):`, text.substring(0, 80));
    return res;
}

/**
 * Dùng khi cần transaction (nhiều query phải thành công hoặc rollback cùng nhau)
 * Ví dụ: trừ kho + ghi order log — nếu 1 cái fail, cả 2 phải rollback
 */
async function withTransaction(fn) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const result = await fn(client);
        await client.query('COMMIT');
        return result;
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
}

async function connectDB() {
    const client = await pool.connect();
    client.release();
    console.log('[DB] PostgreSQL connected (pool ready)');
}

module.exports = { query, withTransaction, connectDB, pool };
