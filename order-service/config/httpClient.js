// order-service/config/httpClient.js
//
// TẠI SAO FILE NÀY TỒN TẠI?
//
// Vấn đề khi dùng axios.get() thẳng không cấu hình:
//
//   const res = await axios.get('http://inventory-service/...')
//
//   Nếu inventory-service không phản hồi (server bị treo, mạng đứt):
//   → axios mặc định KHÔNG có timeout → chờ mãi mãi
//   → Node.js giữ connection đó trong memory
//   → 1000 user gọi đồng thời → 1000 connection treo → server hết memory → crash
//   → user không nhận được response gì, chỉ thấy loading mãi
//
// File này cung cấp hai thứ:
//   1. httpClient: axios instance với timeout mặc định
//   2. requestWithRetry: tự động thử lại khi gặp lỗi tạm thời

const axios = require('axios');
const logger = require('./logger');

// Tạo axios instance với timeout 3 giây
// Nếu không nhận được response sau 3s → hủy request, trả lỗi về
const httpClient = axios.create({
    timeout: 3000,
});

// ============================================================
// requestWithRetry — Tự động thử lại khi gặp lỗi tạm thời
// ============================================================
//
// Tại sao cần retry?
//   Đôi khi service bị lỗi trong vài giây (deploy mới, GC pause, DB reconnect...)
//   Nếu retry sau 200ms, rất có thể thành công
//
// Tại sao dùng Exponential Backoff (delay tăng dần)?
//   Nếu service đang quá tải → retry ngay lập tức → tải thêm → tệ hơn
//   Exponential backoff cho service thời gian thở: 200ms, 400ms, 800ms...
//
// Tại sao KHÔNG retry lỗi 4xx?
//   400 Bad Request: data gửi sai format → retry cũng vẫn sai
//   404 Not Found: resource không tồn tại → retry không tạo ra resource
//   401/403: không có quyền → retry không tự cấp quyền
//   → Retry trong các trường hợp này là lãng phí và che giấu bug thực sự
//
// Chỉ retry lỗi tạm thời:
//   5xx (500, 503...): server đang lỗi, có thể tự recover
//   ECONNABORTED: timeout, server đang chậm tạm thời
//   ECONNREFUSED: service đang restart
//   429 Too Many Requests: bị rate limit, chờ rồi thử lại

async function requestWithRetry(requestFn, { maxRetries = 3, traceId = 'SYSTEM' } = {}) {
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const response = await requestFn();

            if (attempt > 1) {
                logger.info({
                    trace_id: traceId,
                    message: `[HTTP] Success after ${attempt} attempt(s).`
                });
            }

            return response;

        } catch (error) {
            lastError = error;
            const statusCode = error.response?.status;
            const errorCode = error.code;

            // Lỗi vĩnh viễn — không retry
            const isPermanentError = statusCode
                && statusCode >= 400
                && statusCode < 500
                && statusCode !== 429;

            if (isPermanentError) {
                logger.warn({
                    trace_id: traceId,
                    message: `[HTTP] Error ${statusCode} — permanent, not retrying.`
                });
                throw error;
            }

            // Hết lần retry
            if (attempt === maxRetries) {
                break;
            }

            // Lỗi tạm thời — thử lại với exponential backoff
            const delayMs = Math.pow(2, attempt - 1) * 200; // 200ms, 400ms, 800ms

            logger.warn({
                trace_id: traceId,
                message: `[HTTP] Attempt ${attempt}/${maxRetries} failed (${errorCode || statusCode}). Retrying in ${delayMs}ms...`
            });

            await sleep(delayMs);
        }
    }

    logger.error({
        trace_id: traceId,
        message: `[HTTP] All ${maxRetries} attempts failed.`
    });

    throw lastError;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = { httpClient, requestWithRetry };
