// order-service/config/circuitBreaker.js
//
// TẠI SAO CHỈ RETRY LÀ CHƯA ĐỦ?
//
// Kịch bản: inventory-service down hoàn toàn (server tắt, không recover)
//
//   Với retry (maxRetries=3, timeout=3s):
//   → Mỗi request mất: 3s + 3s + 3s = ~9 giây mới báo lỗi
//   → 500 user gọi đồng thời = 500 × 9 giây connection treo = server order-service treo
//   → Trong lúc đó, các request đến inventory đang sống vẫn bị ảnh hưởng
//
// Circuit Breaker giải quyết:
//   Sau khi phát hiện N lỗi liên tiếp → "ngắt cầu dao"
//   → Request tiếp theo fail NGAY LẬP TỨC (< 1ms) thay vì chờ 9 giây
//   → Server order-service không bị block, vẫn xử lý các việc khác
//   → Sau một khoảng thời gian, tự động "thử lại" 1 request để kiểm tra
//
// ===== SƠ ĐỒ TRẠNG THÁI =====
//
//     [Bình thường]              [failureCount >= threshold]
//       CLOSED  ──────────────────────────────────────► OPEN
//         ▲                                               │
//         │                                    [Sau recoveryTimeout]
//         │                                               │
//         └──── [1 request thành công] ───── HALF_OPEN ◄─┘
//                                           [Đang thử nghiệm]
//
// CLOSED   = hoạt động bình thường, mọi request được phép qua
// OPEN     = đang ngắt, mọi request bị từ chối ngay lập tức
// HALF_OPEN = cho 1 request qua để kiểm tra service đã recover chưa

const logger = require('./logger');

class CircuitBreaker {
    constructor(name, options = {}) {
        this.name = name;

        // Trạng thái ban đầu: bình thường
        this.state = 'CLOSED';

        // Đếm số lỗi liên tiếp (reset về 0 khi có request thành công)
        this.failureCount = 0;

        // Thời điểm xảy ra lỗi gần nhất (dùng để tính thời gian chờ)
        this.lastFailureTime = null;

        // Sau bao nhiêu lỗi liên tiếp thì ngắt → OPEN
        this.failureThreshold = options.failureThreshold ?? 3;

        // Ở trạng thái OPEN bao lâu trước khi thử → HALF_OPEN
        this.recoveryTimeout = options.recoveryTimeout ?? 10000; // 10 giây
    }

    // Bọc bất kỳ HTTP request nào vào circuit breaker
    async execute(requestFn, traceId = 'SYSTEM') {

        // ── Trạng thái OPEN: ngắt cầu dao, từ chối ngay ──
        if (this.state === 'OPEN') {
            const elapsed = Date.now() - this.lastFailureTime;

            if (elapsed < this.recoveryTimeout) {
                const remaining = Math.ceil((this.recoveryTimeout - elapsed) / 1000);

                logger.warn({
                    trace_id: traceId,
                    message: `[CB:${this.name}] OPEN — rejecting request immediately. Retry in ${remaining}s`
                });

                // Ném lỗi đặc biệt để route handler biết đây là circuit breaker từ chối
                // (không phải lỗi từ service thực sự)
                const err = new Error(`Service ${this.name} is temporarily unavailable`);
                err.circuitOpen = true;
                throw err;
            }

            // Đã chờ đủ thời gian → cho qua 1 request để test
            this.state = 'HALF_OPEN';
            logger.info({
                trace_id: traceId,
                message: `[CB:${this.name}] OPEN -> HALF_OPEN, probing with 1 request...`
            });
        }

        // ── Thực hiện request ──
        try {
            const result = await requestFn();
            this._onSuccess(traceId);
            return result;

        } catch (error) {
            // Không đếm lỗi circuit open (vòng lặp đệ quy) vào failure count
            if (!error.circuitOpen) {
                this._onFailure(traceId);
            }
            throw error;
        }
    }

    _onSuccess(traceId) {
        if (this.state === 'HALF_OPEN') {
            logger.info({
                trace_id: traceId,
                message: `[CB:${this.name}] HALF_OPEN -> CLOSED (service recovered)`
            });
        }
        this.state = 'CLOSED';
        this.failureCount = 0;
    }

    _onFailure(traceId) {
        this.failureCount++;
        this.lastFailureTime = Date.now();

        if (this.state === 'HALF_OPEN' || this.failureCount >= this.failureThreshold) {
            this.state = 'OPEN';
            logger.error({
                trace_id: traceId,
                message: `[CB:${this.name}] -> OPEN (${this.failureCount} consecutive failures)`
            });
        } else {
            logger.warn({
                trace_id: traceId,
                message: `[CB:${this.name}] Failure ${this.failureCount}/${this.failureThreshold}`
            });
        }
    }

    // Xem trạng thái hiện tại (dùng cho endpoint /circuit-status)
    status() {
        return {
            name: this.name,
            state: this.state,
            failureCount: this.failureCount,
            failureThreshold: this.failureThreshold,
            lastFailureAt: this.lastFailureTime
                ? new Date(this.lastFailureTime).toISOString()
                : null,
        };
    }
}

// ============================================================
// Singleton — dùng chung 1 instance cho toàn bộ order-service
// ============================================================
// Tại sao singleton?
// Nếu mỗi request tạo CircuitBreaker mới → failureCount luôn = 0
// → Không bao giờ đạt threshold → không bao giờ OPEN → vô nghĩa
//
// Singleton đảm bảo failureCount tích lũy qua các request
const inventoryBreaker = new CircuitBreaker('inventory-service', {
    failureThreshold: 3,     // ngắt sau 3 lỗi liên tiếp
    recoveryTimeout: 10000,  // thử lại sau 10 giây
});

module.exports = { inventoryBreaker };
