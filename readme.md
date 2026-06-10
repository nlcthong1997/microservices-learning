# Microservice Learning 2026

Dự án học microservice thực chiến — xây dựng từng giai đoạn, hiểu từng quyết định thiết kế, biết tại sao làm như vậy thay vì chỉ copy code.

---

## Mục lục

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Tại sao cần microservice?](#2-tại-sao-cần-microservice)
3. [Kiến trúc hệ thống](#3-kiến-trúc-hệ-thống)
4. [Cấu trúc thư mục](#4-cấu-trúc-thư-mục)
5. [Hạ tầng — Docker Compose](#5-hạ-tầng--docker-compose)
6. [Giai đoạn 1 — HTTP Sync cơ bản](#6-giai-đoạn-1--http-sync-cơ-bản)
7. [Giai đoạn 2 — HTTP Resilience](#7-giai-đoạn-2--http-resilience)
8. [Giai đoạn 3 — RabbitMQ Async](#8-giai-đoạn-3--rabbitmq-async)
9. [Giai đoạn 4 — Distributed Tracing & Logging](#9-giai-đoạn-4--distributed-tracing--logging)
10. [Giai đoạn 5 — Kafka Stream](#10-giai-đoạn-5--kafka-stream)
11. [Thuật ngữ](#11-thuật-ngữ)
12. [Dashboard & Monitoring](#12-dashboard--monitoring)
13. [Chạy dự án](#13-chạy-dự-án)

---

## 1. Tổng quan dự án

Dự án mô phỏng hệ thống **đặt hàng e-commerce** với 2 service chính, dần dần được trang bị các cơ chế thực tế mà mọi hệ thống microservice production đều cần.

**Luồng nghiệp vụ đơn giản:**
```
User đặt hàng → check kho → trừ kho → ghi nhận analytics
```

**Điều thú vị không phải ở nghiệp vụ, mà ở cách các bước này giao tiếp với nhau.** Cùng một luồng có thể implement theo nhiều cách với trade-off hoàn toàn khác nhau — và dự án này demo tất cả các cách đó để bạn thấy sự khác biệt.

---

## 2. Tại sao cần microservice?

### Vấn đề của Monolith khi hệ thống lớn lên

Hãy tưởng tượng toàn bộ hệ thống đặt hàng, kho, thanh toán, email, analytics nằm trong **một codebase duy nhất**:

```
monolith/
  ├── orderController.js
  ├── inventoryController.js
  ├── paymentController.js
  ├── emailService.js
  └── analyticsService.js
```

**Các vấn đề thực tế:**

| Vấn đề | Biểu hiện |
|---|---|
| **Deploy chậm** | Sửa 1 dòng email template → phải deploy lại toàn bộ hệ thống → downtime |
| **Scale không linh hoạt** | Analytics cần CPU nhiều → phải scale cả monolith lên → lãng phí |
| **Team đụng code** | Team A sửa order, team B sửa payment → conflict liên tục |
| **Lỗi lan rộng** | Bug trong analytics → có thể crash cả hệ thống đặt hàng |
| **Tech lock-in** | Muốn dùng Python cho ML → không được, toàn bộ đang là Node.js |

### Microservice giải quyết như thế nào

Tách thành các service độc lập:

```
order-service     → Node.js, deploy riêng, scale riêng
inventory-service → Node.js, deploy riêng, scale riêng
payment-service   → Go (nếu muốn), team khác sở hữu
analytics-service → Python (nếu muốn), scale nhiều CPU
```

**Nhưng microservice không phải là silver bullet.** Nó giải quyết vấn đề coupling của monolith nhưng tạo ra vấn đề mới: **các service cần giao tiếp qua mạng** — và mạng thì không đáng tin cậy.

> Đây chính là lý do mọi thứ trong dự án này tồn tại: timeout, retry, circuit breaker, message queue, distributed tracing — tất cả là để đối phó với việc giao tiếp qua mạng.

---

## 3. Kiến trúc hệ thống

```
┌────────────────────────────────────────────────────────────┐
│                    Client (curl / Postman)                   │
└─────────────────────────┬──────────────────────────────────┘
                          │ HTTP
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    order-service (:3001)                      │
│                                                               │
│  POST /orders/sync           → HTTP trực tiếp (cơ bản)       │
│  POST /orders/sync-resilient → HTTP + Timeout + Retry + CB    │
│  POST /orders/async          → Publish RabbitMQ               │
│  POST /orders/stream         → Publish Kafka                  │
│  GET  /orders/circuit-status → Xem trạng thái circuit breaker │
│  GET  /health                → Health check                   │
└──────┬──────────────────────────┬───────────────────────────┘
       │ HTTP (sync)              │ Publish (async)
       ▼                          ▼
┌─────────────────┐     ┌──────────────────┐     ┌───────────────────┐
│inventory-service│     │    RabbitMQ       │     │      Kafka         │
│   (:3002)       │     │ Exchange: fanout  │     │ Topic: user-       │
│                 │     │ order_events      │     │ behavior-logs      │
│ GET /inventory/ │     │ Queue:            │     │                    │
│    :productId   │◄────┤ inventory_order_  │     │ (analytics-service │
│ GET /health     │     │ created_queue     │     │  consume ở đây)    │
│                 │     │ saga_rollback_    │     │                    │
│ TRIGGER-SLOW    │     │ queue             │     └───────────────────┘
│ TRIGGER-ERROR   │     └──────────────────┘
└─────────────────┘

Observability:
  Winston → Loki (:3100) → Grafana (:3000)
  RabbitMQ Management (:15672)
  Kafka UI (:8080)
```

### Tại sao order-service dùng HTTP client (axios) để gọi inventory-service?

Câu hỏi quan trọng. Câu trả lời ngắn:

> Vì hai service là **hai process riêng biệt trên mạng**. Chúng không thể `require()` code của nhau như trong monolith. Muốn lấy dữ liệu → phải gọi qua mạng → cần HTTP client.

**Express và Axios phục vụ hai vai trò hoàn toàn khác nhau:**

| | Express | Axios |
|---|---|---|
| Vai trò | Lắng nghe và xử lý request đến | Gửi request đi service khác |
| Hướng traffic | Client → Service | Service → Service khác |
| Ví dụ | `app.post('/orders', handler)` | `axios.get('http://inventory:3002/...')` |

---

## 4. Cấu trúc thư mục

```
microservice-learning-2026/
│
├── docker-compose.yml          ← Toàn bộ hạ tầng (RabbitMQ, Kafka, Loki, Grafana)
├── loki-config.yaml            ← Cấu hình Loki log storage
├── .gitignore
│
├── docs/
│   ├── rabbitmq.md             ← Hướng dẫn toàn diện RabbitMQ
│   └── kafka.md                ← Hướng dẫn toàn diện Kafka
│
├── order-service/              ← Service nhận đơn hàng từ user
│   ├── server.js               ← Điểm khởi động, kết nối hạ tầng
│   ├── package.json
│   ├── config/
│   │   ├── logger.js           ← Winston logger → Loki
│   │   ├── rabbit.js           ← Kết nối RabbitMQ, tạo exchange
│   │   ├── kafka.js            ← Kết nối Kafka producer
│   │   ├── httpClient.js       ← Axios với timeout + retry logic    [GĐ2]
│   │   └── circuitBreaker.js   ← Circuit breaker pattern            [GĐ2]
│   └── routes/
│       └── orderRoutes.js      ← Tất cả route /orders/*
│
└── inventory-service/          ← Service quản lý kho hàng
    ├── server.js               ← Điểm khởi động + RabbitMQ consumers
    ├── package.json
    ├── config/
    │   ├── logger.js           ← Winston logger → Loki
    │   └── rabbit.js           ← Kết nối RabbitMQ, setup queue/binding
    ├── models/
    │   └── inventory.js        ← Dữ liệu kho giả (thay cho database)
    └── routes/
        └── inventoryRoutes.js  ← Route /inventory/:productId + test triggers
```

`[GĐ2]` — được thêm ở Giai đoạn 2. Mỗi giai đoạn thêm một lớp phức tạp có lý do rõ ràng, không phải thêm cho có.

---

## 5. Hạ tầng — Docker Compose

```bash
docker compose up -d
```

| Service | URL | Mục đích |
|---|---|---|
| **RabbitMQ** | `amqp://localhost:5672` | Message broker cho async communication |
| **RabbitMQ UI** | http://localhost:15672 (guest/guest) | Xem queue, exchange, message rate |
| **Kafka** | `localhost:9092` | Event streaming platform |
| **Kafka UI** | http://localhost:8080 | Xem topic, consumer group, offset |
| **Loki** | `http://localhost:3100` | Thu thập log từ các service |
| **Grafana** | http://localhost:3000 (admin/admin) | Visualize log, query theo traceId |

---

## 6. Giai đoạn 1 — HTTP Sync cơ bản

### Mục tiêu học

Hiểu giao tiếp đồng bộ giữa service là gì và vấn đề của nó.

### Route

```bash
POST /orders/sync
{"productId": "IPHONE-15", "quantity": 1}
```

### Cách hoạt động

```
User gọi POST /orders/sync
         ↓
order-service nhận request
         ↓
axios.get('http://inventory:3002/inventory/IPHONE-15')  ← DỪNG LẠI CHỜ
         ↓
inventory-service check kho → trả { available: true }
         ↓
order-service nhận kết quả → trả 200 về user
```

### Tại sao gọi là "sync" (đồng bộ)?

Vì order-service **dừng lại và chờ** kết quả từ inventory-service trước khi làm bất cứ điều gì tiếp theo. Giống như gọi điện cho ai đó và đứng chờ họ nhấc máy.

### Vấn đề thực tế

```javascript
// Code hiện tại — KHÔNG CÓ BẢO VỆ
const res = await axios.get('http://inventory:3002/inventory/' + productId);
```

**Scenario 1 — inventory-service không phản hồi:**
- axios không có timeout mặc định → chờ mãi
- 1000 user gọi đồng thời = 1000 connection treo trong memory
- order-service hết memory → crash

**Scenario 2 — inventory-service tạm thời chậm:**
- Không có retry → fail ngay lập tức

**Scenario 3 — inventory-service down hoàn toàn:**
- Mỗi request mất N giây timeout → hàng nghìn request treo đồng thời
- Làm chậm toàn bộ order-service dù không liên quan

### Hạn chế

Đây là code level 1 — đúng về logic nhưng không dùng được trong production. Tốt để học khái niệm cơ bản.

---

## 7. Giai đoạn 2 — HTTP Resilience

### Mục tiêu học

Hiểu tại sao cần timeout, retry, và circuit breaker — và cách chúng phối hợp với nhau.

### Route

```bash
POST /orders/sync-resilient
{"productId": "IPHONE-15", "quantity": 1}
```

### 3 lớp bảo vệ

```
Request đến
    ↓
[Lớp 1: Circuit Breaker]  ← Kiểm tra trạng thái inventory-service
    ↓ (CLOSED)
[Lớp 2: Retry]            ← Thử lại khi gặp lỗi tạm thời
    ↓
[Lớp 3: Timeout]          ← Hủy request nếu chờ quá 3 giây
    ↓
inventory-service
```

---

### Lớp 3 (trong cùng): Timeout

**File:** [order-service/config/httpClient.js](order-service/config/httpClient.js)

```javascript
const httpClient = axios.create({ timeout: 3000 }); // 3 giây
```

Sau 3 giây không nhận response → ném lỗi `ECONNABORTED` → retry layer bắt và xử lý.

---

### Lớp 2 (giữa): Retry với Exponential Backoff

**File:** [order-service/config/httpClient.js](order-service/config/httpClient.js)

**Tại sao Exponential Backoff (chờ tăng dần)?**

```
Retry đều nhau (SAI):   200ms → 200ms → 200ms
  Service quá tải → retry ngay → thêm tải → tệ hơn

Exponential Backoff (ĐÚNG):  200ms → 400ms → 800ms
  Cho service thời gian thở và recover
```

**Không retry lỗi 4xx (trừ 429):**

```
400 Bad Request:  data gửi sai → retry vẫn sai → lãng phí + che giấu bug
404 Not Found:    resource không tồn tại → retry không tạo ra resource
401/403:          không có quyền → retry không tự cấp quyền

Chỉ retry:  5xx, timeout, ECONNREFUSED, 429 (rate limited)
```

---

### Lớp 1 (ngoài cùng): Circuit Breaker

**File:** [order-service/config/circuitBreaker.js](order-service/config/circuitBreaker.js)

**Tại sao chỉ retry là chưa đủ?**

```
inventory-service down hoàn toàn:

Chỉ retry (maxRetries=3, timeout=3s):
  Mỗi request mất: 3s + 3s + 3s ≈ 9 giây
  500 user đồng thời = 500 × 9 giây = order-service tắc nghẽn

Với Circuit Breaker:
  Sau 3 lỗi liên tiếp → ngắt → OPEN
  Request tiếp theo fail ngay (< 1ms)
  order-service không bị block
```

**3 trạng thái:**

```
       CLOSED ─────[3 lỗi liên tiếp]─────► OPEN
         ▲                                    │
         │                        [Sau 10 giây]
         │                                    ▼
         └──[1 request thành công]────── HALF_OPEN
```

**Singleton là bắt buộc** — circuit breaker phải là một instance dùng chung, không tạo mới mỗi request.

### Test kịch bản

```bash
# 1. Bình thường
curl -X POST http://localhost:3001/orders/sync-resilient \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'

# 2. Timeout — inventory trả sau 5s, order-service hủy sau 3s → 504
curl -X POST http://localhost:3001/orders/sync-resilient \
  -H "Content-Type: application/json" \
  -d '{"productId":"TRIGGER-SLOW","quantity":1}'

# 3. Retry + Circuit Breaker — gọi 4 lần liên tiếp
#    Lần 1-3: retry rồi fail (chậm ~9s mỗi lần)
#    Lần 4: circuit OPEN → fail ngay (< 100ms)
for i in 1 2 3 4; do
  echo "=== Lần $i ===" && \
  curl -s -X POST http://localhost:3001/orders/sync-resilient \
    -H "Content-Type: application/json" \
    -d '{"productId":"TRIGGER-ERROR","quantity":1}'
  echo ""
done

# 4. Xem trạng thái circuit breaker
curl http://localhost:3001/orders/circuit-status
```

### Hạn chế

- State circuit breaker lưu **trong memory** — nếu có 3 instance order-service, mỗi cái có circuit riêng không chia sẻ.
- Production: lưu state trong **Redis** hoặc dùng thư viện `opossum`.

---

## 8. Giai đoạn 3 — RabbitMQ Async

### Mục tiêu học

Hiểu giao tiếp bất đồng bộ — tại sao cần, trade-off là gì, và cơ chế đảm bảo không mất message.

### Route

```bash
POST /orders/async
{"productId": "IPHONE-15", "quantity": 1}
```

### Cách hoạt động

```
User gọi POST /orders/async
         ↓
order-service publish event vào RabbitMQ (~5ms)
         ↓
order-service trả 202 Accepted NGAY cho user  ← không chờ
         ↓ (ngầm)
RabbitMQ chuyển event vào queue
         ↓
inventory-service consumer nhận → trừ kho
```

### Tại sao 202 chứ không phải 200?

- **200 OK** — yêu cầu đã xử lý **xong hoàn toàn**
- **202 Accepted** — đã **nhận**, đang xử lý — kết quả chưa có ngay

Trả 202 là honest: bạn nói với client *"đã nhận đơn, đang xử lý — nhưng kho chưa chắc trừ xong ngay lúc này"*.

### Đảm bảo không mất message (Ack/Nack)

```
inventory-service nhận message
    ↓ xử lý
    ├─ Thành công → channel.ack(msg)
    │   RabbitMQ xóa message khỏi queue
    │
    └─ Thất bại → channel.nack(msg, false, false)
        Message vào DLX (nếu có cấu hình)

Nếu inventory-service crash giữa chừng (chưa ack):
    → RabbitMQ giữ message, redeliver sau khi service restart
    → Không mất message
```

### Kiến trúc RabbitMQ trong project

```
order-service
    │ publish, routing_key="" (không cần vì fanout)
    ▼
Exchange: "order_events" (type: fanout)
    │
    ▼
Queue: "inventory_order_created_queue" (durable: true)
    │ consume
    ▼
inventory-service → xử lý trừ kho

Luồng SAGA rollback (khi payment fail):
payment-service → Exchange "saga_events" → Queue "inventory_saga_rollback_queue"
               → inventory-service → hoàn lại kho
```

### Trade-off: Eventual Consistency

```
Sync:   User gọi → check kho → có hàng → trả 200  (biết chắc kết quả ngay)
Async:  User gọi → publish → trả 202              (chưa biết kết quả thật)
```

100 user gọi async đồng thời, tất cả đều thấy "còn hàng" — nhưng thực ra chỉ còn 10 cái. 90 đơn sẽ bị cancel sau qua email. **Amazon, Shopee đều làm vậy.** Đây là trade-off có chủ đích: đánh đổi tính nhất quán tức thì để lấy tốc độ và khả năng chịu lỗi.

### Hạn chế

- Exchange hiện là `durable: false` — **mất khi RabbitMQ restart**. Production: phải `durable: true`.
- Chưa có DLX — message lỗi bị mất.
- Chưa có idempotency — consumer xử lý 2 lần cùng message → kho trừ 2 lần.

---

## 9. Giai đoạn 4 — Distributed Tracing & Logging

### Mục tiêu học

Hiểu tại sao logging trong microservice khác monolith, và cách theo dõi một request qua nhiều service.

### Vấn đề

Trong monolith, lỗi → tìm log trong **một file**. Trong microservice, một request đi qua 3 service — log ở **3 nơi khác nhau**. Làm sao biết 3 dòng log này thuộc về cùng một request?

### Giải pháp: Trace ID

Mỗi request được gán một **UUID duy nhất** ngay khi vào hệ thống, truyền qua tất cả service:

```javascript
// order-service — sinh traceId
const traceId = randomUUID(); // "a7f3b2c1-..."

// Truyền qua HTTP Header
axios.get('/inventory/IPHONE-15', { headers: { 'x-trace-id': traceId } });

// Truyền qua RabbitMQ message header
rabbitChannel.publish('order_events', '', payload, {
  headers: { 'x-trace-id': traceId }
});

// Mọi dòng log đều có traceId
logger.info({ trace_id: traceId, message: 'Nhận đặt hàng IPHONE-15' });
```

**Kết quả:**

```
[order-service]     trace_id=a7f3b2c1  "Nhận đặt hàng IPHONE-15"
[order-service]     trace_id=a7f3b2c1  "Đã publish RabbitMQ"
[inventory-service] trace_id=a7f3b2c1  "Check kho IPHONE-15 — còn 5 cái"
[inventory-service] trace_id=a7f3b2c1  "Trừ kho IPHONE-15"
```

Query Grafana để thấy toàn bộ luồng:
```
{app=~"order-service|inventory-service"} |= "a7f3b2c1"
```

### Stack logging

```
Node.js → Winston (JSON format) → winston-loki transport → Loki → Grafana
```

**Tại sao JSON format?**

```
Text log:  "2026-06-10 14:32:11 INFO [a7f3b2c1] Nhận đặt hàng IPHONE-15"
            → khó parse, khó filter theo field cụ thể

JSON log:  {"timestamp":"...","level":"info","trace_id":"a7f3b2c1","message":"..."}
            → filter theo bất kỳ field nào, dễ aggregate
```

---

## 10. Giai đoạn 5 — Kafka Stream

### Mục tiêu học

Hiểu sự khác biệt giữa message queue (RabbitMQ) và event streaming (Kafka).

### Route

```bash
POST /orders/stream
{"productId": "IPHONE-15", "quantity": 1}
```

### Kafka dùng để làm gì

**KHÔNG phải để xử lý đơn hàng** — đó là việc của RabbitMQ.

Kafka ghi lại **hành vi user** cho analytics — nhiều service khác nhau có thể đọc cùng data này độc lập, có thể replay lại lịch sử, không mất data khi một consumer down.

### Tại sao Kafka cho analytics, không phải RabbitMQ?

| Yêu cầu | RabbitMQ | Kafka |
|---|---|---|
| 5 team đọc cùng data | ❌ Message chỉ đến 1 consumer | ✅ Nhiều consumer group độc lập |
| Replay 7 ngày để re-train ML | ❌ Message đã xóa sau khi đọc | ✅ Data giữ theo retention |
| 10 triệu event/ngày | Có thể | ✅ Thiết kế cho throughput cao |

**Nguyên tắc chọn:**
- **RabbitMQ** → gửi **command** ("hãy trừ kho") — xử lý chính xác một lần
- **Kafka** → ghi nhận **event** ("đã có người mua") — nhiều hệ thống phản ứng

---

## 11. Thuật ngữ

| Thuật ngữ | Định nghĩa |
|---|---|
| **Microservice** | Kiến trúc chia ứng dụng thành các service nhỏ, độc lập, deploy riêng |
| **Monolith** | Toàn bộ ứng dụng trong một codebase, deploy cùng nhau |
| **HTTP Sync** | Giao tiếp đồng bộ — gọi và chờ kết quả |
| **Async** | Giao tiếp bất đồng bộ — gửi đi không chờ, xử lý ngầm |
| **Message Broker** | Trung gian nhận/gửi message (RabbitMQ) |
| **Event Streaming** | Luồng sự kiện, nhiều consumer đọc độc lập (Kafka) |
| **Producer / Consumer** | Bên gửi / bên nhận message |
| **Exchange** | Bộ định tuyến RabbitMQ, nhận message và chuyển vào queue |
| **Queue** | Hàng đợi lưu message chờ consumer xử lý |
| **Topic / Partition / Offset** | Kênh / phân mảnh / vị trí message trong Kafka |
| **Ack / Nack** | Xác nhận xử lý xong / từ chối message |
| **DLX** | Dead Letter Exchange — nơi nhận message bị từ chối/hết hạn |
| **Timeout** | Giới hạn thời gian chờ, hủy request nếu quá lâu |
| **Retry** | Thử lại khi gặp lỗi tạm thời |
| **Exponential Backoff** | Thời gian chờ giữa các lần retry tăng dần |
| **Circuit Breaker** | Ngắt kết nối đến service lỗi để tránh cascade failure |
| **CLOSED / OPEN / HALF_OPEN** | 3 trạng thái của circuit breaker |
| **Cascade Failure** | Lỗi lan từ service này sang service khác |
| **Idempotency** | Xử lý cùng một message nhiều lần cho kết quả giống nhau |
| **Trace ID** | UUID gắn vào request, truyền xuyên suốt các service để correlate log |
| **Distributed Tracing** | Theo dõi luồng xử lý của một request qua nhiều service |
| **Centralized Logging** | Gom log từ nhiều service vào một nơi để query |
| **Health Check** | Endpoint `/health` để biết service còn sống |
| **Eventual Consistency** | Tính nhất quán đạt được sau một khoảng thời gian, không nhất thiết tức thì |
| **SAGA Pattern** | Chuỗi transaction phân tán, mỗi bước có compensating transaction để rollback |
| **202 Accepted** | HTTP status — đã nhận, đang xử lý, chưa có kết quả |
| **503 Service Unavailable** | Service tạm thời không khả dụng |
| **504 Gateway Timeout** | Service không phản hồi trong thời gian chờ |
| **Fanout Exchange** | Kiểu exchange gửi message đến TẤT CẢ queue đang bind |
| **Durable** | Queue/Exchange tồn tại sau khi RabbitMQ restart |
| **Consumer Group** | Nhóm consumer Kafka cùng groupId |
| **Retention** | Thời gian Kafka giữ message trước khi xóa |

---

## 12. Dashboard & Monitoring

### RabbitMQ Management UI — http://localhost:15672 (guest/guest)

- **Tab Exchanges** — thấy `order_events` và `saga_events`
- **Tab Queues** — xem message rate, queue depth
- **Tab Connections** — thấy các service đang connect

```bash
# Gửi 10 request để thấy message rate tăng
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:3001/orders/async \
    -H "Content-Type: application/json" \
    -d '{"productId":"IPHONE-15","quantity":1}' &
done
```

### Kafka UI — http://localhost:8080

- **Topics** — `user-behavior-logs`, xem message count
- **Consumer Groups** — consumer lag
- **Messages** — nội dung message trong topic

### Grafana + Loki — http://localhost:3000 (admin/admin)

```bash
# Test distributed tracing
RESPONSE=$(curl -s -X POST http://localhost:3001/orders/async \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}')

TRACE_ID=$(echo $RESPONSE | python3 -c "import sys,json; print(json.load(sys.stdin)['trace_id'])")
echo "Query Grafana với: $TRACE_ID"
# LogQL: {app=~"order-service|inventory-service"} |= "$TRACE_ID"
```

---

## 13. Chạy dự án

### Yêu cầu

- Node.js >= 18
- Docker Desktop

### Khởi động

```bash
# Bước 1: Hạ tầng
docker compose up -d

# Bước 2: Inventory Service (chạy trước)
cd inventory-service && npm install && node server.js

# Bước 3: Order Service (terminal khác)
cd order-service && npm install && node server.js
```

### Verify

```bash
curl http://localhost:3001/health
curl http://localhost:3002/health
```

### Sản phẩm trong kho giả

| productId | Stock |
|---|---|
| `IPHONE-15` | 10 |
| `MACBOOK-M3` | 5 |
| `LAPTOP-MODULAR-TEST` | 100 |

### Test triggers

| productId | Hành vi | Mục đích test |
|---|---|---|
| `TRIGGER-SLOW` | Trả response sau 5 giây | Timeout (kỳ vọng 504) |
| `TRIGGER-ERROR` | Luôn trả 500 | Retry + Circuit Breaker |
