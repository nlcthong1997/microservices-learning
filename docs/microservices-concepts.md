# Microservices — Các khái niệm cốt lõi

> Tài liệu này đi sâu vào từng khái niệm quan trọng trong kiến trúc Microservice:  
> định nghĩa, lý do tồn tại, ví dụ thực tế, trade-off, và cách các công ty lớn áp dụng.

---

## Mục lục

1. [Event-Driven Architecture (EDA)](#1-event-driven-architecture-eda)
2. [gRPC — Remote Procedure Call hiện đại](#2-grpc--remote-procedure-call-hiện-đại)
3. [Security — Bảo mật Microservice](#3-security--bảo-mật-microservice)
4. [Logging & Distributed Tracing](#4-logging--distributed-tracing)
5. [SAGA Pattern — Giao dịch phân tán](#5-saga-pattern--giao-dịch-phân-tán)
6. [ACID, BASE & CAP Theorem](#6-acid-base--cap-theorem)
7. [Race Condition & Concurrency Control](#7-race-condition--concurrency-control)
8. [Pub/Sub Pattern](#8-pubsub-pattern)
9. [Connection Pool](#9-connection-pool)
10. [Push MQ vs Pull MQ](#10-push-mq-vs-pull-mq)
11. [Circuit Breaker](#11-circuit-breaker)
12. [API Gateway & BFF](#12-api-gateway--bff)
13. [Idempotency](#13-idempotency)
14. [Outbox Pattern](#14-outbox-pattern)
15. [Health Check — Liveness vs Readiness](#15-health-check--liveness-vs-readiness)
16. [Backpressure & Rate Limiting](#16-backpressure--rate-limiting)
17. [Bulkhead Pattern](#17-bulkhead-pattern)
18. [Service Discovery & Load Balancing](#18-service-discovery--load-balancing)

---

## 1. Event-Driven Architecture (EDA)

### Vấn đề EDA giải quyết

Trong monolith, các module gọi nhau trực tiếp:

```
OrderModule → tightly coupled → InventoryModule
           → tightly coupled → PaymentModule
           → tightly coupled → NotificationModule
```

Khi thêm một module mới (ví dụ LoyaltyModule), bạn phải vào sửa `OrderModule`.  
Khi `NotificationModule` chết, `OrderModule` cũng bị ảnh hưởng.  
Khi `PaymentModule` chậm, `OrderModule` phải chờ.

**EDA giải quyết bằng cách đảo ngược chiều phụ thuộc:**

```
OrderService ──publish──► "order.created" event
                              │
        ┌─────────────────────┼──────────────────────────┐
        ▼                     ▼                          ▼
InventoryService        PaymentService           NotificationService
(tự subscribe)          (tự subscribe)           (tự subscribe)
```

`OrderService` chỉ quan tâm đến việc "thông báo điều đã xảy ra".  
Ai muốn phản ứng thì tự subscribe — `OrderService` không biết và không cần biết.

---

### 3 mức độ của EDA

| Mức | Tên | Mô tả | Ví dụ |
|-----|-----|-------|-------|
| 1 | **Event Notification** | Thông báo rằng có chuyện xảy ra, không kèm data | `order.shipped { orderId }` → consumer tự gọi API lấy chi tiết |
| 2 | **Event-Carried State Transfer** | Event chứa đủ data để consumer không cần gọi lại | `order.shipped { orderId, items, address, ... }` |
| 3 | **Event Sourcing** | Mọi thay đổi state đều là event, state = replay toàn bộ event | Tài khoản ngân hàng: replay giao dịch để tính số dư |

**Level 2 (ECST) là phổ biến nhất** trong thực tế vì cân bằng giữa loose coupling và performance.

---

### Event vs Command — Sự khác biệt cốt lõi

```
Command (RabbitMQ thường dùng):         Event (Kafka thường dùng):

"ReserveStock(orderId=123)"             "order.created { orderId=123 }"
  ↳ Chỉ thị làm gì                       ↳ Sự thật đã xảy ra
  ↳ Biết rõ ai sẽ thực hiện              ↳ Không quan tâm ai xử lý
  ↳ Expect kết quả                        ↳ Fire and forget
  ↳ 1 receiver                           ↳ N receivers
```

**Nguyên tắc:** Kafka publish FACTS, không phải INSTRUCTIONS.

---

### Khi nào KHÔNG dùng EDA

- Cần response đồng bộ ngay lập tức (ví dụ: check tồn kho trước khi hiển thị nút "Mua ngay")
- Flow nghiệp vụ đơn giản, 1-2 service
- Team nhỏ chưa có kinh nghiệm xử lý eventual consistency
- Cần strong consistency (financial settlement, medical records với real-time requirement)

---

## 2. gRPC — Remote Procedure Call hiện đại

### gRPC là gì?

gRPC (Google Remote Procedure Call) là framework cho phép gọi hàm từ service này sang service khác như gọi hàm local — nhưng thực tế là một network call.

```
// Bạn VIẾT thế này trong inventory-service:
const result = await getProductInfo(productId);

// gRPC làm việc này PHÍA SAU:
1. Serialize productId → binary (Protocol Buffers)
2. Gửi HTTP/2 request đến inventory-service:50051
3. inventory-service deserialize binary → productId
4. Xử lý, serialize result → binary
5. Trả về HTTP/2 response
6. order-service deserialize binary → result object
```

---

### Tại sao dùng gRPC thay REST?

| Tiêu chí | REST + JSON | gRPC + Protobuf |
|----------|-------------|-----------------|
| **Serialization** | JSON (text, parse chậm) | Binary (compact, parse nhanh ~5-10x) |
| **Protocol** | HTTP/1.1 (1 request/connection) | HTTP/2 (multiplexing, nhiều request/connection) |
| **Schema** | Không bắt buộc (OpenAPI tuỳ chọn) | Bắt buộc (.proto file) |
| **Type safety** | ❌ Runtime error | ✅ Compile-time check |
| **Streaming** | ❌ (cần WebSocket riêng) | ✅ Built-in 4 loại |
| **Browser support** | ✅ Native | ⚠️ Cần grpc-web proxy |
| **Human readable** | ✅ Dễ debug với curl | ❌ Binary, cần tooling |

**Kết luận:** gRPC tốt cho **internal service-to-service** communication (performance critical, type-safe). REST tốt cho **external API** (browser, mobile, third-party).

---

### Protocol Buffers — định nghĩa contract

```protobuf
// inventory.proto — đây là "hợp đồng" giữa các service

syntax = "proto3";

service InventoryService {
  rpc GetProduct (ProductRequest) returns (ProductResponse);
  rpc WatchStock (ProductRequest) returns (stream StockUpdate); // Server streaming
}

message ProductRequest {
  string product_id = 1;
}

message ProductResponse {
  string product_id = 1;
  string name       = 2;
  int32  stock      = 3;
  bool   available  = 4;
}
```

`.proto` file được **compile** thành code cho mọi ngôn ngữ (Go, Python, Java, Node.js...).  
Nếu bạn đổi field type mà không backward-compatible → **compile error ngay** thay vì runtime crash.

---

### 4 loại streaming trong gRPC

```
1. Unary (như REST):
   Client ──request──► Server ──response──► Client

2. Server Streaming:
   Client ──request──► Server ──stream of responses──► Client
   Dùng cho: real-time price feed, live dashboard

3. Client Streaming:
   Client ──stream of requests──► Server ──response──► Client
   Dùng cho: upload file lớn theo chunks, batch insert

4. Bidirectional Streaming:
   Client ──stream──►
              ◄──stream── Server
   Dùng cho: chat, real-time collaboration, gaming
```

---

### Ví dụ thực tế trong project này

```javascript
// order-service gọi inventory-service qua gRPC (thay vì HTTP)
// Nhanh hơn ~3x so với REST vì binary serialization + HTTP/2

const grpc = require('@grpc/grpc-js');
const { client } = require('./config/grpcClient');

// Gọi như hàm bình thường
client.GetProduct({ product_id: 'IPHONE-15' }, (err, response) => {
  // response.stock, response.available...
});
```

So sánh latency thực tế:
- REST/JSON: ~5-15ms internal call
- gRPC/Protobuf: ~1-3ms internal call

Với 1000 requests/second, tiết kiệm 4-12ms/request = tiết kiệm 4-12 giây tổng/giây.

---

## 3. Security — Bảo mật Microservice

### Các lớp bảo mật trong Microservice

```
[Internet]
    │
    ▼
[API Gateway]  ← Layer 1: TLS termination, DDoS protection, IP whitelist
    │
    ▼
[Auth Service] ← Layer 2: Authn/Authz, issue JWT
    │
    ▼
[Services]     ← Layer 3: Validate JWT, authorize per endpoint
    │
    ▼
[Database]     ← Layer 4: Encrypted at rest, connection với credentials
```

---

### JWT — Cơ chế hoạt động

```
JWT = Header.Payload.Signature

Header:  { "alg": "HS256", "typ": "JWT" }
Payload: { "userId": "u123", "role": "customer", "exp": 1718000000 }
Signature: HMACSHA256(base64(header) + "." + base64(payload), SECRET_KEY)
```

**Quan trọng:** Payload **KHÔNG được mã hóa** — chỉ được ký (signed).  
Bất kỳ ai cũng đọc được payload bằng base64 decode.  
Nhưng **không thể giả mạo** vì thiếu SECRET_KEY để tạo signature hợp lệ.

```
❌ Đừng để thông tin nhạy cảm trong payload JWT:
   { "userId": "u123", "creditCard": "4111..." }  ← SAI!

✅ Chỉ để identifiers + permissions:
   { "userId": "u123", "role": "customer", "exp": 1718000000 }
```

---

### Access Token vs Refresh Token

```
Access Token:
  - Thời hạn ngắn (15 phút - 1 giờ)
  - Dùng để gọi API
  - Stateless — server không cần lưu
  - Nếu bị đánh cắp → mất tối đa 15 phút

Refresh Token:
  - Thời hạn dài (7-30 ngày)
  - Dùng để lấy Access Token mới
  - Stateful — server lưu trong DB (có thể revoke)
  - Chỉ gửi đến auth endpoint, không gửi mọi request
  - Nếu bị đánh cắp → revoke ngay trong DB
```

**Tại sao cần cả hai?**  
Nếu chỉ dùng access token dài hạn → bị đánh cắp không revoke được.  
Nếu chỉ dùng access token ngắn hạn → user phải login lại mỗi 15 phút.

---

### Service-to-Service Authentication

```
Option 1 — API Key (đơn giản, kém bảo mật):
  inventory-service gọi payment-service với header X-API-Key: abc123
  Nếu lộ key → attacker giả mạo inventory-service

Option 2 — Mutual TLS (mTLS):
  Cả 2 service đều có certificate
  Client cert xác thực "tôi là inventory-service thật"
  Server cert xác thực "tôi là payment-service thật"
  → Dùng trong Kubernetes với service mesh (Istio, Linkerd)

Option 3 — Short-lived Service Token (phổ biến nhất):
  inventory-service xin token từ Auth Service (JWT với audience: "payment-service")
  Gọi payment-service với token đó
  Token hết hạn sau 5 phút → tự động renew
```

---

### OWASP Top 10 cho Microservice

| Lỗ hổng | Mô tả | Phòng chống |
|---------|-------|-------------|
| **Broken Auth** | JWT không verify, secret key yếu | Verify signature, dùng asymmetric keys |
| **Injection** | SQL/NoSQL/Command injection qua input | Parameterized queries, input validation |
| **Sensitive Data Exposure** | Log password, credit card | Mask sensitive fields trong log |
| **Broken Access Control** | User A xem data User B | Validate ownership mỗi request |
| **Security Misconfiguration** | Debug mode bật production, default passwords | Infrastructure as Code, secret management |
| **SSRF** | Service gọi URL do user cung cấp | Whitelist allowed URLs/IPs |
| **Rate Limiting absent** | Brute force login, resource exhaustion | Rate limit tại API Gateway + per service |

---

### Secrets Management — KHÔNG hardcode credentials

```javascript
// ❌ SAI — lộ trong git history
const DB_PASSWORD = 'super_secret_123';

// ✅ ĐÚNG — đọc từ environment variable
const DB_PASSWORD = process.env.DB_PASSWORD;

// ✅ TỐT NHẤT — đọc từ Vault/AWS Secrets Manager tại runtime
const { DB_PASSWORD } = await vault.read('secret/database');
```

Trong Kubernetes: dùng **Kubernetes Secrets** (encrypted at rest với KMS).  
Trong Docker Compose: dùng **Docker Secrets** hoặc `.env` file (gitignore).

---

## 4. Logging & Distributed Tracing

### Tại sao Distributed Tracing quan trọng?

```
User báo: "Đơn hàng #12345 bị lỗi lúc 14:30"

Trong microservice, 1 request đi qua nhiều service:

API Gateway → order-service → inventory-service → payment-service → notification-service
   10ms           50ms              30ms               200ms              20ms
                                                        ↑
                                                     LỖI Ở ĐÂY

Không có Trace ID → bạn phải grep log từng service, khớp thủ công theo timestamp.
Có Trace ID     → grep "trace_id=req-abc123" → thấy toàn bộ hành trình trong 1 query.
```

---

### Structured Logging — Log phải là data, không phải text

```javascript
// ❌ SAI — không thể query/filter/alert
console.log('User 123 bought product IPHONE-15 quantity 2 at 2026-06-12 14:30:00');

// ✅ ĐÚNG — structured JSON, Loki/Elasticsearch có thể filter
logger.info({
  trace_id: 'req-abc123',
  service: 'order-service',
  event: 'order.created',
  userId: 'u123',
  productId: 'IPHONE-15',
  quantity: 2,
  durationMs: 45
});
```

Với structured log, bạn có thể:
- Grafana: vẽ biểu đồ `avg(durationMs) by productId`
- Alert: khi `level=error` tăng đột biến
- Debug: `{trace_id="req-abc123"}` → thấy toàn bộ luồng

---

### Log Levels — dùng đúng level

| Level | Khi nào dùng | Ví dụ |
|-------|-------------|-------|
| `error` | Lỗi cần alert ngay, ảnh hưởng user | Payment failed, DB connection lost |
| `warn` | Bất thường nhưng chưa ảnh hưởng | Retry #2, circuit breaker opening |
| `info` | Milestones quan trọng trong flow | Order created, payment completed |
| `debug` | Chi tiết cho debugging (tắt trên production) | SQL query, HTTP request body |
| `trace` | Cực chi tiết (chỉ dùng khi troubleshoot) | Function entry/exit, variable values |

```javascript
// ❌ SAI — log spam làm khó tìm lỗi thật
logger.info('Entering function');
logger.info('Variable x = ' + x);

// ✅ ĐÚNG — log có ý nghĩa, có context
logger.info({ trace_id, event: 'order.created', productId, quantity });
logger.error({ trace_id, event: 'payment.failed', reason: error.message });
```

---

### Correlation ID / Trace ID — xuyên suốt các service

```javascript
// API Gateway tạo trace_id cho mỗi request từ ngoài vào
const traceId = req.headers['x-trace-id'] || randomUUID();

// Truyền traceId theo mọi downstream call:
// - HTTP header: X-Trace-ID: abc123
// - Message property: { headers: { 'x-trace-id': 'abc123' } }
// - gRPC metadata: metadata.add('x-trace-id', 'abc123')

// Mỗi service log với traceId này → có thể reconstruct toàn bộ hành trình
```

**Distributed Tracing tools** (Jaeger, Zipkin, OpenTelemetry):
- Tự động inject/propagate trace context
- Vẽ flame graph: thấy ngay bottleneck ở service nào, function nào
- Measure latency từng hop

---

### Logging Stack trong project này

```
Service (winston JSON)
    │
    ▼
Loki (:3100)   ← Thu thập và index logs theo labels
    │
    ▼
Grafana (:3000) ← Query, visualize, alert

Query ví dụ:
  {job="order-service"} | json | level="error"
  {job="inventory-service"} | json | trace_id="abc123"
```

---

## 5. SAGA Pattern — Giao dịch phân tán

### Vấn đề: Distributed Transaction

Trong monolith, `@Transactional` đảm bảo all-or-nothing:

```sql
BEGIN TRANSACTION;
  UPDATE inventory SET stock = stock - 1;
  INSERT INTO orders (product_id, status) VALUES ('P1', 'CONFIRMED');
  UPDATE accounts SET balance = balance - 500000;
COMMIT; -- hoặc ROLLBACK nếu bất kỳ bước nào lỗi
```

Trong microservice, 3 bước trên nằm ở 3 database khác nhau.  
Không có `COMMIT` nào bao phủ được cả 3.  
**2PC (Two-Phase Commit)** là giải pháp truyền thống nhưng:**blocking**, **không scale**, **coordinator là SPOF**.

**SAGA** là giải pháp thực tế: chia transaction lớn thành chuỗi local transactions nhỏ, mỗi bước có **compensating transaction** để rollback.

---

### Choreography SAGA — "tự phối hợp qua event"

```
Không có service nào "chỉ huy" — mỗi service tự biết việc của mình:

order-service ──"order.created"──► Kafka
                                      │
                              inventory-service
                              (subscribe "order.created")
                              → reserve stock
                              → publish "inventory.reserved"
                                      │
                              payment-service
                              (subscribe "inventory.reserved")
                              → charge card
                              → publish "payment.completed"
                                      │
                              order-service
                              (subscribe "payment.completed")
                              → confirm order ✅

--- NẾU PAYMENT THẤT BẠI ---

payment-service ──"payment.failed"──► Kafka
                                          │
                                  inventory-service
                                  (subscribe "payment.failed")
                                  → release stock (compensating transaction)
                                          │
                                  order-service
                                  (subscribe "payment.failed")
                                  → cancel order ❌
```

**Ưu điểm:** Loose coupling, dễ thêm service mới, không có SPOF.  
**Nhược điểm:** Khó debug (flow phân tán), khó handle circular events, khó biết state tổng thể.

---

### Orchestration SAGA — "có nhạc trưởng điều phối"

```
Saga Orchestrator (thường là 1 service riêng hoặc state machine):

┌─────────────────────────────────────┐
│           Order Saga                │
│  State: PENDING                     │
│                                     │
│  Step 1: reserve inventory ──────────┼──► inventory-service
│          ↓ success                  │◄── "OK"
│  Step 2: charge payment  ───────────┼──► payment-service
│          ↓ failure                  │◄── "FAILED"
│  Step 3 (compensate): release stock ┼──► inventory-service
│          State: CANCELLED           │
└─────────────────────────────────────┘
```

**Ưu điểm:** Dễ biết state hiện tại, dễ debug, xử lý lỗi phức tạp rõ ràng hơn.  
**Nhược điểm:** Orchestrator trở thành "biết hết" → coupling tăng; là SPOF nếu không HA.

---

### Choreography vs Orchestration — khi nào dùng gì?

| Tiêu chí | Choreography | Orchestration |
|----------|-------------|---------------|
| **Coupling** | Thấp — services độc lập | Cao hơn — orchestrator biết tất cả |
| **Độ phức tạp flow** | Đơn giản (3-5 bước) | Phức tạp (nhiều nhánh, retry, timeout) |
| **Debug** | Khó — event scatter | Dễ — xem state trong orchestrator |
| **SPOF** | Không | Orchestrator (cần HA) |
| **Ví dụ công ty** | Netflix, Airbnb | Uber (Cadence/Temporal) |

---

### Compensating Transaction — "đây không phải rollback DB"

```
ROLLBACK DB: Hoàn tác như chưa từng xảy ra (atomic, instant)

Compensating Transaction: Hành động nghiệp vụ ngược lại (xảy ra SAU, có thể thất bại)

Ví dụ:
  Bước gốc: trừ 100,000 VND từ tài khoản
  Bù đắp:   + cộng lại 100,000 VND (là một giao dịch MỚI, không phải undo)

Ý nghĩa: Hệ thống đi qua trạng thái trung gian không nhất quán (inconsistent)
         rồi TỰ HỒI PHỤC về consistent. Đây là "eventual consistency".
```

---

### Idempotency trong SAGA — vì event có thể đến 2 lần

Kafka/RabbitMQ đảm bảo **at-least-once delivery** — event CÓ THỂ đến 2 lần.  
Compensating transaction phải **idempotent** — xử lý 2 lần không gây hại.

```javascript
// ❌ SAI — xử lý 2 lần → trừ kho 2 lần
channel.consume(queue, (msg) => {
  inventory.stock -= msg.quantity; // Nguy hiểm nếu msg đến 2 lần!
  channel.ack(msg);
});

// ✅ ĐÚNG — check idempotency key trước
channel.consume(queue, async (msg) => {
  const { orderId, quantity } = JSON.parse(msg.content);
  const alreadyProcessed = await redis.get(`processed:${orderId}`);
  if (alreadyProcessed) { channel.ack(msg); return; }

  inventory.stock -= quantity;
  await redis.setex(`processed:${orderId}`, 86400, '1'); // TTL 24h
  channel.ack(msg);
});
```

---

## 6. ACID, BASE & CAP Theorem

### ACID — Tính chất của Database truyền thống

| Tính chất | Viết tắt | Nghĩa | Ví dụ |
|-----------|----------|-------|-------|
| **Atomicity** | A | All or nothing | Chuyển tiền: trừ A và cộng B hoặc không làm gì cả |
| **Consistency** | C | DB luôn ở trạng thái hợp lệ | Balance không âm, foreign key không bị broken |
| **Isolation** | I | Transactions không thấy nhau khi đang chạy | 2 người mua vé cùng lúc → chỉ 1 người thành công |
| **Durability** | D | Data đã commit sẽ không mất dù crash | Ghi xuống disk, không chỉ RAM |

PostgreSQL, MySQL, Oracle: **ACID compliant**.  
Giá phải trả: **chậm hơn** vì cần lock, write-ahead log, fsync.

---

### BASE — Trade-off của distributed systems

Khi scale ra nhiều node, ACID quá tốn kém. BASE là trade-off thực tế:

| Tính chất | Nghĩa |
|-----------|-------|
| **Basically Available** | Hệ thống vẫn hoạt động dù có node lỗi (availability > consistency) |
| **Soft state** | State có thể thay đổi theo thời gian dù không có input mới (do replication) |
| **Eventually consistent** | Sau một khoảng thời gian, tất cả nodes sẽ đồng thuận về giá trị |

**Ví dụ:** Facebook Like.  
Bạn like một post ở California. Server ở Việt Nam thấy "1000 likes".  
1 giây sau mới thấy "1001 likes". Đây là **eventual consistency**.  
Không ai chết vì thấy số like lệch 1 giây. **Đây là acceptable trade-off.**

**Ngược lại:** Balance tài khoản ngân hàng → không thể eventual.  
Nếu bạn có 500k, 2 ATM đọc cùng lúc → cả 2 cho rút 500k → mất tiền ngân hàng.

---

### CAP Theorem — Giới hạn của Distributed Systems

Định lý CAP (Brewer, 2000): Một distributed system **không thể đồng thời đảm bảo cả 3**:

```
         Consistency
              C
             /|\
            / | \
           /  |  \
          /   |   \
         / CA | CP  \
        /_____|______\
       A               P
  Availability     Partition
                  Tolerance

CA: Single node DB (PostgreSQL standalone) — không partition tolerant
CP: HBase, Zookeeper — sacrifice availability khi có partition
AP: Cassandra, DynamoDB — sacrifice consistency khi có partition
```

**Trong thực tế,** network partition LUÔN XẢY RA trong distributed systems.  
Vì vậy bạn **phải chọn C hoặc A khi có partition.**

**Ví dụ:**
- Đặt vé máy bay (strong consistency): dùng CP — thà hệ thống unavailable còn hơn oversell
- Shopping cart (eventual consistency): dùng AP — thà cart lệch vài giây còn hơn không mua được

---

### Isolation Levels — Hiểu để tránh Race Condition

```
READ UNCOMMITTED  → đọc data chưa commit (dirty read) — nguy hiểm nhất
READ COMMITTED    → chỉ đọc data đã commit (PostgreSQL mặc định)
REPEATABLE READ   → data trong 1 transaction không đổi dù có commit khác
SERIALIZABLE      → như chạy tuần tự, chậm nhất nhưng an toàn nhất
```

---

## 7. Race Condition & Concurrency Control

### Race Condition là gì?

```
Kịch bản: Chỉ còn 1 sản phẩm. 2 user mua cùng lúc.

User A: READ  stock = 1  → kiểm tra: 1 >= 1 → OK
User B: READ  stock = 1  → kiểm tra: 1 >= 1 → OK
User A: WRITE stock = 0  (trừ 1)
User B: WRITE stock = 0  (trừ 1)

Kết quả: Cả 2 đặt hàng thành công, nhưng stock = 0 thay vì -1
         (Tệ hơn: nếu không có constraint, stock = -1)
```

Đây là **Read-Modify-Write** race condition — cực phổ biến trong e-commerce.

---

### Giải pháp 1 — Optimistic Locking (Không lock, phát hiện sau)

```sql
-- Thêm cột version vào bảng
ALTER TABLE products ADD COLUMN version INTEGER DEFAULT 0;

-- Khi update, kèm version vào WHERE
UPDATE products
SET stock = stock - 1, version = version + 1
WHERE product_id = 'P1'
  AND version = 5          -- ← chỉ update nếu version vẫn là 5
  AND stock >= 1;

-- Nếu affected rows = 0 → có người khác đã update trước → retry
```

**Khi nào dùng:** Read nhiều, conflict hiếm (< 10% requests có conflict).  
**Ví dụ:** Cập nhật profile, edit document, vote.

---

### Giải pháp 2 — Pessimistic Locking (Lock trước, xử lý sau)

```sql
-- SELECT FOR UPDATE: lock row, không ai đọc/ghi được cho đến khi COMMIT
BEGIN;
SELECT stock FROM products WHERE product_id = 'P1' FOR UPDATE;
-- Lúc này, User B sẽ bị block tại đây, chờ User A COMMIT

UPDATE products SET stock = stock - 1 WHERE product_id = 'P1';
COMMIT;
-- User B mới được tiếp tục
```

**Khi nào dùng:** Conflict thường xuyên, không thể retry.  
**Nhược điểm:** Throughput thấp hơn, risk deadlock.  
**Ví dụ:** Đặt vé (seat booking), flash sale, deduct wallet balance.

---

### Giải pháp 3 — Distributed Lock với Redis

Khi state nằm ở nhiều service (không dùng chung DB):

```javascript
// Redis SETNX (Set if Not eXist) — distributed mutex
const lockKey = `lock:product:${productId}`;
const lockValue = randomUUID();  // unique per caller
const lockTTL = 5000;            // 5 seconds TTL (auto-expire nếu crash)

// Attempt to acquire lock
const acquired = await redis.set(lockKey, lockValue, 'PX', lockTTL, 'NX');
if (!acquired) {
  throw new Error('Cannot acquire lock — another process is updating this product');
}

try {
  // Critical section — chỉ 1 process chạy đến đây
  const product = await db.getProduct(productId);
  if (product.stock < quantity) throw new Error('Out of stock');
  await db.updateStock(productId, -quantity);
} finally {
  // Release lock — chỉ người giữ lock mới được release
  // Dùng Lua script để đảm bảo atomic
  await redis.eval(
    `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`,
    1, lockKey, lockValue
  );
}
```

**Chú ý:** Redis distributed lock (Redlock) có edge cases. Với critical data, database-level lock vẫn an toàn hơn.

---

### Giải pháp 4 — Atomic Database Operations (đơn giản nhất)

```sql
-- Thay vì READ rồi WRITE, dùng 1 atomic UPDATE với điều kiện
UPDATE products
SET stock = stock - :quantity
WHERE product_id = :productId
  AND stock >= :quantity;  -- ← atomic check-and-update

-- Nếu affected_rows = 0 → out of stock hoặc race condition
```

**Đây là giải pháp đơn giản và hiệu quả nhất cho hầu hết trường hợp.**  
Tận dụng tính atomicity của database — không cần code phức tạp.

---

## 8. Pub/Sub Pattern

### Pub/Sub là gì?

```
Publisher                    Subscriber 1
    │                           ▲
    │    ┌─────────────────┐    │
    └───►│    Message      │────┤
         │    Broker       │    │ Subscriber 2
         │    (Topic)      │────┤
         └─────────────────┘    │
                                │ Subscriber 3
                                ▲
```

**Publisher** publish message đến một **Topic** mà không biết ai đang subscribe.  
**Subscriber** subscribe topic và nhận mọi message — mỗi subscriber nhận **bản sao riêng**.

---

### Pub/Sub vs Message Queue — Sự khác biệt

| Đặc điểm | Message Queue | Pub/Sub |
|----------|--------------|---------|
| **Người nhận** | 1 consumer nhận 1 message | N subscribers, mỗi người nhận bản sao |
| **Mục đích** | Task distribution, load balancing | Event broadcasting, fan-out |
| **Khi consumer down** | Message chờ trong queue | Message có thể mất (nếu không persist) |
| **Ví dụ** | Email send queue | Live score update cho 1000 users |

```
Queue (RabbitMQ default):           Pub/Sub (Kafka / RabbitMQ fanout):

Producer → [msg1][msg2][msg3]       Producer → topic
           Consumer A nhận msg1                 ↓        ↓        ↓
           Consumer B nhận msg2          Subscriber A  B    C
           Consumer A nhận msg3          (mỗi người nhận đủ tất cả)
```

---

### Fan-out Pattern — 1 event, nhiều hành động

```javascript
// Kafka topic: "order.created"
// Fanout tới nhiều consumer groups — mỗi group xử lý độc lập

// Consumer Group 1: inventory-service-group
// → giảm tồn kho

// Consumer Group 2: notification-service-group
// → gửi email xác nhận

// Consumer Group 3: analytics-service-group
// → ghi vào data warehouse

// Consumer Group 4: fraud-detection-group
// → kiểm tra gian lận
```

**Sức mạnh:** Thêm use case mới → chỉ tạo Consumer Group mới. Không sửa producer.

---

### Message Ordering trong Pub/Sub

```
Kafka: Ordering guaranteed WITHIN a partition
  → Cùng partitionKey → cùng partition → đúng thứ tự

  order.created  (key: order-123) → partition 0
  order.updated  (key: order-123) → partition 0  ← cùng partition, đúng thứ tự
  order.cancelled(key: order-123) → partition 0

  order.created  (key: order-456) → partition 1  ← khác partition, song song
```

**Sai lầm phổ biến:** Publish order.created và order.cancelled với key khác nhau  
→ vào 2 partition khác nhau → consumer có thể nhận cancelled trước created.

---

## 9. Connection Pool

### Tại sao cần Connection Pool?

```
Không có pool — mỗi request tạo connection mới:

Request 1 ──► TCP handshake (10ms) ──► Authenticate (5ms) ──► Query (2ms) ──► Close (5ms)
              Tổng: 22ms, nhưng query thật chỉ 2ms
              CPU + RAM tốn để setup/teardown mỗi lần

Với Pool — connection được tái sử dụng:

Startup: tạo sẵn 10 connections
Request 1 ──► lấy connection từ pool (0.1ms) ──► Query (2ms) ──► trả về pool
              Tổng: 2.1ms
```

**Connection setup tốn kém:** TCP handshake + TLS + DB authentication = 10-50ms.  
Với 1000 requests/second, overhead này là không chấp nhận được.

---

### Pool Sizing — Công thức thực tế

```
Formula từ HikariCP (PostgreSQL best practices):

Pool size = (core_count * 2) + effective_spindle_count

Ví dụ: 4 CPU cores, SSD (effective_spindle = 1)
  Pool size = (4 * 2) + 1 = 9

Ví dụ: 8 CPU cores, HDD RAID-10 (effective_spindle = 4)
  Pool size = (8 * 2) + 4 = 20
```

**Tại sao pool quá lớn lại tệ?**

```
Database có giới hạn max_connections (PostgreSQL mặc định: 100)
100 services × 50 connections/service = 5000 connections → DB từ chối

Nhiều connections idle = RAM tốn = context switching = chậm hơn
"More is not better" với connection pool
```

---

### Connection Pool trong Node.js

```javascript
// pg (PostgreSQL) — pool built-in
const { Pool } = require('pg');
const pool = new Pool({
  host: 'localhost',
  database: 'mydb',
  max: 10,              // tối đa 10 connections
  idleTimeoutMillis: 30000,  // đóng connection idle sau 30s
  connectionTimeoutMillis: 2000, // timeout nếu không có connection available sau 2s
});

// Mongoose (MongoDB)
mongoose.connect(uri, {
  maxPoolSize: 10,
  minPoolSize: 2,
  socketTimeoutMS: 45000,
});
```

---

### Pool Exhaustion — khi pool cạn

```
Tình huống: pool size = 10, đang có 10 requests đang chạy slow query

Request 11 đến → không có connection → chờ connectionTimeoutMillis
→ Timeout error: "Connection pool exhausted"

Nguyên nhân thường gặp:
1. Slow query chưa return connection về pool
2. Leak — code quên gọi client.release() sau khi dùng
3. Pool quá nhỏ so với load

Cách detect: monitor metric "pool.waiting" hoặc "pool.idle"
```

---

## 10. Push MQ vs Pull MQ

### Push Model — Broker chủ động đẩy message đến consumer

```
Broker ──push──► Consumer

Consumer không cần hỏi — message tự đến khi sẵn sàng.
```

**Ví dụ:** RabbitMQ — consumer đăng ký `channel.consume(queue, callback)`.  
Khi có message, RabbitMQ tự gọi callback.

**Ưu điểm:**
- Latency thấp — message đến gần như ngay lập tức
- Consumer code đơn giản — không cần polling loop

**Nhược điểm:**
- Broker không biết consumer đang bận hay rảnh — có thể **overwhelm** consumer
- Cần `prefetch` để giới hạn số message đang xử lý cùng lúc
- `channel.prefetch(10)` → RabbitMQ chỉ push tối đa 10 message chưa ack

---

### Pull Model — Consumer chủ động kéo message từ broker

```
Consumer ──poll──► Broker ──response──► Consumer (batch of messages)
  (lặp lại liên tục)
```

**Ví dụ:** Kafka — consumer liên tục gọi `consumer.run({ eachMessage })` — bên trong là polling.

```javascript
// Kafka consumer — pull model, batch polling
await consumer.run({
  eachBatch: async ({ batch }) => {
    // Lấy cả batch messages cùng lúc — hiệu quả hơn từng message
    for (const message of batch.messages) {
      await processMessage(message);
    }
  }
});
```

**Ưu điểm:**
- Consumer kiểm soát tốc độ — không bao giờ bị overwhelm
- Batch processing — lấy 100 message xử lý 1 lần thay vì 100 lần
- Consumer có thể pause khi cần (backpressure)

**Nhược điểm:**
- Latency cao hơn (polling interval)
- Phức tạp hơn phía consumer

---

### So sánh RabbitMQ vs Kafka qua lens Push/Pull

| | RabbitMQ (Push) | Kafka (Pull) |
|--|-----------------|--------------|
| **Mô hình** | Broker đẩy đến consumer | Consumer kéo từ broker |
| **Backpressure** | `prefetch` (giới hạn in-flight messages) | Consumer tự control polling rate |
| **Throughput** | Tốt cho low-latency, message đơn lẻ | Tốt cho high-throughput, batch |
| **Consumer overload** | Có thể xảy ra nếu không set prefetch | Không bao giờ — consumer tự quyết |
| **Use case phù hợp** | Task queue, RPC, notification | Stream processing, analytics pipeline |

---

## 11. Circuit Breaker

### Vấn đề: Cascade Failure

```
Không có Circuit Breaker:

order-service → inventory-service (đang chết, timeout 30s)
                ↑
   1000 requests đang chờ 30s × 1000 = hàng nghìn threads bị block
   Thread pool exhausted → order-service cũng chết
   API Gateway → order-service (chết) → toàn bộ hệ thống sập
```

**Circuit Breaker ngăn cascade failure** bằng cách nhanh chóng fail khi phát hiện downstream service đang có vấn đề.

---

### 3 Trạng thái của Circuit Breaker

```
                 ┌─────────────────────────────┐
                 │                             │
    failures     ▼        timeout (half-open)  │
    >= threshold                               │
CLOSED ──────────► OPEN ──────────────────► HALF-OPEN
  ▲                                              │
  │                                              │ probe request
  └──────────────────────────────────────────────┘
            success → CLOSED
            failure → OPEN
```

| Trạng thái | Mô tả | Hành động |
|------------|-------|-----------|
| **CLOSED** | Bình thường, request đi qua | Đếm lỗi |
| **OPEN** | Đang có lỗi, fail fast | Trả lỗi ngay, không gọi downstream |
| **HALF-OPEN** | Thử phục hồi | Cho 1 request đi qua, xem kết quả |

---

### Cài đặt trong project này

```javascript
// config/circuitBreaker.js
class CircuitBreaker {
  constructor({ failureThreshold = 3, timeout = 10000 }) {
    this.state = 'CLOSED';
    this.failures = 0;
    this.failureThreshold = failureThreshold;
    this.lastFailureTime = null;
    this.timeout = timeout; // thời gian giữ OPEN trước khi thử HALF-OPEN
  }

  async execute(fn, traceId) {
    if (this.state === 'OPEN') {
      // Kiểm tra đã đến lúc thử lại chưa
      if (Date.now() - this.lastFailureTime >= this.timeout) {
        this.state = 'HALF-OPEN';
      } else {
        throw Object.assign(new Error('Circuit OPEN'), { circuitOpen: true });
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      throw err;
    }
  }
}
```

**Uber** dùng Circuit Breaker (Hystrix/Resilience4j) cho mọi inter-service call.  
Khi surge pricing service down, ứng dụng vẫn hoạt động với giá mặc định thay vì crash.

---

## 12. API Gateway & BFF

### API Gateway — cổng vào duy nhất

```
Mobile App  ──►┐
Web App     ──►│   [API Gateway]  ── Auth ── Rate Limit ── SSL
3rd Party   ──►┘        │
                         ├──► order-service
                         ├──► inventory-service
                         ├──► payment-service
                         └──► user-service
```

**Trách nhiệm của API Gateway:**

| Responsibility | Mô tả |
|----------------|-------|
| **TLS Termination** | Decrypt HTTPS ở đây, internal traffic là HTTP |
| **Authentication** | Validate JWT, không để từng service tự validate |
| **Rate Limiting** | Giới hạn request/second per IP, per user |
| **Routing** | `/orders/*` → order-service, `/payments/*` → payment-service |
| **Load Balancing** | Round-robin giữa instances của cùng service |
| **Request Logging** | Log tất cả request ở một chỗ |
| **Response Caching** | Cache GET response để giảm load |

**Tools:** Kong, AWS API Gateway, Nginx, Traefik, Envoy.

---

### BFF — Backend for Frontend

**Vấn đề:** Mobile app chỉ cần 3 field từ `GET /orders/:id` nhưng API trả về 20 field.  
Web app cần aggregate data từ 3 service khác nhau cho 1 page.

```
Mobile BFF:             Web BFF:
  - Trả ít data hơn      - Aggregate từ nhiều service
  - Optimize bandwidth   - Trả đúng shape data cho web component
  - Specific endpoints   - Handle web-specific pagination/filter
```

**Airbnb** áp dụng BFF: Mobile có BFF riêng trả response nhỏ hơn, ít battery hơn.  
**SoundCloud** phổ biến hóa pattern này.

---

## 13. Idempotency

### Idempotency là gì?

> **Idempotent operation:** Gọi 1 lần hay 100 lần, kết quả giống nhau.

```
Idempotent:     DELETE /users/123 (gọi lần 2 → user đã xóa rồi, không có gì thay đổi)
                GET /users/123    (đọc không thay đổi state)

NOT idempotent: POST /payments/charge (gọi 2 lần → charge 2 lần!)
                POST /emails/send     (gọi 2 lần → user nhận 2 email!)
```

---

### Idempotency Key Pattern

```
Client gán 1 unique key cho mỗi "intent" (không phải mỗi request):

POST /payments/charge
  Headers:
    Idempotency-Key: client-generated-uuid-abc123
  Body:
    { amount: 500000, userId: "u123" }

Server logic:
  1. Check: đã xử lý key "abc123" chưa?
  2. Nếu rồi → trả lại cached response (không charge lại)
  3. Nếu chưa → xử lý, lưu result với key "abc123", trả response

Stripe, Braintree, PayPal đều implement pattern này.
```

```javascript
// Express middleware cho idempotency
async function idempotencyMiddleware(req, res, next) {
  const key = req.headers['idempotency-key'];
  if (!key) return next();

  const cached = await redis.get(`idempotency:${key}`);
  if (cached) {
    const { statusCode, body } = JSON.parse(cached);
    return res.status(statusCode).json(body);
  }

  // Intercept response để cache
  const originalJson = res.json.bind(res);
  res.json = (body) => {
    if (res.statusCode < 500) { // Chỉ cache success và 4xx
      redis.setex(`idempotency:${key}`, 86400, JSON.stringify({
        statusCode: res.statusCode,
        body
      }));
    }
    return originalJson(body);
  };

  next();
}
```

---

### At-least-once + Idempotent Consumer = Exactly-once Semantics

```
Kafka/RabbitMQ: at-least-once delivery (message có thể đến 2 lần)
Consumer: idempotent (xử lý 2 lần = 1 lần)
Kết quả: exactly-once BEHAVIOR (dù message đến nhiều lần)

Key insight: Exactly-once không đến từ transport layer —
             mà đến từ idempotent consumer logic.
```

---

## 14. Outbox Pattern

### Dual Write Problem — Vấn đề gốc

```
Tình huống:
1. Lưu order vào DB ✅
2. Publish event "order.created" vào Kafka ❌ (mất kết nối)

→ DB có order nhưng không có event → inventory không giảm → inconsistency

Hoặc ngược lại:
1. Publish event "order.created" ✅
2. Lưu order vào DB ❌ (DB crash)

→ Event publish nhưng order không tồn tại trong DB
```

Không thể wrap DB write và message publish trong cùng 1 transaction (khác system).

---

### Outbox Pattern — Giải pháp

```
Nguyên tắc: Ghi vào DB và Outbox Table trong CÙNG 1 DB transaction.
            Outbox Processor đọc outbox table và publish lên message broker.

┌──────────────────────────────────────────────────────┐
│                    Database                          │
│  ┌─────────────┐         ┌─────────────────────────┐ │
│  │  orders     │         │  outbox                 │ │
│  │  table      │  ←── 1  │  (id, event_type, data) │ │
│  │  (new row)  │  transaction                      │ │
│  └─────────────┘         └─────────────────────────┘ │
└────────────────────────────────┬─────────────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   Outbox Processor       │
                    │   (polling / CDC)        │
                    └────────────┬─────────────┘
                                 │ publish
                                 ▼
                           Kafka / RabbitMQ
```

```javascript
// Trong 1 DB transaction (ví dụ với Knex/Sequelize):
await db.transaction(async (trx) => {
  // 1. Lưu order
  const order = await trx('orders').insert({ productId, quantity, status: 'pending' });

  // 2. Ghi outbox entry (cùng transaction)
  await trx('outbox').insert({
    event_type: 'order.created',
    payload: JSON.stringify({ orderId: order.id, productId, quantity }),
    created_at: new Date(),
    published: false
  });
  // Nếu bất kỳ step nào fail → cả 2 rollback → không inconsistency
});

// Outbox Processor (chạy riêng, polling mỗi 100ms):
setInterval(async () => {
  const events = await db('outbox').where({ published: false }).limit(100);
  for (const event of events) {
    await kafkaProducer.send({ topic: event.event_type, messages: [{ value: event.payload }] });
    await db('outbox').where({ id: event.id }).update({ published: true });
  }
}, 100);
```

**CDC approach** (Debezium): Thay vì polling, đọc database write-ahead log (WAL) real-time → hiệu quả hơn, latency thấp hơn.

---

## 15. Health Check — Liveness vs Readiness

### Liveness vs Readiness — Tại sao cần phân biệt?

```
Kubernetes gọi 2 loại health check khác nhau:

Liveness Probe: "Service còn sống không?"
  → Fail → Kubernetes restart pod
  → Dùng khi: service bị deadlock, out of memory, unresponsive

Readiness Probe: "Service sẵn sàng nhận traffic không?"
  → Fail → Kubernetes KHÔNG gửi traffic vào pod này (nhưng không restart)
  → Dùng khi: đang khởi động, đang chạy migration, DB connection chưa ready
```

---

### Implement trong project này

```javascript
// Liveness: service còn chạy không? (minimal check)
app.get('/health/live', (req, res) => {
  res.json({ status: 'alive' });
});

// Readiness: tất cả dependencies sẵn sàng chưa?
app.get('/health/ready', async (req, res) => {
  const checks = {
    rabbitmq: !!getRabbitChannel(),
    kafka: !!getKafkaProducer(),
    // db: await checkDbConnection()
  };

  const allReady = Object.values(checks).every(Boolean);
  res.status(allReady ? 200 : 503).json({
    status: allReady ? 'ready' : 'not_ready',
    checks
  });
});
```

**Kubernetes deployment:**

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 3001
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /health/ready
    port: 3001
  initialDelaySeconds: 5
  periodSeconds: 5
```

---

## 16. Backpressure & Rate Limiting

### Backpressure — Consumer chậm hơn Producer

```
Tình huống:

Producer: publish 10,000 messages/second
Consumer: xử lý được 1,000 messages/second

Không có backpressure:
  Queue tăng 9,000 messages/second
  → Sau 1 phút: 540,000 messages tồn đọng
  → Memory exhausted → consumer crash → mất messages

Với backpressure:
  Consumer signal broker "tôi đang đầy, dừng đẩy"
  Producer phải chậm lại hoặc hệ thống load shed (drop request)
```

---

### Rate Limiting — Giới hạn request từ client

**Token Bucket Algorithm** (phổ biến nhất):

```
Bucket sức chứa: 100 tokens
Refill rate: 10 tokens/second

Request đến → lấy 1 token từ bucket
  Bucket còn token → xử lý request ✅
  Bucket rỗng     → từ chối request (429 Too Many Requests) ❌

Cho phép burst ngắn (dùng hết 100 tokens) nhưng average rate = 10 req/s
```

**Leaky Bucket Algorithm** (smooth rate):

```
Nước (requests) đổ vào bucket
Bucket nhỏ giọt ở rate cố định (ví dụ: 10 req/s)
Bucket tràn → drop request

Không cho phép burst — rate luôn đều đặn
Tốt cho billing/payment API
```

---

### Rate Limiting Implementation

```javascript
// Dùng redis để rate limit distributed (nhiều instances)
const rateLimit = require('express-rate-limit');
const RedisStore = require('rate-limit-redis');

app.use('/api/', rateLimit({
  windowMs: 60 * 1000,  // 1 minute window
  max: 100,             // 100 requests per window per IP
  standardHeaders: true,
  store: new RedisStore({ client: redisClient }),
  handler: (req, res) => {
    res.status(429).json({
      message: 'Too many requests, please try again later.',
      retryAfter: Math.ceil(req.rateLimit.resetTime / 1000)
    });
  }
}));
```

---

## 17. Bulkhead Pattern

### Bulkhead — Ngăn cách để một lỗi không lan sang phần khác

Tên lấy từ khoang tàu (bulkhead): Nếu một khoang thủng, nước không tràn sang khoang khác.

```
Không có Bulkhead:

┌─────────────────────────────────────────────────┐
│              Shared Thread Pool (50 threads)    │
│  order.sync requests: 45 threads (đang chờ     │
│                       inventory-service chết)   │
│  order.async requests: 5 threads còn lại       │
│  → async requests cũng bị chậm vì hết thread   │
└─────────────────────────────────────────────────┘

Có Bulkhead:

┌──────────────────────┐   ┌──────────────────────┐
│ Sync Thread Pool     │   │ Async Thread Pool    │
│ (20 threads)         │   │ (30 threads)         │
│ → inventory down     │   │ → vẫn chạy bình thường│
│ → 20 threads stuck   │   │                      │
│ → chỉ sync bị ảnh hưởng│  └──────────────────────┘
└──────────────────────┘
```

---

### Bulkhead ở cấp độ Connection Pool

```javascript
// Tách connection pool cho các loại operation khác nhau
const readPool = new Pool({ max: 20 });   // cho GET requests
const writePool = new Pool({ max: 5 });   // cho POST/PUT/DELETE

// Nếu có spike writes → write pool đầy, nhưng read pool vẫn phục vụ GET
// Đảm bảo dashboard vẫn load được dù có batch import đang chạy
```

---

## 18. Service Discovery & Load Balancing

### Service Discovery — Làm sao services tìm thấy nhau?

```
Hardcode IP (❌ không scale):
  order-service: INVENTORY_URL = "http://192.168.1.10:3002"
  → Server di chuyển → URL thay đổi → phải redeploy order-service

Service Discovery (✅):
  Mỗi service đăng ký vào Service Registry khi startup
  Khi cần gọi → query Registry: "inventory-service ở đâu?"
  → Registry trả về list healthy instances
```

```
Tools:
  Consul      ← đơn giản, battle-tested
  Kubernetes  ← built-in DNS-based discovery
  Eureka      ← Netflix, Java ecosystem
  etcd        ← Kubernetes dùng bên dưới

Kubernetes DNS:
  inventory-service → automatically accessible tại:
  http://inventory-service.default.svc.cluster.local:3002
```

---

### Client-side vs Server-side Load Balancing

```
Server-side LB (truyền thống):
  Client ──► Load Balancer (Nginx) ──► Server A
                                    ──► Server B
                                    ──► Server C
  Client không biết gì về các server.

Client-side LB (microservice):
  Client biết list servers từ Service Registry
  Client tự chọn server (round-robin, least-connection...)
  → Loại bỏ load balancer như single point of failure
  → Ví dụ: Netflix Ribbon, Spring Cloud LoadBalancer
```

---

## Tổng kết — Mental Model

```
Request đến hệ thống:

External Client
      │
      ▼
  API Gateway          ← Rate limit, Auth, SSL, Routing
      │
      ▼
  Service A            ← Business logic
  │   │   │
  │   │   └──► gRPC ──► Service B    (sync, internal, performance-critical)
  │   └──────► REST ──► Service C    (sync, external-facing)
  └──────────► Kafka ──► Service D   (async, event-driven, no response needed)
                     ──► Service E   (same event, different consumer group)

Mỗi service:
  ┌─────────────────────────────────┐
  │  Connection Pool (DB, Redis)    │
  │  Circuit Breaker (downstream)  │
  │  Rate Limiter (incoming)       │
  │  Structured Logger (Loki)      │
  │  Health Check (/health/ready)  │
  └─────────────────────────────────┘
```

> Microservice không phải là mục tiêu — là công cụ.  
> Mỗi pattern ở đây giải quyết một vấn đề cụ thể.  
> Chỉ áp dụng khi bạn gặp vấn đề đó.
