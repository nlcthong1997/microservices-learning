# Interview Prep — Microservice & Full-stack

---

## 1. Tiếng Anh — Tự giới thiệu

### Giới thiệu bản thân (1–2 phút)

> "Hi, my name is [Tên]. I have [X] years of experience as a software engineer, mainly focusing on backend and full-stack development.
>
> In my current/most recent project, I worked on a microservice-based system where I was responsible for [order service / authentication / infrastructure]. I've been working with technologies like Node.js, Kafka, RabbitMQ, and PostgreSQL on the backend, and React on the frontend.
>
> I enjoy building scalable systems and I'm particularly interested in distributed systems and event-driven architecture — which is why this role caught my attention."

### Giới thiệu dự án & vai trò

> "The project is an e-commerce platform built with a microservice architecture. We have around [N] services: order service, inventory service, payment service, and a few supporting ones.
>
> My role was [backend developer / tech lead]. I designed and implemented the order flow end-to-end — from the REST API that receives orders, to publishing events on Kafka, to consuming those events in downstream services. I also set up the RabbitMQ SAGA pattern for distributed transaction handling and configured our logging pipeline with Loki and Grafana."

---

## 2. Tiếng Việt — Microservices trong dự án

### Giới thiệu microservices đang làm

Trả lời theo 3 tầng:

1. **Tại sao dùng microservice** — scale độc lập từng service, team làm việc song song, deploy riêng không ảnh hưởng toàn hệ thống
2. **Kiến trúc cụ thể** — liệt kê các service (order, inventory, payment, analytics, ui), port, trách nhiệm từng service
3. **Giao tiếp** — dẫn sang câu tiếp theo

### Các service giao tiếp như thế nào?

Có **2 loại giao tiếp**, dùng tùy use case:

**Đồng bộ (Synchronous):**
- REST HTTP giữa order-service → inventory-service (GĐ1)
- gRPC — HTTP/2 + Protobuf binary (GĐ6) — nhỏ hơn REST ~80%, type-safe hơn
- Dùng khi: cần kết quả ngay lập tức (check stock trước khi xác nhận đơn)

**Bất đồng bộ (Asynchronous):**
- **RabbitMQ** — Exchange/Queue pattern, fanout. order-service publish vào `order_events` exchange, inventory-service tự tạo queue và bind vào exchange đó. Không service nào biết service kia.
- **Kafka** — topic/partition/consumer group. Dùng cho SAGA pattern và user behavior analytics.
- Dùng khi: không cần kết quả ngay, tăng throughput, loose coupling

**Câu hay bị hỏi thêm:** "Khi nào dùng Kafka, khi nào dùng RabbitMQ?"
> - RabbitMQ: message routing phức tạp, cần DLX/retry, SAGA choreography với nhiều bước bù trừ
> - Kafka: cần replay (rewind offset), analytics, fan-out nhiều consumer group, message volume lớn

---

## 3. Authentication & Authorization

### Kể flow đang làm trong dự án (MSAL)

```
User click Login
  → redirect tới Microsoft Login (Azure AD)
  → user nhập credential
  → Azure AD trả về Authorization Code
  → MSAL exchange code lấy access_token + id_token + refresh_token
  → redirect về callback URL (redirect_uri trong MSAL config)
  → FE lưu token
  → Mỗi request gửi kèm Authorization: Bearer <access_token>
  → BE validate token (verify signature với Azure AD public key)
  → BE check claims (roles, scopes)
  → Trả về data
```

### Làm sao redirect về trang sau khi login thành công?

- MSAL config có `redirectUri` — phải đăng ký trong Azure AD App Registration
- Có thể dùng `loginHint` hoặc `state` param để truyền URL gốc, sau khi redirect về thì FE đọc `state` và navigate đến đúng trang
- MSAL có `handleRedirectPromise()` để xử lý callback

### Lưu token ở đâu?

| Storage | XSS Risk | CSRF Risk | Dùng khi |
|---|---|---|---|
| `localStorage` | ⚠️ Cao | Không | Tiện nhưng không an toàn với XSS |
| `sessionStorage` | ⚠️ Cao | Không | Chỉ sống trong tab, an toàn hơn localStorage |
| `httpOnly cookie` | ✅ An toàn | ⚠️ Cần CSRF token | Production best practice |
| In-memory (JS var) | ✅ Tốt nhất | Không | SPA — mất khi refresh |

**Best practice:** access_token trong memory, refresh_token trong httpOnly cookie.

MSAL mặc định dùng `sessionStorage` hoặc `localStorage` — cấu hình được qua `cacheLocation`.

### Bảo mật chống XSS

- httpOnly cookie — JS không đọc được
- Content Security Policy (CSP) header — giới hạn script source
- Sanitize input — không render HTML từ user input thẳng vào DOM
- `DOMPurify` nếu cần render rich text
- Không dùng `innerHTML` với data từ server

### Sau khi login, FE gửi request, BE làm gì?

```
FE: axios.get('/api/orders', { headers: { Authorization: 'Bearer eyJ...' } })
  → API Gateway / Auth middleware
      1. Extract token từ Authorization header
      2. Fetch public key từ Azure AD (JWKS endpoint) — cache lại
      3. jwt.verify(token, publicKey) — check signature + expiry
      4. Decode claims: sub (userId), roles, oid (object ID)
      5. Attach decoded user vào req.user
      6. Next() → controller xử lý
      7. Nếu verify fail → 401 Unauthorized
      8. Nếu không có role cần thiết → 403 Forbidden
```

---

## 4. API Gateway — Chống Burst Traffic

### Rate Limiting

- **Token Bucket**: mỗi user có bucket chứa N token. Mỗi request tiêu 1 token. Token refill theo thời gian. Vượt quá → 429 Too Many Requests.
- **Fixed Window**: đếm request trong window 1 phút, vượt ngưỡng thì block.
- **Sliding Window**: chính xác hơn Fixed Window, không bị "double burst" ở ranh giới window.

Implementation: Redis + Lua script (atomic) hoặc dùng thư viện có sẵn (`express-rate-limit`, Kong Rate Limiting plugin).

### Circuit Breaker

- CLOSED → OPEN khi error rate vượt ngưỡng
- OPEN: fail ngay, không forward request xuống service → tránh cascade failure
- HALF_OPEN: thả một số request thử, nếu ok thì CLOSE lại

### Các kỹ thuật khác

- **Queue/Backpressure**: không drop request mà đưa vào queue, xử lý dần (dùng khi SLA cho phép latency cao hơn)
- **Horizontal scaling** + Load balancer
- **Caching** ở Gateway layer — cache response của GET request

---

## 5. WebSocket

### Config & khởi tạo

```js
// Server (Node.js + socket.io)
const io = require('socket.io')(httpServer, {
  cors: { origin: '*' },
  maxHttpBufferSize: 1e6,  // 1MB max payload
  pingTimeout: 20000,      // 20s không ping → disconnect
  pingInterval: 25000,     // server ping mỗi 25s
});

io.on('connection', (socket) => {
  console.log('connected:', socket.id);
  socket.on('disconnect', (reason) => { /* xử lý */ });
});
```

### Flow giao tiếp

```
Browser → HTTP Upgrade request (header: Upgrade: websocket)
Server  → 101 Switching Protocols
Kết nối TCP giữ mở hai chiều
Browser/Server có thể push bất kỳ lúc nào
Không cần polling, không overhead mỗi request
```

### Làm sao nhận biết user ngắt kết nối?

- Socket.io: event `disconnect` fire tự động khi mất kết nối (tab đóng, mạng mất, browser crash)
- `socket.on('disconnect', reason => { })` — `reason` có thể là: `transport close`, `ping timeout`, `server namespace disconnect`
- Với `pingTimeout/pingInterval`: server ping định kỳ, nếu client không pong trong `pingTimeout` ms → server tự đóng socket và fire event `disconnect`

### MAX_CONNECTIONS = 50, user mở 100 tabs

Mỗi tab = **1 socket connection riêng biệt** (khác nhau hoàn toàn).

- 50 tab đầu → connect thành công
- Tab 51-100 → tùy implementation:
  - Nếu server enforce `maxConnections` → new connection bị reject ngay
  - Nếu không có limit cứng → server chấp nhận nhưng có thể quá tải
  - Trong thực tế: dùng Redis để đếm connection per userId, vượt ngưỡng thì trả lỗi và FE thông báo "quá nhiều phiên đăng nhập"

**Cách xử lý đúng trong production:**
- Track `userId → [socketIds]` trong Redis
- Khi connect: kiểm tra số connection hiện tại của userId
- Nếu vượt MAX_PER_USER → force disconnect socket cũ nhất, hoặc reject mới
- Khi disconnect: cleanup Redis

---

## 6. Kafka

### Dùng Kafka để làm gì?

Trong dự án dùng cho:
1. **User behavior analytics** — mỗi request → publish event vào `user-behavior-logs` topic, analytics-service consume
2. **Choreography SAGA** — order → inventory → payment qua các topic `order-events`, `inventory-events`, `payment-events`
3. **Fan-out** — 1 event, nhiều consumer group nhận độc lập (analytics-group và ui-service-group cùng đọc `user-behavior-logs`)

### Cải thiện performance — các config quan trọng

**Producer side:**
```
acks=1           # chỉ leader ack (vs acks=all chậm hơn, an toàn hơn)
linger.ms=5      # gom nhiều message thành 1 batch, giảm round-trip
batch.size=65536 # 64KB batch
compression.type=lz4  # nén message, giảm network I/O
```

**Consumer side:**
```
fetch.min.bytes=1024   # đợi ít nhất 1KB trước khi trả về fetch
fetch.max.wait.ms=500  # hoặc đợi tối đa 500ms → giảm idle fetch
max.poll.records=500   # xử lý 500 message mỗi lần poll
```

**Broker side:**
```
num.partitions=N        # nhiều partition = song song hơn
replication.factor=3    # HA
log.retention.hours=168 # giữ 7 ngày
```

### Partition Key là gì?

Key quyết định message đi vào partition nào:
```
partition = hash(key) % num_partitions
```

- Không có key → Round-Robin (phân đều)
- Có key → cùng key **luôn vào cùng 1 partition** → đảm bảo ordering cho key đó

Ví dụ: `orderId` làm key → tất cả event của 1 đơn hàng đi vào 1 partition → có thể đảm bảo thứ tự xử lý.

### Chiến lược chia message vào partitions

| Strategy | Cách hoạt động | Khi nào dùng |
|---|---|---|
| **Round-Robin** | Xoay vòng 0→1→2→0→... | Không cần ordering, muốn phân đều |
| **Key-based** | `hash(key) % N` | Cần ordering theo key (orderId, userId) |
| **Sticky** | Gom vào 1 partition đến khi batch đầy, rồi chuyển | Giảm số batch nhỏ, tăng throughput |
| **Custom** | Tự implement Partitioner | Business logic phức tạp |

**Trường hợp 3 partition nhưng message chỉ vào 1:**
- Nguyên nhân: **key bị hot** — tất cả message dùng cùng 1 key
- Hậu quả: 2 partition còn lại rỗng, consumer của chúng idle
- Fix:
  - Thêm suffix ngẫu nhiên vào key: `orderId + "_" + (random % 3)`
  - Dùng composite key: `userId + productCategory`
  - Xem lại business logic để phân tán key tốt hơn

### Đảm bảo thứ tự message

**Trong 1 partition:** Kafka đảm bảo ordering theo offset — message được append và đọc theo thứ tự FIFO.

**Cạm bẫy:** `max.in.flight.requests.per.connection > 1` + retry có thể gây reorder. Fix: set `enable.idempotence=true` → Kafka tự xử lý deduplication và ordering.

**Câu đánh đố: "Đã phân đều vào các partition rồi, làm sao đảm bảo thứ tự?"**

> Thực ra **không thể đảm bảo thứ tự toàn cục** khi có nhiều partition. Đây là trade-off cố ý:
> - Kafka đảm bảo thứ tự **trong cùng partition**
> - Nếu cần thứ tự toàn cục → dùng **1 partition** (mất scalability)
> - Giải pháp thực tế: dùng key để nhóm những message cần ordering vào cùng partition (vd: cùng orderId)
> - Consumer đọc nhiều partition → không thể merge lại theo thứ tự toàn cục, chỉ xử lý độc lập từng partition

### Consumer Group là gì? Tại sao cần?

```
Topic: user-behavior-logs [P0][P1][P2]

Group A (analytics-service):
  consumer A1 → P0, P1
  consumer A2 → P2
  → nhận 100% message, dùng cho analytics

Group B (ui-service):
  consumer B1 → P0, P1, P2
  → nhận 100% message, dùng để push lên dashboard
```

**Tại sao cần:** 2 service khác nhau đều muốn nhận **toàn bộ** message — nếu dùng 1 queue (RabbitMQ style) thì message bị chia đôi. Consumer group giải quyết bằng cách mỗi group giữ **offset riêng**.

**Quy tắc:** 1 partition chỉ được assign cho **1 consumer trong cùng group** tại một thời điểm. Nếu có nhiều consumer hơn partition → consumer thừa sẽ idle.

---

## 7. Log System

### Đang làm trong dự án

- **Winston logger** mỗi service — format JSON, có `trace_id` field
- **Loki** — log aggregation (như Elasticsearch nhưng không index toàn bộ, chỉ index labels → nhẹ hơn nhiều)
- **Grafana** — visualize và query log từ Loki
- **Labels:** `service=order-service`, `level=error` → filter nhanh

```js
// Mỗi request tạo 1 traceId duy nhất
const traceId = req.headers['x-trace-id'] || uuidv4();
logger.info({ trace_id: traceId, message: 'Processing order', orderId });
```

### Có cách nào khác không?

| Stack | Ưu điểm | Nhược điểm |
|---|---|---|
| **ELK** (Elasticsearch + Logstash + Kibana) | Full-text search mạnh, query linh hoạt | Nặng, tốn RAM/disk |
| **Loki + Grafana** | Nhẹ, chỉ index labels, tích hợp tốt với Prometheus | Query kém linh hoạt hơn ES |
| **Datadog / New Relic** | Turnkey, APM tích hợp | Tốn tiền |
| **CloudWatch** (AWS) | Native AWS, không cần tự host | Lock-in AWS |

### Khi có lỗi, check log như thế nào?

1. Mỗi request tạo `trace_id` (UUID) → truyền qua tất cả service qua header `X-Trace-ID`
2. Khi lỗi xảy ra: lấy `trace_id` từ response/alert
3. Query Grafana/Loki: `{trace_id="abc-123"}` → thấy **toàn bộ hành trình** của request qua các service

### Làm sao biết log bị ở service nào?

- **Label `service`** trong Loki: `{service="order-service", level="error"}`
- Kết hợp `trace_id`: query `{trace_id="xyz"}` → Loki trả log từ tất cả service có label đó, sắp xếp theo timestamp → thấy rõ lỗi bắt đầu từ service nào, propagate ra sao
- Nếu dùng distributed tracing (Jaeger/Zipkin): còn thấy được latency từng hop

---

## 8. SDLC & Soft Skills

### Kể về thách thức và bài học

**Framework trả lời: STAR (Situation → Task → Action → Result)**

Ví dụ cấu trúc:
> "Trong dự án X, chúng tôi gặp tình huống [mô tả vấn đề — vd: race condition khi nhiều user cùng đặt hàng sản phẩm last item]. Task của tôi là [fix trước khi release]. Tôi đã [phân tích query, phát hiện thiếu SELECT FOR UPDATE, implement locking + test với concurrent requests]. Kết quả là [zero duplicate order sau khi deploy].
>
> Bài học: luôn test concurrent scenarios với tool như `k6` hoặc `autocannon` trước khi release feature liên quan đến shared resources."

### Đặt tình huống xử lý

Interviewer hay hỏi dạng này để test tư duy hệ thống. Approach:

1. **Clarify** — hỏi lại nếu chưa rõ constraint (scale, latency requirement, consistency level)
2. **Trade-off** — không có giải pháp hoàn hảo, trình bày trade-off
3. **Đề xuất iterative** — bắt đầu đơn giản, scale sau
4. **Hỏi lại feedback** — "Đây có phải hướng anh/chị muốn nghe không?"

---

## Quick Reference — Câu hỏi hay bị vặn

| Câu hỏi | Tóm tắt trả lời |
|---|---|
| Kafka vs RabbitMQ | Kafka: replay/fan-out/throughput cao. RabbitMQ: routing/DLX/SAGA |
| Tại sao fanout không cần routing key? | Broker copy message vào TẤT CẢ queue bind vào exchange, không filter |
| Circuit Breaker khác Retry như nào? | Retry: thử lại. CB: tắt hẳn mạch khi quá nhiều lỗi, tránh cascade |
| SELECT FOR UPDATE làm gì? | Lock row cho transaction hiện tại, transaction khác phải đợi đến COMMIT |
| httpOnly cookie vs localStorage | httpOnly: JS không đọc được → an toàn XSS. localStorage: JS đọc được → dễ bị steal |
| Consumer group tại sao cần? | Mỗi group giữ offset riêng → nhận 100% message độc lập, không chia nhau |
| Thứ tự message Kafka | Chỉ đảm bảo trong 1 partition. Dùng cùng key → cùng partition → có ordering |
