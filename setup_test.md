# Setup & Test Guide — Microservice Learning 2026

## Ports nhanh

| Service | Port | Giao thức |
|---|---|---|
| order-service | 3001 | HTTP REST |
| inventory-service | 3002 | HTTP REST + gRPC :50051 |
| payment-service | 3003 | HTTP REST |
| ui-service (dashboard) | 3004 | HTTP + SSE |
| analytics-service | 3005 | HTTP REST |
| RabbitMQ broker | 5672 | AMQP |
| RabbitMQ UI | 15672 | HTTP (guest/guest) |
| Kafka broker | 9092 | Kafka |
| Kafka UI | 8080 | HTTP |
| Loki | 3100 | HTTP |
| Grafana | 3000 | HTTP (admin/admin) |
| PostgreSQL | 5432 | TCP |

---

## 1. Khởi động hệ thống

### Bước 1 — Start infrastructure (Docker)

```bash
# Tất cả infra một lệnh
docker compose up -d

# Hoặc từng service nếu máy yếu:
docker compose up -d rabbitmq kafka kafka-ui loki grafana postgres
```

Chờ ~20 giây để Kafka ready. Kiểm tra:

```bash
docker compose ps
# Tất cả phải có Status: running (healthy)
```

### Bước 2 — Start Node.js services (mỗi tab terminal riêng)

```bash
# Terminal 1
cd order-service && npm install && node server.js

# Terminal 2
cd inventory-service && npm install && node server.js

# Terminal 3
cd payment-service && npm install && node server.js

# Terminal 4
cd ui-service && npm install && node server.js

# Terminal 5 (optional — Consumer Groups demo)
cd analytics-service && npm install && node server.js
```

### Bước 3 — Mở dashboard

```
http://localhost:3004
```

---

## 2. Reset / Wipe toàn bộ

```bash
# Xóa containers + volumes (DB bị reset về init.sql)
docker compose down -v

# Start lại fresh
docker compose up -d
```

---

## Test Cases

---

### TEST 1 — REST Sync (không có resilience)

**Mục đích bài học**: Hiểu call chain đồng bộ cơ bản, tại sao cần resilience.

**Route**: `POST /orders/sync`

```bash
# ✓ Thành công
curl -X POST http://localhost:3001/orders/sync \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'

# ✗ Hết hàng
curl -X POST http://localhost:3001/orders/sync \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":999}'
```

**Kết quả mong đợi**:
- Thành công: `200 { "message": "Order placed successfully [sync-basic]" }`
- Hết hàng: `400 { "message": "Out of stock" }`

**Quan sát**: Nếu tắt inventory-service → request treo vô thời hạn (không có timeout).

---

### TEST 2 — REST Sync với Timeout + Retry + Circuit Breaker

**Mục đích bài học**: Hiểu 3 lớp bảo vệ và thứ tự: Circuit Breaker → Retry → Timeout.

**Route**: `POST /orders/sync-resilient`

#### 2a. Normal call

```bash
curl -X POST http://localhost:3001/orders/sync-resilient \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'
```

**Kết quả**: `200 OK` trong < 200ms.

#### 2b. Timeout demo

```bash
curl -X POST http://localhost:3001/orders/sync-resilient \
  -H "Content-Type: application/json" \
  -d '{"productId":"TRIGGER-SLOW","quantity":1}'
```

**Kết quả**: `504 Gateway Timeout` sau ~9 giây (3 lần retry × 3s timeout).

**Điều xảy ra bên trong**:
```
Lần 1: gọi inventory → TRIGGER-SLOW → chờ 3s → timeout
Lần 2: retry → chờ 3s → timeout
Lần 3: retry → chờ 3s → timeout
→ Tất cả retry thất bại → trả 504
```

#### 2c. Circuit Breaker demo

```bash
# Gửi 4 lần liên tiếp để trigger circuit open
for i in 1 2 3 4; do
  curl -s -X POST http://localhost:3001/orders/sync-resilient \
    -H "Content-Type: application/json" \
    -d '{"productId":"TRIGGER-ERROR","quantity":1}' | python -m json.tool
  sleep 0.5
done
```

**Kết quả**:
- Lần 1-3: `500` (lỗi, retry hết)
- Lần 4: `503 "Inventory service temporarily unavailable"` — trả ngay không chờ (Circuit OPEN)

Xem trạng thái CB:
```bash
curl http://localhost:3001/orders/circuit-status
# → {"state":"OPEN","failures":3,...}
```

**Điều quan trọng**: Circuit OPEN = fail fast, không lãng phí thread chờ service đã biết là down.

---

### TEST 3 — gRPC vs REST so sánh

**Mục đích bài học**: Thấy sự khác biệt protocol — HTTP/2 binary vs HTTP/1.1 JSON.

#### 3a. REST check stock

```bash
curl http://localhost:3002/inventory/IPHONE-15
```

**Kết quả**: JSON text `{"productId":"IPHONE-15","stock":10,"available":true}`

#### 3b. gRPC check stock (qua order-service proxy)

```bash
curl -X POST http://localhost:3001/orders/sync-grpc \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'
```

**Kết quả**: `200 { "message": "Order placed successfully [gRPC]", "protocol": "gRPC" }`

#### 3c. gRPC — NOT_FOUND

```bash
curl -X POST http://localhost:3001/orders/sync-grpc \
  -H "Content-Type: application/json" \
  -d '{"productId":"INVALID-SKU","quantity":1}'
```

**Kết quả**: `404` — gRPC status NOT_FOUND (5) → HTTP 404.

**Quan sát trên Dashboard**: Tab "REST/gRPC" → click button → Packet Inspector hiện side-by-side:
- REST: text JSON ~200 bytes, HTTP/1.1
- gRPC: binary hex ~80 bytes, HTTP/2 — nhỏ hơn ~60%

---

### TEST 4 — RabbitMQ Async (fire-and-forget cơ bản)

**Mục đích bài học**: Fire-and-forget pattern — order-service không chờ inventory xử lý xong.

```bash
curl -X POST http://localhost:3001/orders/async \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'
```

**Kết quả**: `202 Accepted` — trả ngay lập tức, processing in background.

**Quan sát**:
1. Mở RabbitMQ UI: `http://localhost:15672` (guest/guest) → Queues → thấy message được route
2. Dashboard Tab "RabbitMQ" → thấy packet animation qua exchange → queue → consumer

**So sánh với REST sync**:
- REST sync: client chờ → inventory xử lý → trả kết quả → client nhận (~50ms)
- RabbitMQ async: client nhận `202` ngay (~2ms) → inventory xử lý sau

**Hạn chế**: Client không biết order cuối cùng success hay fail (cần polling hoặc webhook).

---

### TEST 4b — RabbitMQ SAGA (full flow qua payment-service)

**Mục đích bài học**: SAGA pattern hoàn chỉnh qua RabbitMQ — so sánh trực tiếp với Kafka SAGA (Test 5).

```
POST /orders/async
  → order_events (fanout)
  → inventory-service: reserve → publish inventory_events
  → payment-service: charge →
       success → publish payment_completed_events → inventory confirmSale()
       fail    → publish saga_events              → inventory releaseReserve()
```

#### 4b-1. Happy path — reserve → payment OK → confirm sale

```bash
curl -X POST http://localhost:3001/orders/async \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'
```

**Luồng log mong đợi** (theo thứ tự thời gian):
```
inventory-service: [RabbitMQ Async] RESERVE success. Stock remaining: 9
payment-service:   [RabbitMQ SAGA] Processing payment for orderId=..., productId=IPHONE-15
payment-service:   [RabbitMQ SAGA] Payment SUCCESS — orderId=... amount=... VND
inventory-service: [RabbitMQ SAGA] CONFIRM SALE — stock deducted for orderId=..., product=IPHONE-15
```

**Kiểm tra DB** — stock thật phải giảm 1:
```bash
docker exec local_postgres psql -U inventory_user -d inventory_db \
  -c "SELECT product_id, stock, reserved FROM inventory WHERE product_id='IPHONE-15';"
# stock = 9, reserved = 0
```

#### 4b-2. Out of stock → DLQ, payment-service không nhận

```bash
curl -X POST http://localhost:3001/orders/async \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":999}'
```

**Luồng log**:
```
inventory-service: [RabbitMQ Async] RESERVE failed — out_of_stock
inventory-service: [DLQ] Message failed - routed to Dead Letter Queue
```

Payment-service **không nhận được gì** vì inventory không publish `inventory_events` khi reserve fail.

Verify trên RabbitMQ UI: Queues → `inventory_order_failed_queue` → Get messages → thấy `x-death` header.

#### 4b-3. SAGA Rollback — payment từ chối

```bash
curl -X POST http://localhost:3001/orders/async \
  -H "Content-Type: application/json" \
  -d '{"productId":"FAIL-PAYMENT","quantity":1}'
```

**Luồng log**:
```
inventory-service: [RabbitMQ Async] RESERVE success. Stock remaining: 98
payment-service:   [RabbitMQ SAGA] Processing payment for orderId=..., productId=FAIL-PAYMENT
payment-service:   [RabbitMQ SAGA] Payment FAILED — card declined. SAGA rollback triggered.
inventory-service: [SAGA] ROLLBACK success — stock released for FAIL-PAYMENT
```

**Kiểm tra DB** — reserved phải về 0 sau rollback:
```bash
docker exec local_postgres psql -U inventory_user -d inventory_db \
  -c "SELECT product_id, stock, reserved FROM inventory WHERE product_id='FAIL-PAYMENT';"
# reserved = 0
```

**So sánh RabbitMQ SAGA vs Kafka SAGA:**

| | RabbitMQ SAGA | Kafka SAGA |
|---|---|---|
| Route trigger | `POST /orders/async` | `POST /orders/kafka-saga` |
| Message broker | fanout exchanges | topics |
| Replay khi restart | ❌ (message mất nếu chưa ack) | ✅ (offset giữ nguyên) |
| Message order | Không đảm bảo | Đảm bảo trong 1 partition |
| Dead letter | ✅ DLX/DLQ có sẵn | ❌ phải tự xử lý |
| Khi nào dùng | Task queue, cần DLQ | Event sourcing, cần replay |

---

### TEST 5 — Kafka SAGA (luồng phức tạp nhất)

**Mục đích bài học**: Choreography SAGA pattern — 3 service phối hợp qua Kafka không gọi nhau trực tiếp.

```
order-service → [order-events] → inventory-service → [inventory-events] → payment-service
                                                                                ↓
                                                              [payment-events] → inventory-service (rollback nếu cần)
```

#### 5a. Happy path — đặt hàng thành công

```bash
curl -X POST http://localhost:3001/orders/kafka-saga \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'
```

**Luồng**:
1. order-service publish `order.created` → `order-events`
2. inventory-service consume → reserve stock → publish `inventory.reserved` → `inventory-events`
3. payment-service consume → charge card → publish `payment.completed` → `payment-events`
4. inventory-service consume `payment.completed` → confirm sale (trừ stock thật)

**Kiểm tra DB sau**:
```bash
docker exec local_postgres psql -U inventory_user -d inventory_db \
  -c "SELECT product_id, stock, reserved FROM inventory WHERE product_id='IPHONE-15';"
# stock giảm 1, reserved về 0
```

#### 5b. Out of stock — SAGA dừng sớm

```bash
curl -X POST http://localhost:3001/orders/kafka-saga \
  -H "Content-Type: application/json" \
  -d '{"productId":"FAKE-SKU","quantity":1}'
```

**Luồng**: inventory-service publish `inventory.failed` → payment-service **không xử lý** → SAGA kết thúc.

#### 5c. SAGA Rollback — payment thất bại

```bash
curl -X POST http://localhost:3001/orders/kafka-saga \
  -H "Content-Type: application/json" \
  -d '{"productId":"FAIL-PAYMENT","quantity":1}'
```

**Luồng**:
1. inventory-service: FAIL-PAYMENT có hàng → **reserve thành công** → `inventory.reserved`
2. payment-service: nhận FAIL-PAYMENT → **từ chối** → publish `payment.failed`
3. inventory-service: nhận `payment.failed` → **rollback reserve** (compensating transaction)

**Điều quan trọng**: Đây là SAGA rollback — không có distributed transaction, thay vào đó là compensating action.

**Kiểm tra DB**:
```bash
docker exec local_postgres psql -U inventory_user -d inventory_db \
  -c "SELECT product_id, stock, reserved FROM inventory WHERE product_id='FAIL-PAYMENT';"
# reserved phải về 0 sau rollback
```

**Quan sát trên Dashboard**: Tab "Kafka SAGA" → thấy 3 packet animation qua 3 topics.

---

### TEST 6 — Consumer Groups (Kafka fan-out)

**Mục đích bài học**: Hiểu tại sao Kafka consumer groups cho phép nhiều service đọc cùng 1 topic độc lập.

#### 6a. Gửi event → cả 2 groups nhận

```bash
curl -X POST http://localhost:3001/orders/stream \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'
```

**Điều xảy ra**:
- `analytics-group` (analytics-service :3005) nhận event → cập nhật stats
- `ui-service-group` (ui-service :3004) nhận event → push SSE → dashboard

**Kiểm tra analytics-service**:
```bash
curl http://localhost:3005/analytics/summary
# → {"totalEvents":1,"topProducts":[{"productId":"IPHONE-15","count":1}],...}
```

#### 6b. Gửi nhiều events

```bash
for product in IPHONE-15 MACBOOK-M3 IPHONE-15 LAPTOP-MODULAR-TEST IPHONE-15; do
  curl -s -X POST http://localhost:3001/orders/stream \
    -H "Content-Type: application/json" \
    -d "{\"productId\":\"$product\",\"quantity\":1}" > /dev/null
done

curl http://localhost:3005/analytics/summary
# → topProducts: IPHONE-15 count=3, MACBOOK-M3 count=1, ...
```

#### 6c. Kiểm tra Kafka UI — xem lag

Mở `http://localhost:8080` → Consumer Groups → `analytics-group`:
- **Lag = 0** khi service đang chạy bình thường
- **Lag > 0** khi service bị restart — số message chưa xử lý

**Quan sát trên Dashboard**: Tab "Consumer Groups" → click "⚡ Crash" một group → gửi events → group kia vẫn tăng counter → click Restart → group crash tự replay đủ messages bị bỏ lỡ.

---

### TEST 7 — Centralized Logging (Grafana + Loki)

**Mục đích bài học**: Tập hợp logs từ nhiều service vào 1 nơi, tìm theo trace_id xuyên service.

**Bước 1**: Gửi một request SAGA:

```bash
curl -s -X POST http://localhost:3001/orders/kafka-saga \
  -H "Content-Type: application/json" \
  -d '{"productId":"IPHONE-15","quantity":1}'
# Lấy trace_id từ response
```

**Bước 2**: Mở Grafana `http://localhost:3000` (admin/admin) → Explore → Loki.

**Query tất cả services**:
```
{app=~"order-service|inventory-service|payment-service"} | json
```

**Query theo trace_id** (paste trace_id từ response):
```
{app=~"order-service|inventory-service|payment-service"} | json | trace_id="<your-trace-id>"
```

**Kết quả**: Thấy toàn bộ hành trình của 1 request xuyên qua 3 service theo thứ tự thời gian.

---

### TEST 8 — PostgreSQL SELECT FOR UPDATE (race condition demo)

**Mục đích bài học**: Hiểu tại sao cần row-level lock khi nhiều request cùng cập nhật 1 row.

```bash
# Gửi 20 requests đồng thời (chỉ có 10 stock)
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:3001/orders/kafka-saga \
    -H "Content-Type: application/json" \
    -d '{"productId":"IPHONE-15","quantity":1}' &
done
wait

# Kiểm tra kết quả — stock không được âm
docker exec local_postgres psql -U inventory_user -d inventory_db \
  -c "SELECT product_id, stock, reserved FROM inventory WHERE product_id='IPHONE-15';"
```

**Kết quả mong đợi**: `stock >= 0` — SELECT FOR UPDATE đảm bảo serialized writes, không có race condition.

**Nếu không có lock**: Có thể thấy `reserved > stock` (double-booking).

---

## Cheat sheet lệnh hay dùng

```bash
# Xem log inventory-service real-time
cd inventory-service && node server.js 2>&1 | grep -v "heartbeat"

# Check tất cả container
docker compose ps

# Xem log container postgres
docker logs local_postgres --tail 20

# Query DB trực tiếp
docker exec -it local_postgres psql -U inventory_user -d inventory_db
# Trong psql:
#   SELECT * FROM inventory;
#   \q

# Reset stock về ban đầu (xóa volume)
docker compose down -v && docker compose up -d postgres

# Xem Kafka topics
curl http://localhost:8080/api/clusters/local-cluster/topics
```
