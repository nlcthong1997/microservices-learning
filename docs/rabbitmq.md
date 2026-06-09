# RabbitMQ — Hướng dẫn toàn diện

---

## Mục lục

1. [RabbitMQ là gì?](#1-rabbitmq-là-gì)
2. [Dùng làm gì?](#2-dùng-làm-gì)
3. [Khi nào nên dùng RabbitMQ?](#3-khi-nào-nên-dùng-rabbitmq)
4. [Kiến trúc & Các khái niệm cốt lõi](#4-kiến-trúc--các-khái-niệm-cốt-lõi)
   - [Producer & Consumer](#41-producer--consumer)
   - [Queue](#42-queue)
   - [Exchange](#43-exchange)
   - [Binding & Routing Key](#44-binding--routing-key)
   - [Channel & Connection](#45-channel--connection)
   - [Virtual Host (vhost)](#46-virtual-host-vhost)
   - [Message & Properties](#47-message--properties)
5. [Các loại Exchange](#5-các-loại-exchange)
6. [Message Durability & Acknowledgement](#6-message-durability--acknowledgement)
7. [Dead Letter Exchange (DLX)](#7-dead-letter-exchange-dlx)
8. [Prefetch & QoS](#8-prefetch--qos)
9. [RabbitMQ vs Kafka — so sánh thực tế](#9-rabbitmq-vs-kafka--so-sánh-thực-tế)
10. [Ví dụ thực tế trong dự án microservice](#10-ví-dụ-thực-tế-trong-dự-án-microservice)
11. [Các pattern phổ biến](#11-các-pattern-phổ-biến)
12. [Lưu ý vận hành](#12-lưu-ý-vận-hành)

---

## 1. RabbitMQ là gì?

**RabbitMQ** là một **message broker** mã nguồn mở, được xây dựng trên giao thức **AMQP 0-9-1** (Advanced Message Queuing Protocol).

Hãy hình dung nó như một **bưu cục trung gian**:

```
[Producer]  →→→  [RabbitMQ Broker]  →→→  [Consumer]
  Người gửi          Bưu cục             Người nhận
```

- **Producer** không cần biết ai sẽ xử lý message.
- **Consumer** không cần biết message đến từ đâu.
- RabbitMQ chịu trách nhiệm **định tuyến, lưu trữ tạm, và đảm bảo giao nhận**.

> **Điểm đặc biệt:** RabbitMQ được viết bằng **Erlang** — ngôn ngữ được thiết kế cho hệ thống viễn thông đòi hỏi độ tin cậy cao, concurrent cực lớn và fault-tolerant. Đây là lý do RabbitMQ xử lý hàng triệu message/giây một cách ổn định.

---

## 2. Dùng làm gì?

| Use case | Ví dụ cụ thể |
|---|---|
| **Decoupling services** | Order service gửi event "order created", inventory service lắng nghe và trừ kho |
| **Task queue** | Upload file → đẩy vào queue → worker resize ảnh bất đồng bộ |
| **Pub/Sub** | Một sự kiện "user registered" → gửi đồng thời đến email service, analytics service, notification service |
| **RPC bất đồng bộ** | Gửi request, không block thread, nhận kết quả qua reply queue |
| **Rate limiting** | Consumer chỉ xử lý N message/giây, RabbitMQ giữ phần còn lại trong queue |
| **Retry & Dead letter** | Message xử lý lỗi được đẩy sang DLX để retry hoặc audit |

---

## 3. Khi nào nên dùng RabbitMQ?

### ✅ Nên dùng khi:

- Cần **giao tiếp bất đồng bộ** giữa các service (fire-and-forget).
- Cần **đảm bảo message được xử lý ít nhất một lần** (at-least-once delivery).
- Cần **routing linh hoạt** — một message đến nhiều consumer khác nhau dựa trên rule.
- Xử lý **background jobs** (gửi email, resize ảnh, tạo báo cáo).
- Cần **load balancing** tự nhiên giữa nhiều worker instance.
- Hệ thống có **traffic spike** — queue làm buffer, tránh làm sập service downstream.

### ❌ Không nên dùng khi:

- Cần **replay message theo thời gian** (dùng Kafka thay thế).
- Cần **stream processing** với dữ liệu lớn (Kafka/Flink phù hợp hơn).
- Message cần được **lưu lâu dài** để nhiều consumer đọc lại nhiều lần (Kafka).
- Cần **ordered message theo partition** trên quy mô lớn.

---

## 4. Kiến trúc & Các khái niệm cốt lõi

```
                        ┌─────────────────────────────────────┐
                        │           RabbitMQ Broker            │
                        │                                      │
[Producer] ──publish──► │  [Exchange] ──binding──► [Queue]  │ ──deliver──► [Consumer]
                        │                                      │
                        └─────────────────────────────────────┘
```

### 4.1 Producer & Consumer

- **Producer**: Ứng dụng gửi message vào broker. Producer chỉ biết đến **Exchange**, không biết Queue nào sẽ nhận.
- **Consumer**: Ứng dụng lắng nghe và xử lý message từ **Queue**. Consumer không biết message đến từ đâu.

### 4.2 Queue

Queue là **nơi lưu trữ message** chờ được consumer xử lý.

```
Queue properties:
  - name        : tên queue (unique trong vhost)
  - durable     : true → tồn tại sau khi broker restart
  - exclusive   : true → chỉ connection hiện tại dùng được, tự xóa khi disconnect
  - auto-delete : true → tự xóa khi consumer cuối cùng unsubscribe
  - arguments   : metadata mở rộng (TTL, DLX, max-length, ...)
```

**Ví dụ khai báo queue durable:**
```javascript
// Tồn tại sau khi RabbitMQ restart
await channel.assertQueue('order.created', {
  durable: true,
  arguments: {
    'x-message-ttl': 86400000,  // message hết hạn sau 24h
    'x-max-length': 10000,      // tối đa 10,000 message
  }
});
```

### 4.3 Exchange

Exchange là **bộ định tuyến** — nhận message từ Producer và quyết định chuyển vào Queue nào dựa trên **type** và **routing key**.

> Exchange **không lưu message**. Nếu không có queue nào phù hợp, message bị **dropped** (hoặc trả lại nếu set `mandatory: true`).

### 4.4 Binding & Routing Key

- **Binding**: Liên kết giữa Exchange và Queue, có thể kèm theo **binding key**.
- **Routing Key**: Nhãn mà Producer gắn vào message khi publish. Exchange dùng nó để so khớp với binding key.

```
Producer publish:  exchange="orders", routing_key="order.created"
                         ↓
Exchange so khớp routing_key với các binding:
  - binding key "order.created" → Queue "inventory-queue"   ✅
  - binding key "order.shipped" → Queue "shipping-queue"    ❌ (không khớp)
```

### 4.5 Channel & Connection

- **Connection**: Một TCP connection đến RabbitMQ (tốn kém, nên tái sử dụng).
- **Channel**: Kênh ảo bên trong Connection (nhẹ, có thể tạo nhiều). Mỗi goroutine/thread nên dùng Channel riêng.

```javascript
// Best practice: 1 connection, nhiều channel
const connection = await amqp.connect('amqp://localhost');
const publishChannel  = await connection.createChannel(); // dùng để publish
const consumeChannel  = await connection.createChannel(); // dùng để consume
```

### 4.6 Virtual Host (vhost)

vhost giống như **namespace** — cô lập hoàn toàn exchanges, queues, bindings giữa các môi trường hoặc tenant.

```
amqp://user:pass@localhost:5672/production   ← vhost "production"
amqp://user:pass@localhost:5672/staging      ← vhost "staging"
```

### 4.7 Message & Properties

```javascript
channel.publish(exchange, routingKey, content, {
  persistent: true,         // lưu xuống disk (kết hợp với durable queue)
  contentType: 'application/json',
  messageId: uuid(),        // ID duy nhất, dùng để deduplicate
  timestamp: Date.now(),
  expiration: '60000',      // TTL riêng cho message này (ms, dạng string)
  headers: {
    'x-retry-count': 0
  }
});
```

---

## 5. Các loại Exchange

### 5.1 Direct Exchange

Định tuyến dựa trên **exact match** của routing key.

```
routing_key = "order.created"
                    ↓
Exchange (direct)
  ├── binding "order.created"  → Queue A  ✅
  └── binding "order.shipped"  → Queue B  ❌
```

**Dùng khi:** Muốn gửi message đến đúng một queue cụ thể.

```javascript
await channel.assertExchange('direct_orders', 'direct', { durable: true });
await channel.bindQueue('inventory-queue', 'direct_orders', 'order.created');

// Publish
channel.publish('direct_orders', 'order.created', Buffer.from(JSON.stringify(payload)));
```

---

### 5.2 Fanout Exchange

**Broadcast** — gửi message đến **tất cả** queue đang bind, bất kể routing key.

```
Exchange (fanout)
  ├── Queue A (email-service)       ← nhận
  ├── Queue B (analytics-service)   ← nhận
  └── Queue C (audit-log)           ← nhận
```

**Dùng khi:** Một event cần thông báo cho nhiều service đồng thời (pub/sub).

```javascript
await channel.assertExchange('user.events', 'fanout', { durable: true });

// Mỗi consumer tạo queue riêng và bind vào exchange
const q = await channel.assertQueue('', { exclusive: true }); // queue tạm
await channel.bindQueue(q.queue, 'user.events', ''); // routing key bị bỏ qua

channel.consume(q.queue, (msg) => { /* xử lý */ });
```

---

### 5.3 Topic Exchange

Định tuyến theo **pattern với wildcard**:
- `*` — khớp đúng **một** từ
- `#` — khớp **không hoặc nhiều** từ

```
routing_key patterns:
  "order.created"         → khớp "order.*" và "order.#" và "#"
  "order.item.deleted"    → khớp "order.#" và "#" nhưng KHÔNG khớp "order.*"
```

```
Exchange (topic: "app.events")
  ├── binding "order.*"    → Queue order-service
  ├── binding "user.#"     → Queue user-service
  └── binding "#"          → Queue audit-log (nhận tất cả)
```

**Dùng khi:** Cần routing linh hoạt theo domain/category (phổ biến nhất trong microservices).

```javascript
await channel.assertExchange('app.events', 'topic', { durable: true });

// Order service chỉ quan tâm order events
await channel.bindQueue('order-queue', 'app.events', 'order.*');

// Audit log nhận tất cả
await channel.bindQueue('audit-queue', 'app.events', '#');

// Publish
channel.publish('app.events', 'order.created', Buffer.from(JSON.stringify(order)));
channel.publish('app.events', 'order.item.removed', Buffer.from(JSON.stringify(item)));
```

---

### 5.4 Headers Exchange

Định tuyến dựa trên **headers** của message thay vì routing key.

```javascript
await channel.bindQueue('queue-A', 'headers_exchange', '', {
  'x-match': 'all',    // 'all' = AND, 'any' = OR
  format: 'pdf',
  type: 'report'
});

// Message này sẽ đến Queue A
channel.publish('headers_exchange', '', content, {
  headers: { format: 'pdf', type: 'report' }
});
```

**Dùng khi:** Logic routing phức tạp không thể diễn đạt qua routing key đơn thuần.

---

## 6. Message Durability & Acknowledgement

### Durability — Không mất message khi restart

Để message không bị mất khi RabbitMQ restart, cần **cả hai** điều kiện:

```javascript
// 1. Queue phải durable
await channel.assertQueue('orders', { durable: true });

// 2. Message phải persistent
channel.sendToQueue('orders', content, { persistent: true });
```

> ⚠️ `persistent: true` + `durable queue` → message được ghi xuống disk. Có overhead I/O nhưng đảm bảo không mất data.

---

### Acknowledgement — Đảm bảo message được xử lý

**3 chế độ ack:**

#### `ack` — Xử lý thành công, xóa message khỏi queue
```javascript
channel.consume('orders', (msg) => {
  try {
    processOrder(JSON.parse(msg.content));
    channel.ack(msg); // ✅ xử lý xong, xóa khỏi queue
  } catch (err) {
    channel.nack(msg, false, true); // ❌ thất bại, requeue = true
  }
});
```

#### `nack` với requeue
```javascript
channel.nack(msg,
  false,   // multiple: false = chỉ nack message này
  true     // requeue: true = đưa lại vào queue để thử lại
);
```

#### `nack` không requeue → đẩy sang Dead Letter Queue
```javascript
channel.nack(msg, false, false); // requeue = false → vào DLX nếu được cấu hình
```

#### `noAck` mode — Auto-ack (không đảm bảo)
```javascript
channel.consume('queue', handler, { noAck: true }); // nguy hiểm, dễ mất message
```

---

## 7. Dead Letter Exchange (DLX)

DLX là nơi nhận các message **bị từ chối** hoặc **hết TTL**, để xử lý riêng (retry, alert, audit).

### Các trường hợp message vào DLX:
1. `nack` với `requeue = false`
2. `reject` với `requeue = false`
3. Message hết TTL (`x-message-ttl`)
4. Queue đầy (`x-max-length`)

### Cấu hình DLX:

```javascript
// 1. Tạo dead letter exchange
await channel.assertExchange('dlx.orders', 'direct', { durable: true });

// 2. Tạo dead letter queue
await channel.assertQueue('dlq.orders', { durable: true });
await channel.bindQueue('dlq.orders', 'dlx.orders', 'order.failed');

// 3. Gắn DLX vào main queue
await channel.assertQueue('orders', {
  durable: true,
  arguments: {
    'x-dead-letter-exchange': 'dlx.orders',
    'x-dead-letter-routing-key': 'order.failed',
    'x-message-ttl': 30000,   // message tự vào DLX sau 30s nếu chưa được xử lý
  }
});
```

### Retry pattern với DLX:

```
[Main Queue] → xử lý lỗi → nack(requeue=false)
                                  ↓
                           [DLX Exchange]
                                  ↓
                           [Retry Queue] (TTL = 5s)
                                  ↓  (sau 5s, message hết hạn)
                           [DLX của Retry Queue] = Main Queue
                                  ↓
                           [Main Queue] ← thử lại
```

```javascript
// Retry queue: chờ 5s rồi tự đưa message về main queue
await channel.assertQueue('orders.retry', {
  durable: true,
  arguments: {
    'x-dead-letter-exchange': '',           // default exchange
    'x-dead-letter-routing-key': 'orders', // routing về main queue
    'x-message-ttl': 5000,                 // chờ 5s
  }
});
```

---

## 8. Prefetch & QoS

**Prefetch** kiểm soát số message RabbitMQ đẩy cho consumer trước khi nhận được ack.

```javascript
// Consumer chỉ nhận tối đa 1 message tại một thời điểm
await channel.prefetch(1);
```

### Tại sao quan trọng?

```
Không có prefetch:
  Consumer A ← nhận 100 message (đang xử lý dở, chậm)
  Consumer B ← idle, không có việc

Với prefetch(1):
  Consumer A ← nhận 1 message, xử lý xong mới nhận tiếp
  Consumer B ← nhận 1 message song song
  → Load được phân phối đều
```

### Chọn prefetch bao nhiêu?

| Trường hợp | Prefetch khuyến nghị |
|---|---|
| Task nặng, xử lý lâu (I/O, DB) | `1` — tránh một worker ôm hết |
| Task nhẹ, xử lý nhanh | `10–100` — tăng throughput |
| Real-time, latency quan trọng | `1–5` |

---

## 9. RabbitMQ vs Kafka — so sánh thực tế

| Tiêu chí | RabbitMQ | Kafka |
|---|---|---|
| **Mô hình** | Message broker (push) | Event log (pull) |
| **Lưu trữ** | Message xóa sau khi ack | Message giữ theo retention (ngày/GB) |
| **Replay** | ❌ Không hỗ trợ natively | ✅ Consumer đọc lại từ offset bất kỳ |
| **Ordering** | Per-queue (FIFO) | Per-partition |
| **Routing** | Rất linh hoạt (Exchange types) | Chỉ theo topic/partition |
| **Throughput** | Vừa (~50K msg/s/node) | Rất cao (~1M msg/s/node) |
| **Latency** | Thấp (< 1ms) | Thấp nhưng cao hơn RabbitMQ |
| **Use case** | Task queue, RPC, complex routing | Stream processing, event sourcing, audit log |
| **Độ phức tạp** | Đơn giản hơn | Phức tạp hơn (cần Zookeeper/KRaft) |

> **Nguyên tắc chọn:** Nếu bạn cần xử lý **command/task** → RabbitMQ. Nếu bạn cần xử lý **event stream/log** → Kafka.

---

## 10. Ví dụ thực tế trong dự án microservice

### Bối cảnh: Order Service → Inventory Service

```
[Order Service]  --"order.created"-→  [RabbitMQ]  --→  [Inventory Service]
                                                    --→  [Notification Service]
                                                    --→  [Analytics Service]
```

### `config/rabbit.js` (dùng chung)

```javascript
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';

let connection = null;
let channel = null;

async function connect() {
  if (channel) return channel;

  connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();

  // Đảm bảo exchange tồn tại
  await channel.assertExchange('app.events', 'topic', { durable: true });

  // Xử lý lỗi kết nối
  connection.on('error', (err) => {
    console.error('RabbitMQ connection error:', err.message);
    channel = null;
    setTimeout(connect, 5000); // reconnect sau 5s
  });

  return channel;
}

module.exports = { connect };
```

### Order Service — Publish event

```javascript
const { connect } = require('./config/rabbit');

async function publishOrderCreated(order) {
  const ch = await connect();

  const payload = Buffer.from(JSON.stringify({
    orderId: order._id,
    userId: order.userId,
    items: order.items,
    totalAmount: order.totalAmount,
    createdAt: new Date().toISOString(),
  }));

  ch.publish(
    'app.events',
    'order.created',       // routing key
    payload,
    {
      persistent: true,
      messageId: order._id.toString(),
      contentType: 'application/json',
      timestamp: Date.now(),
    }
  );

  console.log(`[Order Service] Published order.created: ${order._id}`);
}
```

### Inventory Service — Consume event

```javascript
const { connect } = require('./config/rabbit');

async function startConsumer() {
  const ch = await connect();

  // Tạo queue durable với DLX
  await ch.assertQueue('inventory.order.created', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'dlx.inventory',
      'x-dead-letter-routing-key': 'inventory.failed',
      'x-message-ttl': 300000, // 5 phút
    }
  });

  // Bind queue với exchange theo pattern
  await ch.bindQueue('inventory.order.created', 'app.events', 'order.created');

  // Prefetch = 1: xử lý tuần tự, tránh race condition khi trừ kho
  await ch.prefetch(1);

  ch.consume('inventory.order.created', async (msg) => {
    if (!msg) return;

    try {
      const order = JSON.parse(msg.content.toString());
      console.log(`[Inventory] Processing order: ${order.orderId}`);

      await deductInventory(order.items);

      ch.ack(msg); // ✅ thành công
      console.log(`[Inventory] Order processed: ${order.orderId}`);
    } catch (err) {
      console.error(`[Inventory] Failed to process order:`, err.message);

      // Lấy retry count từ header
      const retryCount = (msg.properties.headers['x-retry-count'] || 0);

      if (retryCount < 3) {
        // Gửi lại với retry count tăng (không requeue trực tiếp)
        ch.nack(msg, false, false); // vào DLX
      } else {
        console.error(`[Inventory] Max retries reached, discarding message`);
        ch.ack(msg); // Bỏ qua sau 3 lần thất bại
      }
    }
  });

  console.log('[Inventory Service] Waiting for order events...');
}
```

---

## 11. Các pattern phổ biến

### Pattern 1: Work Queue (Task Distribution)

Phân phối task đều cho nhiều worker — mỗi task chỉ được xử lý **bởi một worker**.

```
[Producer]
     ↓ publish
  [Queue]
  ↓     ↓     ↓
[W1]  [W2]  [W3]    ← mỗi worker nhận phần task khác nhau
```

```javascript
// 3 worker cùng consume một queue
// RabbitMQ tự round-robin (kết hợp prefetch để cân bằng tải thực sự)
channel.prefetch(1);
channel.consume('task.queue', handler);
```

---

### Pattern 2: Pub/Sub với Fanout

Một event được xử lý bởi **tất cả** subscriber.

```
[Producer] → [fanout exchange] → [Queue Email]    → Email Service
                              → [Queue Analytics] → Analytics Service
                              → [Queue Audit]     → Audit Service
```

---

### Pattern 3: RPC (Remote Procedure Call)

Gửi request và chờ response qua RabbitMQ, hữu ích khi cần kết quả nhưng muốn loose coupling.

```javascript
// Client (caller)
async function rpcCall(payload) {
  const ch = await connect();
  const replyQueue = await ch.assertQueue('', { exclusive: true });

  const correlationId = uuid();

  return new Promise((resolve) => {
    ch.consume(replyQueue.queue, (msg) => {
      if (msg.properties.correlationId === correlationId) {
        resolve(JSON.parse(msg.content.toString()));
      }
    }, { noAck: true });

    ch.sendToQueue('rpc.pricing', Buffer.from(JSON.stringify(payload)), {
      correlationId,
      replyTo: replyQueue.queue,
    });
  });
}

// Server (handler)
ch.consume('rpc.pricing', async (msg) => {
  const request = JSON.parse(msg.content.toString());
  const result = await calculatePrice(request);

  ch.sendToQueue(
    msg.properties.replyTo,
    Buffer.from(JSON.stringify(result)),
    { correlationId: msg.properties.correlationId }
  );
  ch.ack(msg);
});
```

---

### Pattern 4: Priority Queue

Message quan trọng được xử lý trước.

```javascript
await channel.assertQueue('tasks', {
  durable: true,
  arguments: { 'x-max-priority': 10 } // priority từ 0-10
});

// Publish message với priority cao
channel.sendToQueue('tasks', content, { priority: 8 });
channel.sendToQueue('tasks', content, { priority: 1 });
```

---

## 12. Lưu ý vận hành

### Connection Management
```javascript
// ❌ Sai: tạo connection mỗi lần publish
async function badPublish(msg) {
  const conn = await amqp.connect(URL); // tốn kém!
  const ch = await conn.createChannel();
  ch.sendToQueue(...);
  await conn.close(); // overhead
}

// ✅ Đúng: tái sử dụng connection & channel
const channel = await getSharedChannel();
channel.sendToQueue(...);
```

### Graceful Shutdown
```javascript
process.on('SIGTERM', async () => {
  console.log('Shutting down gracefully...');
  await channel.close();
  await connection.close();
  process.exit(0);
});
```

### Monitoring
- **RabbitMQ Management UI**: `http://localhost:15672` (guest/guest)
- Theo dõi: queue depth, consumer count, message rate, memory usage
- Alert khi: queue depth > threshold, consumer = 0, memory > 80%

### Checklist production
- [ ] Exchange và Queue đều `durable: true`
- [ ] Message `persistent: true` cho critical data
- [ ] Luôn `ack`/`nack` sau khi xử lý
- [ ] Đặt `prefetch` phù hợp
- [ ] Cấu hình DLX cho mọi queue quan trọng
- [ ] Đặt `x-message-ttl` và `x-max-length` tránh queue tràn bộ nhớ
- [ ] Implement reconnect logic
- [ ] Monitor queue depth qua Management UI hoặc Prometheus exporter
