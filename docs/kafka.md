# Apache Kafka — Hướng dẫn toàn diện

---

## Mục lục

1. [Kafka là gì?](#1-kafka-là-gì)
2. [Dùng làm gì?](#2-dùng-làm-gì)
3. [Khi nào nên dùng Kafka?](#3-khi-nào-nên-dùng-kafka)
4. [Kiến trúc & Các khái niệm cốt lõi](#4-kiến-trúc--các-khái-niệm-cốt-lõi)
   - [Producer](#41-producer)
   - [Consumer & Consumer Group](#42-consumer--consumer-group)
   - [Topic](#43-topic)
   - [Partition](#44-partition)
   - [Offset](#45-offset)
   - [Broker & Cluster](#46-broker--cluster)
   - [Replication & Leader/Follower](#47-replication--leaderfollower)
   - [ZooKeeper & KRaft](#48-zookeeper--kraft)
5. [Cơ chế lưu trữ — Commit Log](#5-cơ-chế-lưu-trữ--commit-log)
6. [Delivery Semantics](#6-delivery-semantics)
7. [Producer — Cấu hình chi tiết](#7-producer--cấu-hình-chi-tiết)
8. [Consumer — Cấu hình chi tiết](#8-consumer--cấu-hình-chi-tiết)
9. [Kafka vs RabbitMQ — so sánh thực tế](#9-kafka-vs-rabbitmq--so-sánh-thực-tế)
10. [Ví dụ thực tế trong dự án microservice](#10-ví-dụ-thực-tế-trong-dự-án-microservice)
11. [Các pattern phổ biến](#11-các-pattern-phổ-biến)
12. [Lưu ý vận hành](#12-lưu-ý-vận-hành)
13. [Những điều hay bị hiểu nhầm về Kafka](#13-những-điều-hay-bị-hiểu-nhầm-về-kafka)
    - [Kafka là nơi lưu trữ lâu dài — SAI](#131-kafka-là-nơi-lưu-trữ-lâu-dài--sai)
    - [Kafka chỉ để log — CHƯA ĐỦ](#132-kafka-chỉ-để-log--chưa-đủ)
    - [Event Sourcing — tại sao bank bắt buộc dùng](#133-event-sourcing--tại-sao-bank-bắt-buộc-dùng)
    - [Choreography-based SAGA](#134-choreography-based-saga--nhiều-service-phối-hợp-qua-kafka)
    - [Ai lưu gì, bao lâu, mục đích gì](#135-ai-lưu-gì-bao-lâu-mục-đích-gì)

---

## 1. Kafka là gì?

**Apache Kafka** là một **distributed event streaming platform** — không chỉ là message queue mà là một **hệ thống lưu trữ và xử lý luồng sự kiện** theo thời gian thực.

Hãy hình dung Kafka như một **nhật ký ghi chép bất biến (append-only log)**:

```
[Producer A] ──┐
[Producer B] ──┼──► [Kafka Topic: user-behavior-logs] ──► [Consumer Group A: Analytics]
[Producer C] ──┘                                       ──► [Consumer Group B: ML Pipeline]
                                                        ──► [Consumer Group C: Audit]
```

- **Mỗi Consumer Group** đọc **độc lập** — không ảnh hưởng nhau.
- **Message không bị xóa sau khi đọc** — được giữ theo thời gian cấu hình.
- **Consumer có thể đọc lại từ bất kỳ điểm nào** trong lịch sử.

> **Điểm đặc biệt:** Kafka được thiết kế bởi LinkedIn để xử lý **hàng tỷ event/ngày**. Throughput cực cao đạt được nhờ ghi tuần tự xuống disk (sequential I/O) + zero-copy transfer + batching — điều này ngược lại với trực giác: **disk đôi khi nhanh hơn memory** khi dùng đúng cách.

---

## 2. Dùng làm gì?

| Use case | Ví dụ cụ thể |
|---|---|
| **Event streaming** | Ghi lại mọi hành vi user (click, view, purchase) để phân tích real-time |
| **Event sourcing** | Lưu toàn bộ lịch sử thay đổi trạng thái hệ thống thay vì chỉ lưu state cuối |
| **CQRS** | Tách luồng write (command) và read (query) qua Kafka |
| **Data pipeline** | Đưa data từ DB → Data Warehouse / Data Lake (Kafka Connect) |
| **Stream processing** | Aggregate, join, filter data real-time (Kafka Streams / Flink) |
| **Log aggregation** | Thu thập log từ nhiều service vào một nơi |
| **Microservice decoupling** | Service publish event, nhiều service downstream subscribe độc lập |
| **Metrics & Monitoring** | Gửi metrics hệ thống để xử lý và alert real-time |

---

## 3. Khi nào nên dùng Kafka?

### ✅ Nên dùng khi:

- Cần **throughput cực cao** (hàng trăm nghìn đến triệu message/giây).
- Cần **replay message** — đọc lại lịch sử event từ một thời điểm cụ thể.
- Nhiều **Consumer Group độc lập** cùng tiêu thụ một stream.
- Cần **audit log bất biến** — lịch sử không thể sửa/xóa.
- Xây dựng **event sourcing** hoặc **CQRS**.
- Cần **stream processing** — tính toán real-time trên luồng dữ liệu.
- Data pipeline: đồng bộ dữ liệu giữa nhiều hệ thống.

### ❌ Không nên dùng khi:

- Cần **routing linh hoạt** theo nhiều tiêu chí (dùng RabbitMQ).
- Message có **TTL ngắn** và cần xóa ngay sau xử lý.
- Cần **RPC pattern** hoặc request-reply đơn giản.
- Hệ thống nhỏ, không cần throughput cao — Kafka có overhead vận hành lớn.
- Cần **priority queue** — Kafka không hỗ trợ.

---

## 4. Kiến trúc & Các khái niệm cốt lõi

```
                    Kafka Cluster
┌──────────────────────────────────────────────────────┐
│                                                      │
│  Topic: "user-behavior-logs"                         │
│  ┌─────────────────────────────────────────────┐    │
│  │ Partition 0: [msg0][msg1][msg2][msg3]...    │    │
│  │ Partition 1: [msg0][msg1][msg2]...          │    │
│  │ Partition 2: [msg0][msg1][msg2][msg3][msg4] │    │
│  └─────────────────────────────────────────────┘    │
│                                                      │
│  Broker 0 (Leader P0, Follower P1)                   │
│  Broker 1 (Leader P1, Follower P2)                   │
│  Broker 2 (Leader P2, Follower P0)                   │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 4.1 Producer

Producer **publish message vào Topic**, có thể chỉ định Partition cụ thể hoặc để Kafka tự phân phối.

**Cách Kafka chọn Partition:**
1. Nếu **chỉ định key** → hash(key) % numPartitions → message cùng key luôn vào cùng partition (đảm bảo ordering cho key đó).
2. Nếu **không có key** → round-robin hoặc sticky partitioner (batch các message vào một partition để tối ưu throughput).

```javascript
// Message có key → cùng productId luôn vào cùng partition
await producer.send({
  topic: 'user-behavior-logs',
  messages: [{
    key: 'user-123',          // ← đảm bảo ordering cho user-123
    value: JSON.stringify({ action: 'purchase' }),
  }]
});
```

### 4.2 Consumer & Consumer Group

- **Consumer**: Ứng dụng đọc message từ một hoặc nhiều Partition.
- **Consumer Group**: Nhóm các consumer cùng `groupId` — mỗi Partition chỉ được đọc bởi **một consumer trong group tại một thời điểm**.

```
Topic: 3 partitions
Consumer Group "analytics" có 3 consumers:

  Partition 0 ──► Consumer A
  Partition 1 ──► Consumer B
  Partition 2 ──► Consumer C
  → Mỗi consumer xử lý 1 partition, song song hoàn toàn

Consumer Group "ml-pipeline" có 2 consumers:
  Partition 0 ──► Consumer X
  Partition 1 ──► Consumer X  (Consumer X xử lý 2 partitions)
  Partition 2 ──► Consumer Y
```

> **Quy tắc vàng:** Số consumer trong group **không vượt quá số partition**. Consumer dư sẽ **idle**, không nhận được message.

**Rebalancing** xảy ra khi consumer join/leave group → Kafka tự phân phối lại partition. Trong thời gian rebalance, consumer group tạm dừng xử lý.

### 4.3 Topic

Topic là **kênh phân loại message**, tương tự như tên bảng trong database.

```
Topics trong một hệ thống e-commerce:
  ├── user-behavior-logs    ← hành vi user
  ├── order-events          ← các sự kiện đơn hàng
  ├── inventory-updates     ← cập nhật kho
  └── payment-events        ← sự kiện thanh toán
```

**Topic configuration:**
```
Topic: "order-events"
  - partitions: 6          (số partition)
  - replication-factor: 3  (mỗi partition có 3 bản sao)
  - retention.ms: 604800000 (giữ data 7 ngày)
  - retention.bytes: -1    (-1 = không giới hạn theo dung lượng)
  - cleanup.policy: delete  (xóa message cũ khi hết retention)
```

### 4.4 Partition

Partition là **đơn vị song song hóa** và **đơn vị ordering** của Kafka.

```
Topic "order-events" với 3 partitions:

P0: [order#1][order#4][order#7]...  ← ordering đảm bảo trong P0
P1: [order#2][order#5][order#8]...  ← ordering đảm bảo trong P1
P2: [order#3][order#6][order#9]...  ← ordering đảm bảo trong P2
```

**Chọn số partition:**
- Càng nhiều partition → throughput cao hơn (nhiều consumer song song hơn).
- Nhưng: overhead metadata tăng, rebalance chậm hơn, file descriptor nhiều hơn.
- **Thực tế:** bắt đầu với `partitions = max_consumers_expected * 2`.

> ⚠️ **Không thể giảm số partition** sau khi tạo. Tăng được nhưng sẽ phá vỡ ordering theo key.

### 4.5 Offset

Offset là **vị trí (số thứ tự) của message trong một Partition** — bắt đầu từ 0, tăng dần, **bất biến**.

```
Partition 0:
  offset: [0]    [1]    [2]    [3]    [4]    [5]
  msg:  [msg_a][msg_b][msg_c][msg_d][msg_e][msg_f]
                              ↑
                   Consumer đang ở offset 3
                   (đã xử lý msg_a, msg_b, msg_c)
```

**Consumer tự quản lý offset:**
```javascript
// Auto commit (dễ dùng nhưng có thể mất message)
{ autoCommit: true, autoCommitInterval: 5000 }

// Manual commit (kiểm soát chính xác hơn)
await consumer.commitOffsets([{
  topic: 'order-events',
  partition: 0,
  offset: (currentOffset + 1).toString() // commit offset TIẾP THEO
}]);
```

### 4.6 Broker & Cluster

- **Broker**: Một Kafka server node — lưu trữ partition data, phục vụ producer/consumer.
- **Cluster**: Nhiều broker cùng hoạt động → fault tolerance + horizontal scaling.

```
Cluster 3 brokers:
  Broker 0: leader của P0, P3 | follower của P1, P4
  Broker 1: leader của P1, P4 | follower của P2, P5
  Broker 2: leader của P2, P5 | follower của P0, P3
```

**Bootstrap servers:** Producer/Consumer chỉ cần biết một vài broker để khởi tạo — Kafka tự cung cấp metadata về toàn bộ cluster.

### 4.7 Replication & Leader/Follower

- Mỗi partition có **một Leader** và **(replication-factor - 1) Follower**.
- **Producer và Consumer** chỉ giao tiếp với **Leader**.
- **Follower** liên tục sync data từ Leader — sẵn sàng lên làm Leader khi Leader chết.

```
Partition 0:
  Broker 0: [Leader]   ← producer ghi vào đây, consumer đọc từ đây
  Broker 1: [Follower] ← sync từ Leader
  Broker 2: [Follower] ← sync từ Leader

→ Broker 0 chết → Broker 1 hoặc 2 tự động lên làm Leader
```

**ISR (In-Sync Replicas):** Tập hợp các replica đang sync kịp với Leader. Chỉ khi message được ghi vào đủ số ISR mới được coi là "committed".

### 4.8 ZooKeeper & KRaft

- **ZooKeeper (cũ):** Quản lý metadata cluster, leader election. Phức tạp khi vận hành, cần deploy riêng.
- **KRaft (Kafka 3.3+, mặc định từ 3.7):** Kafka tự quản lý metadata không cần ZooKeeper — đơn giản hóa vận hành đáng kể.

```yaml
# docker-compose.yml với KRaft (không cần ZooKeeper)
kafka:
  image: confluentinc/cp-kafka:7.6.0
  environment:
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_NODE_ID: 1
    KAFKA_KRAFT_MODE: "true"
```

---

## 5. Cơ chế lưu trữ — Commit Log

Đây là điểm **khác biệt cốt lõi** của Kafka so với traditional message queue.

```
Traditional Queue (RabbitMQ):          Kafka Commit Log:
  Enqueue → [A][B][C][D]               [A][B][C][D][E][F]...
  Consume → [B][C][D]   (A bị xóa)        ↑     ↑
  Consume → [C][D]      (B bị xóa)    Consumer1  Consumer2
                                       offset=0   offset=3
                                      (đọc độc lập, A vẫn còn)
```

**Hệ quả:**
- **Replay**: Consumer có thể seek về `offset=0` để đọc lại toàn bộ lịch sử.
- **Multiple consumers**: 10 consumer group đọc cùng một topic, không ảnh hưởng nhau.
- **Time-travel**: `consumer.seek({ topic, partition, offset })` — đọc lại data từ 2 ngày trước.
- **Data không mất** khi consumer chết — chỉ cần resume từ offset đã commit.

**Retention policy:**
```
cleanup.policy=delete   → xóa message sau X ngày / khi vượt X bytes
cleanup.policy=compact  → chỉ giữ message mới nhất cho mỗi key (dùng cho state store)
```

---

## 6. Delivery Semantics

### At-most-once (tối đa một lần)
Message có thể **bị mất**, không bao giờ duplicate. Nhanh nhất.

```javascript
// Producer: không retry
{ retries: 0 }

// Consumer: commit offset TRƯỚC khi xử lý
await consumer.commitOffsets([...]);
processMessage(msg); // nếu crash ở đây → mất message
```

### At-least-once (ít nhất một lần) ← mặc định phổ biến nhất
Message **không bị mất**, nhưng có thể **duplicate**. Consumer phải idempotent.

```javascript
// Producer: có retry
{ retries: 5, acks: 'all' }

// Consumer: commit offset SAU khi xử lý
processMessage(msg);
await consumer.commitOffsets([...]); // nếu crash ở đây → message được xử lý lại
```

> **Idempotent consumer:** Xử lý cùng một message nhiều lần cho kết quả giống nhau. Thường dùng `messageId` để deduplicate.

### Exactly-once (đúng một lần) ← khó nhất
Message **không mất, không duplicate**. Yêu cầu **Kafka Transactions**.

```javascript
// Producer với transactional ID
const producer = kafka.producer({
  transactionalId: 'order-service-producer',
  idempotent: true,          // tự động bật khi dùng transactions
  maxInFlightRequests: 1,
});

await producer.transaction(async (tx) => {
  await tx.send({ topic: 'order-events', messages: [...] });
  await tx.sendOffsets({     // commit offset cùng transaction
    consumerGroupId: 'my-group',
    topics: [{ topic: 'input-topic', partitions: [...] }]
  });
});
```

---

## 7. Producer — Cấu hình chi tiết

```javascript
const producer = kafka.producer({
  // Reliability
  acks: 'all',          // -1/'all' = đợi tất cả ISR xác nhận (an toàn nhất)
                        //  0       = không đợi ack (nhanh nhất, có thể mất)
                        //  1       = đợi leader xác nhận

  // Retry
  retries: 5,
  retry: {
    initialRetryTime: 100,  // ms
    retryFactor: 2,          // exponential backoff
    maxRetryTime: 30000,
  },

  // Performance
  maxInFlightRequests: 5,  // số request đang bay song song (set 1 nếu cần strict ordering)

  // Batching (tối ưu throughput)
  // lingerMs: 10,        // chờ 10ms để gom nhiều message vào 1 batch
  // batchSize: 16384,    // kích thước batch tối đa (bytes)

  // Compression
  compression: CompressionTypes.GZIP,  // hoặc SNAPPY, LZ4, ZSTD
});
```

**Acks giải thích:**

```
acks=0:  Producer ──► Broker     (không đợi, có thể mất nếu broker crash)
acks=1:  Producer ──► Leader     (đợi leader ghi xong, follower chưa sync → có thể mất)
acks=-1: Producer ──► Leader + tất cả ISR  (an toàn nhất, chậm nhất)
```

---

## 8. Consumer — Cấu hình chi tiết

```javascript
const consumer = kafka.consumer({
  groupId: 'analytics-service',

  // Session & Heartbeat
  sessionTimeout: 30000,        // consumer bị coi là chết nếu không heartbeat trong 30s
  heartbeatInterval: 3000,      // gửi heartbeat mỗi 3s

  // Fetch behavior
  maxWaitTimeInMs: 5000,        // đợi tối đa 5s nếu không có message
  minBytes: 1,                  // fetch khi có ít nhất 1 byte
  maxBytes: 10485760,           // tối đa 10MB mỗi fetch

  // Rebalance
  rebalanceTimeout: 60000,
});

await consumer.subscribe({
  topics: ['user-behavior-logs'],
  fromBeginning: false,   // true = đọc từ đầu, false = đọc từ offset mới nhất
});

await consumer.run({
  autoCommit: false,      // tắt auto-commit để kiểm soát thủ công
  eachMessage: async ({ topic, partition, message, heartbeat }) => {
    // heartbeat() nên được gọi trong long-running tasks
    // để tránh session timeout
  },
});
```

**Manual offset commit:**
```javascript
eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
  for (const message of batch.messages) {
    try {
      await processMessage(message);
      resolveOffset(message.offset);   // đánh dấu offset đã xử lý
      await heartbeat();               // tránh timeout khi batch lớn
    } catch (err) {
      // không resolve offset → message sẽ được đọc lại sau khi restart
      break;
    }
  }
  await commitOffsetsIfNecessary();   // commit tất cả resolved offsets
}
```

---

## 9. Kafka vs RabbitMQ — so sánh thực tế

| Tiêu chí | Apache Kafka | RabbitMQ |
|---|---|---|
| **Mô hình** | Distributed event log (pull) | Message broker (push) |
| **Lưu trữ message** | Giữ theo retention (ngày/GB) | Xóa sau khi ack |
| **Replay** | ✅ Đọc lại từ offset bất kỳ | ❌ Không hỗ trợ natively |
| **Multiple consumers** | ✅ Nhiều group đọc độc lập | ❌ Message chỉ đến một consumer |
| **Throughput** | Rất cao (~1M msg/s/node) | Vừa (~50K msg/s/node) |
| **Latency** | ~5-15ms | < 1ms |
| **Ordering** | Đảm bảo trong partition | Đảm bảo trong queue |
| **Routing** | Chỉ theo topic/partition | Rất linh hoạt (4 loại exchange) |
| **Priority queue** | ❌ Không hỗ trợ | ✅ Hỗ trợ |
| **RPC pattern** | ❌ Khó | ✅ Dễ |
| **Vận hành** | Phức tạp hơn | Đơn giản hơn |
| **Use case chính** | Event streaming, audit log, data pipeline | Task queue, RPC, complex routing |

**Nguyên tắc chọn:**
- Cần xử lý **command/task** → **RabbitMQ**
- Cần xử lý **event stream/log/replay** → **Kafka**
- Trong dự án này: RabbitMQ cho `order.created` (task), Kafka cho `user-behavior-logs` (stream analytics)

---

## 10. Ví dụ thực tế trong dự án microservice

### Bối cảnh: Order Service publish hành vi user lên Kafka

```
[Order Service] ──"purchase"──► [Kafka: user-behavior-logs]
                                         │
                         ┌───────────────┼───────────────┐
                         ▼               ▼               ▼
               [Analytics Service] [ML Pipeline]  [Audit Service]
               (Consumer Group A)  (Consumer Group B) (Consumer Group C)
```

### `config/kafka.js`

```javascript
const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  retry: {
    initialRetryTime: 100,
    retries: 8,
  },
  // Tắt log mặc định của KafkaJS trong production
  logLevel: process.env.NODE_ENV === 'production' ? 1 : 2, // 1=ERROR, 2=WARN
});

let producer = null;

async function connectProducer() {
  if (producer) return producer;

  producer = kafka.producer({
    acks: 'all',
    retries: 5,
    compression: CompressionTypes.GZIP,
  });

  await producer.connect();

  producer.on('producer.disconnect', () => {
    console.error('[Kafka] Producer disconnected, reconnecting...');
    producer = null;
    setTimeout(connectProducer, 5000);
  });

  console.log('[Kafka] Producer connected');
  return producer;
}

function getKafkaProducer() {
  return producer;
}

module.exports = { connectProducer, getKafkaProducer };
```

### Order Service — Publish event stream

```javascript
const { getKafkaProducer } = require('./config/kafka');
const { v4: uuidv4 } = require('uuid');

async function publishUserBehavior({ action, productId, quantity, traceId }) {
  const kafkaProducer = getKafkaProducer();
  if (!kafkaProducer) {
    logger.warn({ trace_id: traceId, message: '[Kafka] Producer chưa sẵn sàng, bỏ qua' });
    return;
  }

  await kafkaProducer.send({
    topic: 'user-behavior-logs',
    messages: [{
      key: productId,              // cùng productId → cùng partition → ordering đảm bảo
      value: JSON.stringify({
        messageId: uuidv4(),       // dùng để deduplicate ở consumer
        action,
        productId,
        quantity,
        traceId,
        timestamp: new Date().toISOString(),
      }),
      headers: {
        'x-trace-id': traceId,
        'x-source-service': 'order-service',
      },
    }],
  });

  logger.info({ trace_id: traceId, message: `[Kafka Stream] Đã publish: ${action} cho ${productId}` });
}
```

### Analytics Service — Consume event stream

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'analytics-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

async function startAnalyticsConsumer() {
  const consumer = kafka.consumer({
    groupId: 'analytics-service-group',
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });

  await consumer.connect();
  await consumer.subscribe({ topic: 'user-behavior-logs', fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const traceId = message.headers['x-trace-id']?.toString();

      try {
        const event = JSON.parse(message.value.toString());

        // Deduplicate bằng messageId
        const alreadyProcessed = await checkIfProcessed(event.messageId);
        if (alreadyProcessed) {
          console.log(`[Analytics] Duplicate message, skipping: ${event.messageId}`);
        } else {
          await saveToAnalytics(event);
          await markAsProcessed(event.messageId);
        }

        // Manual commit sau khi xử lý thành công
        await consumer.commitOffsets([{
          topic,
          partition,
          offset: (BigInt(message.offset) + 1n).toString(),
        }]);

      } catch (err) {
        console.error(`[Analytics] Lỗi xử lý message:`, err.message);
        // Không commit offset → message sẽ được đọc lại sau khi restart
        // Cần cẩn thận: nếu message luôn lỗi → infinite loop
        // Cân nhắc Dead Letter Topic
      }
    },
  });

  console.log('[Analytics Service] Consuming user-behavior-logs...');
}
```

---

## 11. Các pattern phổ biến

### Pattern 1: Event Sourcing

Lưu **tất cả sự kiện** thay vì chỉ lưu state hiện tại. State có thể tái tạo bằng cách replay events.

```
Thay vì: DB { orderId: 1, status: "shipped" }

Event log:
  [order.created]  → { orderId: 1, items: [...] }
  [order.paid]     → { orderId: 1, amount: 500 }
  [order.shipped]  → { orderId: 1, trackingCode: "ABC" }
  → Replay toàn bộ = tái tạo state bất kỳ thời điểm
```

```javascript
// Publish mọi state change
await producer.send({
  topic: 'order-events',
  messages: [{
    key: orderId,          // cùng orderId → cùng partition → ordering đảm bảo
    value: JSON.stringify({
      type: 'order.shipped',
      aggregateId: orderId,
      version: 3,          // version tăng dần → detect conflict
      payload: { trackingCode: 'ABC123' },
      timestamp: Date.now(),
    }),
  }],
});
```

---

### Pattern 2: CQRS với Kafka

Tách luồng **write** (Command) và **read** (Query) — Kafka là backbone trung gian.

```
[Write API] ──command──► [Command Handler] ──event──► [Kafka]
                                                           │
                                        ┌──────────────────┘
                                        ▼
                               [Projection Builder] ──► [Read DB (MongoDB/Redis)]
                                        ▼
                               [Query API] ◄── [Client]
```

---

### Pattern 3: Dead Letter Topic

Xử lý message lỗi sau nhiều lần thất bại — tương tự DLX của RabbitMQ.

```javascript
// Consumer chính
eachMessage: async ({ message }) => {
  const retryCount = parseInt(message.headers['x-retry-count'] || '0');

  try {
    await processMessage(message);
  } catch (err) {
    if (retryCount < 3) {
      // Publish lại với retry count tăng
      await producer.send({
        topic: 'order-events.retry',
        messages: [{
          ...message,
          headers: {
            ...message.headers,
            'x-retry-count': (retryCount + 1).toString(),
            'x-original-topic': 'order-events',
            'x-error': err.message,
          },
        }],
      });
    } else {
      // Đưa vào Dead Letter Topic
      await producer.send({
        topic: 'order-events.dlq',
        messages: [message],
      });
    }
  }
}
```

---

### Pattern 4: Kafka Streams (Aggregation real-time)

Đếm số lần purchase theo productId trong cửa sổ 5 phút.

```javascript
// Dùng kafkajs với state store thủ công (hoặc dùng Kafka Streams Java API)
const purchaseCount = new Map(); // productId → count

eachMessage: async ({ message }) => {
  const event = JSON.parse(message.value.toString());

  if (event.action === 'purchase') {
    const count = (purchaseCount.get(event.productId) || 0) + 1;
    purchaseCount.set(event.productId, count);

    // Publish aggregated result mỗi N messages
    if (count % 100 === 0) {
      await producer.send({
        topic: 'product-purchase-aggregates',
        messages: [{
          key: event.productId,
          value: JSON.stringify({ productId: event.productId, count }),
        }],
      });
    }
  }
}
```

---

### Pattern 5: Compacted Topic (State Store)

Kafka giữ **chỉ message mới nhất cho mỗi key** — hoạt động như distributed key-value store.

```
Topic "product-inventory" (cleanup.policy=compact):

  Offset 0: key=P001, value={ stock: 100 }
  Offset 1: key=P002, value={ stock: 50 }
  Offset 2: key=P001, value={ stock: 95 }   ← offset 0 sẽ bị xóa
  Offset 3: key=P001, value={ stock: 90 }   ← offset 2 sẽ bị xóa

Sau compaction:
  key=P001 → { stock: 90 }   (chỉ giữ mới nhất)
  key=P002 → { stock: 50 }
```

**Dùng khi:** Cần sync state giữa các service mà không dùng shared database.

---

## 12. Lưu ý vận hành

### Cấu hình Producer an toàn

```javascript
// ✅ Cấu hình production-ready
const producer = kafka.producer({
  acks: 'all',               // không mất message
  idempotent: true,          // tự động dedup ở broker level
  maxInFlightRequests: 5,    // set 1 nếu idempotent=false và cần strict ordering
  retries: 10,
  compression: CompressionTypes.LZ4, // nhanh hơn GZIP, nén tốt hơn NONE
});
```

### Graceful Shutdown

```javascript
const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach(type => {
  process.on(type, async (err) => {
    console.error(`[Kafka] Process error (${type}):`, err.message);
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(1);
  });
});

signalTraps.forEach(type => {
  process.once(type, async () => {
    console.log(`[Kafka] Graceful shutdown (${type})`);
    await consumer.disconnect(); // tự commit offset đang pending
    await producer.disconnect();
    process.exit(0);
  });
});
```

### Monitoring metrics quan trọng

| Metric | Ý nghĩa | Alert khi |
|---|---|---|
| `consumer_lag` | Số message chưa được xử lý | > 10,000 |
| `messages_in_per_sec` | Throughput producer | Giảm đột ngột |
| `under_replicated_partitions` | Partition không đủ replica | > 0 |
| `active_controller_count` | Số controller trong cluster | ≠ 1 |
| `offline_partitions_count` | Partition không có leader | > 0 |

### Checklist production

- [ ] `replication-factor >= 3` cho topic quan trọng
- [ ] `acks: 'all'` + `idempotent: true` cho producer
- [ ] Tắt `autoCommit`, commit thủ công sau khi xử lý
- [ ] Implement Dead Letter Topic cho message lỗi
- [ ] Đặt `retention.ms` phù hợp (tránh disk đầy)
- [ ] Monitor consumer lag liên tục
- [ ] Số partition ≥ số consumer instance tối đa
- [ ] Consumer phải **idempotent** (at-least-once delivery)
- [ ] Dùng `messageId` để deduplicate khi cần exactly-once
- [ ] Graceful shutdown để tránh rebalance không cần thiết

---

## 13. Những điều hay bị hiểu nhầm về Kafka

### 13.1 "Kafka là nơi lưu trữ lâu dài" — SAI

Đây là hiểu lầm phổ biến nhất. Kafka **không phải** kho lưu trữ lâu dài.

> Kafka giống như **băng chuyền** — data chạy qua để được xử lý, không phải chỗ chứa hàng lâu dài.

Mặc định Kafka chỉ giữ data **7 ngày** rồi tự xóa. Sau 7 ngày, data biến mất hoàn toàn.

**Kiến trúc thực tế:**

```
Giao dịch xảy ra
      ↓
   Kafka topic "transactions"
   (giữ 7-30 ngày để các service đọc real-time)
      ↓              ↓                   ↓
Fraud Detection   Account Service    Kafka Connect
(phát hiện        (cập nhật DB           ↓
gian lận          số dư ngay)     PostgreSQL / S3 / BigQuery
real-time)                        (lưu 5-10 năm, audit, query)
```

**Kafka Connect** là công cụ tự động hút data từ Kafka → đổ vào kho dài hạn. Sau khi đổ xong, Kafka tự xóa theo retention policy — không tốn disk.

---

### 13.2 "Kafka chỉ để log" — CHƯA ĐỦ

Log hành vi user chỉ là một trong nhiều use case. Kafka còn được dùng cho **Data Pipeline** — và đây là thứ Netflix, Uber dùng hàng ngày.

---

#### Netflix và Uber dùng Kafka như thế nào?

Hãy lấy ví dụ **Netflix**. Khi bạn xem một bộ phim, Netflix cần đồng thời cập nhật rất nhiều thứ:

- **Recommendation engine** biết bạn đang xem gì để gợi ý phim tiếp theo
- **Analytics dashboard** cập nhật số lượt xem real-time
- **Billing service** ghi nhận bạn đang dùng gói nào
- **CDN service** biết bạn đang xem để pre-load tập tiếp theo
- **Search index** cập nhật phim này đang trending

**Nếu không có Kafka**, video-service phải gọi HTTP đến từng service một:

```
video-service
  ├──► HTTP POST recommendation-service/track    (nếu recommendation chết → cả luồng lỗi)
  ├──► HTTP POST analytics-service/record        (nếu analytics chậm → video-service chờ)
  ├──► HTTP POST billing-service/log             (coupling chặt, sửa 1 service phải sửa cả đây)
  ├──► HTTP POST cdn-service/preload
  └──► HTTP POST search-service/update-trending
```

Vấn đề rõ ràng: **video-service biết tên của 5 service khác**. Thêm service mới phải vào sửa video-service. Một service chết có thể kéo cả luồng xuống.

**Với Kafka**, video-service chỉ làm một việc duy nhất:

```
video-service ──publish──► Kafka topic: "video.watched"
                           { userId, videoId, timestamp, duration }

                                    │
              ┌─────────────────────┼──────────────────────┐
              ▼                     ▼                      ▼
  recommendation-service    analytics-service        cdn-service
  (tự subscribe, tự đọc)   (tự subscribe, tự đọc)  (tự subscribe, tự đọc)
```

video-service **không biết** recommendation-service tồn tại. Nó chỉ publish sự kiện *"có người vừa xem phim"* rồi đi. Ai muốn phản ứng với sự kiện đó thì tự subscribe.

**Kết quả thực tế của Netflix:**
- Thêm service mới (ví dụ A/B testing service) → chỉ cần subscribe topic, **không sửa một dòng code nào trong video-service**
- analytics-service chết → video-service không hay biết, vẫn chạy bình thường, analytics tự đọc lại khi recover
- 200 triệu người dùng cùng xem → Kafka xử lý hàng tỷ event/ngày mà không nghẽn

---

**Tương tự với Uber** — khi tài xế hoàn thành chuyến đi:

```
trip-service ──publish──► Kafka topic: "trip.completed"
                          { driverId, riderId, distance, fare, timestamp }

                                    │
              ┌─────────────────────┼──────────────────────┐
              ▼                     ▼                      ▼
  payment-service           rating-service          driver-earnings-service
  (tự thu tiền)             (nhắc user đánh giá)    (cộng thu nhập tài xế)
```

Ba service xử lý **song song**, không phải tuần tự. Uber không cần trip-service biết payment-service hay rating-service tồn tại.

---

**Data Pipeline — sơ đồ đầy đủ:**

```
MySQL (production DB)
      ↓  Kafka Connect (CDC - Change Data Capture)
         (tự động đọc mọi thay đổi trong DB và đẩy lên Kafka)
    Kafka
      ↓              ↓              ↓
Elasticsearch    BigQuery        Redis Cache
(search)         (analytics)     (invalidate cache khi data thay đổi)
```

**CDC (Change Data Capture)** là kỹ thuật Kafka Connect dùng để theo dõi mọi thay đổi trong DB (INSERT/UPDATE/DELETE) và tự động publish lên Kafka — không cần sửa code ứng dụng.

---

### 13.3 "Event Sourcing" — tại sao bank bắt buộc dùng

**Cách truyền thống — chỉ lưu state hiện tại:**

```
Bảng accounts trong DB:
┌─────────────┬──────────────┐
│ account_id  │ balance      │
├─────────────┼──────────────┤
│ ACC-001     │ 3,500,000    │
└─────────────┴──────────────┘
```

Bạn nhìn vào DB chỉ thấy **số dư hiện tại là 3.5 triệu**. Không biết tại sao lại là 3.5 triệu, tiền từ đâu ra, đã tiêu gì, ai đã thay đổi.

**Cách Event Sourcing — lưu từng sự kiện lên Kafka:**

```
Kafka topic: "account.transactions" (không bao giờ xóa, đổ vào cold storage)

offset 0: { type: "account.opened",    amount: 5,000,000  } ← mở tài khoản
offset 1: { type: "money.deposited",   amount: 2,000,000  } ← nạp tiền
offset 2: { type: "money.withdrawn",   amount: 1,500,000  } ← rút tiền
offset 3: { type: "money.transferred", amount: 2,000,000  } ← chuyển đi
offset 4: { type: "money.deposited",   amount: 500,000    } ← nhận về

Số dư = replay toàn bộ event:
5,000,000 + 2,000,000 - 1,500,000 - 2,000,000 + 500,000 = 4,000,000 ✅
```

**Tại sao bank bắt buộc phải làm vậy:**

Tình huống 1 — Khách hàng khiếu nại *"Tôi không rút 1.5 triệu ngày hôm qua"*:
```json
{
  "type": "money.withdrawn",
  "amount": 1500000,
  "timestamp": "2026-06-09T14:32:11Z",
  "ip": "118.70.xx.xx",
  "device": "iPhone 15",
  "location": "Hà Nội"
}
```
→ Bank lấy ra ngay toàn bộ context của giao dịch đó.

Tình huống 2 — Ngân hàng Nhà nước yêu cầu xem toàn bộ giao dịch 5 năm qua:
→ Không query Kafka (đã hết retention), query BigQuery / Data Warehouse — nơi Kafka Connect đã đổ vào từng ngày.

Tình huống 3 — Phát hiện bug tính lãi suất sai cho 10,000 tài khoản:
```
1. Sửa bug trong code
2. Replay lại toàn bộ event từ thời điểm xảy ra bug
3. Tính lại số dư đúng cho 10,000 tài khoản
4. Bù tiền chênh lệch
```
→ Nếu chỉ lưu state cuối, **không có cách nào biết ai bị ảnh hưởng**.

**Điểm mấu chốt:**

```
State (DB truyền thống):     Event Log (Kafka → cold storage):

balance = 3,500,000          Đây là SỰ THẬT — đã xảy ra, không thể thay đổi
    ↑                        balance chỉ là KẾT QUẢ TÍNH TOÁN từ các event
  Có thể sai nếu có bug
```

> State là thứ có thể sai. Event là thứ đã xảy ra — bất biến, không thể xóa.

---

### 13.4 Choreography-based SAGA — nhiều service phối hợp qua Kafka

Đây là pattern Uber, Netflix dùng để điều phối luồng xử lý qua nhiều service **mà không có service nào gọi trực tiếp vào service khác**.

**Câu hỏi thường gặp:** *"Vậy các service biết đến lượt mình xử lý bằng cách nào?"*

**Trả lời:** Không service nào "theo dõi đến lượt mình" theo kiểu tuần tự. Mỗi service chỉ subscribe **topic cụ thể của mình** và phản ứng khi thấy event phù hợp.

```
order-service        Kafka                inventory-service    payment-service
     │                  │                        │                    │
     │─"order.created"──►│                        │                    │
     │                  │───────────────────────►│                    │
     │                  │               xử lý trừ kho                  │
     │                  │◄──"inventory.reserved"─│                    │
     │                  │                                              │
     │                  │──────────────────────────────────────────────►│
     │                  │                                     xử lý thanh toán
     │                  │◄────────────────────────"payment.completed"──│
     │                  │
     │◄──đọc "payment.completed", cập nhật status đơn hàng
```

Mỗi service:
1. **Subscribe** topic của mình — `inventory-service` chỉ nghe `"order.created"`
2. **Xử lý** phần việc của mình — trừ kho
3. **Publish** event mới — `"inventory.reserved"`
4. **Không biết** service nào sẽ đọc event đó tiếp theo

**So sánh với RabbitMQ command-based:**

| | RabbitMQ (command) | Kafka (choreography) |
|---|---|---|
| **order-service biết gì** | Biết inventory-service tồn tại, gửi lệnh trực tiếp | Chỉ publish event "order.created", không quan tâm ai nhận |
| **Thêm service mới** | Phải sửa order-service để gửi thêm lệnh | Chỉ cần service mới subscribe topic, không sửa gì cả |
| **Coupling** | Loose nhưng vẫn biết nhau | Hoàn toàn độc lập |
| **Replay/Debug** | ❌ | ✅ Replay lại toàn bộ luồng |

---

### 13.5 Ai lưu gì, bao lâu, mục đích gì

| Hệ thống | Lưu gì | Bao lâu | Mục đích chính |
|---|---|---|---|
| **Kafka** | Event stream | 7–30 ngày | Real-time processing, pipeline vận chuyển |
| **PostgreSQL / MySQL** | State hiện tại | Mãi mãi | App đọc/ghi hàng ngày |
| **S3 / GCS** | Raw event archive | 5–10 năm | Compliance, replay quy mô lớn |
| **BigQuery / Redshift** | Structured history | 5–10 năm | Analytics, báo cáo, audit |
| **Redis** | Hot state, cache | Giờ / ngày | Truy vấn cực nhanh |

> Kafka không cạnh tranh với database hay cold storage. Nó là **lớp vận chuyển** nối tất cả hệ thống trên lại với nhau.
