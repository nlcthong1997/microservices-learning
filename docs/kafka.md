# Apache Kafka — Hướng dẫn toàn diện

> Tài liệu này không chỉ giải thích "Kafka là gì" mà còn đi sâu vào **tại sao nó được thiết kế như vậy**, **bên trong hoạt động ra sao**, và **các công ty lớn áp dụng như thế nào trong thực tế**. Đọc xong bạn sẽ hiểu Kafka như người đã từng vận hành nó trong production.

---

## Mục lục

1. [Tại sao Kafka ra đời?](#1-tại-sao-kafka-ra-đời)
2. [Kafka là gì?](#2-kafka-là-gì)
3. [Kafka hoạt động nhanh — tại sao?](#3-kafka-hoạt-động-nhanh--tại-sao)
4. [Kiến trúc & Các khái niệm cốt lõi](#4-kiến-trúc--các-khái-niệm-cốt-lõi)
   - [4.1 Topic](#41-topic)
   - [4.2 Partition — Đơn vị song song hóa](#42-partition--đơn-vị-song-song-hóa)
   - [4.3 Broker & Cluster](#43-broker--cluster)
   - [4.4 Replication & ISR](#44-replication--isr)
   - [4.5 ZooKeeper & KRaft](#45-zookeeper--kraft)
5. [Producer — Hiểu sâu](#5-producer--hiểu-sâu)
6. [Consumer & Consumer Group — Hiểu sâu](#6-consumer--consumer-group--hiểu-sâu)
   - [6.1 Consumer Group là gì và tại sao?](#61-consumer-group-là-gì-và-tại-sao)
   - [6.2 Partition Assignment & Rebalancing](#62-partition-assignment--rebalancing)
   - [6.3 Offset Management](#63-offset-management)
7. [Delivery Semantics — Đảm bảo giao nhận](#7-delivery-semantics--đảm-bảo-giao-nhận)
8. [Kafka vs RabbitMQ — Khi nào dùng cái nào?](#8-kafka-vs-rabbitmq--khi-nào-dùng-cái-nào)
9. [Các Pattern thực tế](#9-các-pattern-thực-tế)
   - [9.1 Event Sourcing](#91-event-sourcing)
   - [9.2 CQRS với Kafka](#92-cqrs-với-kafka)
   - [9.3 Choreography-based SAGA](#93-choreography-based-saga)
   - [9.4 Data Pipeline & CDC](#94-data-pipeline--cdc)
   - [9.5 Dead Letter Topic](#95-dead-letter-topic)
   - [9.6 Compacted Topic — State Store](#96-compacted-topic--state-store)
10. [Các ông lớn dùng Kafka như thế nào](#10-các-ông-lớn-dùng-kafka-như-thế-nào)
11. [Production Guide — Cấu hình thực tế](#11-production-guide--cấu-hình-thực-tế)
12. [Anti-patterns & Lỗi hay gặp](#12-anti-patterns--lỗi-hay-gặp)
13. [Mental Models — Cách tư duy đúng về Kafka](#13-mental-models--cách-tư-duy-đúng-về-kafka)

---

## 1. Tại sao Kafka ra đời?

Năm **2010**, LinkedIn đang đối mặt với một vấn đề không nhỏ: họ có hàng chục service phân tán, mỗi service tạo ra dữ liệu (metrics, logs, user activity), nhưng **không có cách nào hiệu quả để chia sẻ dữ liệu đó** giữa các hệ thống.

**Vấn đề cụ thể — Data pipeline hỗn loạn trước Kafka:**

```
[Web Service] ---HTTP---> [Metrics DB]
      |
      +---HTTP---> [Analytics DB]         (mỗi service phải biết địa chỉ mọi consumer)
      |
      +---HTTP---> [Recommendation Engine]  (thêm consumer mới = sửa source service)
      |
      +---HTTP---> [Search Index]           (consumer chậm = source service bị block)
```

LinkedIn thử dùng **ActiveMQ** và các message queue truyền thống, nhưng không đáp ứng được throughput ~10 tỷ event/ngày thời điểm đó (nay là **7 nghìn tỷ message/ngày**).

**Jay Kreps**, **Neha Narkhede**, và **Jun Rao** đặt câu hỏi: *"Nếu thiết kế từ đầu cho bài toán này, chúng ta sẽ làm gì?"*

Câu trả lời là **Kafka** — mã nguồn mở năm 2011, donate cho Apache Foundation năm 2012.

**Insight cốt lõi của thiết kế Kafka:**

> Thay vì dùng queue (đọc xong là xóa), dùng **append-only log** — như nhật ký, chỉ ghi thêm, không xóa. Consumer tự theo dõi vị trí đọc của mình qua **offset**.

Đây là quyết định thiết kế thay đổi tất cả — và bạn sẽ thấy hệ quả của nó xuyên suốt tài liệu này.

---

## 2. Kafka là gì?

**Apache Kafka** là một **distributed event streaming platform** — không phải message queue truyền thống mà là **hệ thống lưu trữ và vận chuyển luồng sự kiện** theo thời gian thực.

**So sánh cốt lõi — cách tư duy đúng:**

```
Traditional Queue (RabbitMQ):        Kafka Commit Log:

[A][B][C][D]  <-- queue              offset:  0    1    2    3    4    5
                                     msg:    [A]  [B]  [C]  [D]  [E]  [F]
Consumer1 đọc A -> A bị xóa
Consumer1 đọc B -> B bị xóa                       ^              ^
Queue: [C][D]                               Group-1          Group-2
                                          (offset=1)       (offset=3)
- Chỉ 1 consumer dùng được          - Nhiều consumer group đọc độc lập
- Mất data sau khi đọc              - Data vẫn còn theo retention policy
- Không thể đọc lại                 - Replay từ offset bất kỳ
```

**Kafka phù hợp khi:**

| Bạn cần | Ví dụ thực tế |
|---|---|
| **Throughput cực cao** | 1 triệu message/giây trên 1 node |
| **Replay** | Đọc lại event 7 ngày trước sau khi deploy bug fix |
| **Multiple consumers** | Analytics, ML, Audit cùng đọc 1 event stream |
| **Audit log bất biến** | Bank transaction history |
| **Stream processing** | Real-time aggregation trên dòng dữ liệu |
| **Data pipeline** | DB → Kafka → Data Warehouse tự động |
| **Event sourcing / CQRS** | Lưu lịch sử thay đổi, tái tạo state |

**Kafka KHÔNG phù hợp khi:**

| Bạn cần | Dùng gì thay thế |
|---|---|
| **Routing linh hoạt** (fanout/topic/headers exchange) | RabbitMQ |
| **RPC / Request-Reply** | gRPC, REST |
| **Priority queue** | RabbitMQ |
| **Hệ thống nhỏ, ít traffic** | RabbitMQ (overhead Kafka quá lớn) |
| **Latency < 1ms** | RabbitMQ (~0.5ms vs Kafka ~5-15ms) |

---

## 3. Kafka hoạt động nhanh — tại sao?

Kafka đạt **1 triệu message/giây** trên một máy thông thường nhờ 4 kỹ thuật được kết hợp thông minh. Hiểu điều này giúp bạn cấu hình đúng và debug performance vấn đề.

### 3.1 Sequential I/O — Disk tuần tự nhanh hơn bạn nghĩ

```
Random disk access:   ~10ms  (đầu đọc phải di chuyển)
Sequential disk read: ~0.1ms (đầu đọc không di chuyển, chỉ đọc tiếp)

Random RAM access:  ~100ns
Sequential RAM:     ~10ns

Disk sequential / RAM random = 0.1ms / 100ns = 1000x chậm hơn
NHƯNG: Kafka chỉ dùng sequential I/O (append-only log)
       -> Disk throughput ~200-600 MB/s, gần bằng RAM nhiều trường hợp
```

**Hệ quả thực tế:** Kafka chạy trên HDD rẻ tiền vẫn nhanh hơn nhiều database trên SSD vì database cần random read/write. Đây là lý do LinkedIn chạy Kafka trên cụm HDD JBOD thay vì RAID SSD.

### 3.2 Page Cache — OS là đồng minh của Kafka

Linux kernel tự động cache file reads vào RAM (**page cache**). Kafka khai thác triệt để:

```
Trường hợp thường gặp: Consumer đọc message vừa được produce

Producer writes -> OS page cache (RAM) -> disk (async, background)
Consumer reads  <- OS page cache (RAM)  (không đọc disk!)

-> Kafka trở thành hệ thống in-memory trong thực tế với traffic bình thường
-> Kafka không tự quản lý memory heap -> tránh Java GC pauses
```

Đây là lý do Kafka khuyến nghị **không dùng heap lớn** (thường 4-8GB JVM heap là đủ) và để OS page cache dùng phần RAM còn lại.

### 3.3 Zero-Copy — Tránh copy không cần thiết

```
Traditional read + send:              Zero-copy (sendfile syscall):

Disk                                  Disk
 |                                     |
 v  copy 1 (DMA)                       v  copy 1 (DMA)
Kernel Buffer                         Kernel Buffer
 |                                     |
 v  copy 2 (CPU)                       v  copy 2 (DMA, không qua CPU!)
User Space Buffer                     Socket Buffer
 |                                     |
 v  copy 3 (CPU)                       v  NIC -> Network
Socket Buffer
 |
 v  copy 4 (DMA)
NIC -> Network

4 lần copy, 2 syscalls                2 lần copy (DMA), 1 syscall
CPU tham gia 2 lần copy               CPU không tham gia copy nào
```

Kafka dùng `FileChannel.transferTo()` của Java (ánh xạ xuống `sendfile()` trên Linux) → data đi thẳng từ page cache đến network card mà không qua CPU.

### 3.4 Batching & Compression

```javascript
// Thay vì gửi 1000 message riêng lẻ:
// 1000 lần syscall + 1000 network round-trips

// Kafka gom batch:
producer.config = {
  lingerMs: 5,          // chờ 5ms để gom thêm message
  batchSize: 32768,     // batch tối đa 32KB mỗi partition
  compression: 'lz4',   // compress cả batch -> giảm 60-80% network bandwidth
}
// 1 lần syscall + 1 network round-trip cho 1000 message
// -> giảm overhead ~1000x
```

LZ4 là lựa chọn tốt nhất cho hầu hết use case: nén nhanh nhất, CPU overhead thấp, compression ratio chấp nhận được. GZIP nén tốt hơn nhưng CPU tốn hơn nhiều.

---

## 4. Kiến trúc & Các khái niệm cốt lõi

```
Tổng quan Kafka Cluster:

  Producers                Kafka Cluster                   Consumers
  ---------          -------------------------         -----------------
                     Topic: "order-events"
  [Order Svc]  -->   P0: [e1]-[e4]-[e7]-[e10]  -->   [Analytics]  (Group A)
                     P1: [e2]-[e5]-[e8]-[e11]  -->   [ML Service] (Group B)
  [Pay Svc]    -->   P2: [e3]-[e6]-[e9]-[e12]  -->   [Audit Svc]  (Group C)

                     Topic: "user-behavior-logs"
  [Web Svc]    -->   P0: [b1]-[b2]-[b3]...     -->   [Analytics]  (Group A)
                     P1: [b4]-[b5]-[b6]...
```

### 4.1 Topic

Topic là **kênh phân loại event** — tương tự tên bảng trong database, hoặc tên folder.

**Quy ước đặt tên (convention phổ biến):**

```
{domain}.{entity}.{action}

order.created            <- service order vừa tạo đơn
order.payment.completed  <- thanh toán xong
order.shipped            <- đã giao vận
user.registered          <- user đăng ký mới
inventory.updated        <- kho thay đổi
user-behavior-logs       <- stream hành vi (analytics)
```

**Configuration quan trọng khi tạo topic:**

```bash
kafka-topics.sh --create \
  --topic order-events \
  --partitions 6 \                        # số partition (không giảm được sau khi tạo!)
  --replication-factor 3 \                # mỗi partition có 3 bản sao
  --config retention.ms=604800000 \       # giữ data 7 ngày
  --config min.insync.replicas=2 \        # tối thiểu 2 replica phải ACK khi write
  --config compression.type=lz4           # broker tự compress nếu producer không nén
```

---

### 4.2 Partition — Đơn vị song song hóa

Partition là **đơn vị cơ bản nhất** của Kafka. Mọi thứ xoay quanh partition: ordering, throughput, parallelism.

```
Topic "order-events" với 3 partitions:

P0: [order#1]--[order#4]--[order#7]--[order#10]-->  (append-only, offset tăng dần)
P1: [order#2]--[order#5]--[order#8]--[order#11]-->
P2: [order#3]--[order#6]--[order#9]--[order#12]-->
     offset=0   offset=1   offset=2   offset=3

Ordering ĐẢMM BẢO: trong cùng partition (FIFO)
Ordering KHÔNG đảm bảo: giữa các partition khác nhau
```

**Kafka chọn partition nào để ghi?**

```
Case 1: Message có key
  producer.send({ key: 'user-123', value: '...' })
  -> hash('user-123') % numPartitions = 1
  -> Luôn vào Partition 1
  -> Mọi event của user-123 đều theo đúng thứ tự

Case 2: Không có key
  producer.send({ value: '...' })
  -> Round-robin hoặc sticky batch (gom batch vào 1 partition rồi chuyển)
  -> Phân tán đều, không đảm bảo ordering
```

**Tại sao key quan trọng — ví dụ bank:**

```
Chuyển tiền từ tài khoản ACC-001 (3 lệnh theo thứ tự):
  [1] deposit  +5M  -> balance = 5M
  [2] withdraw -3M  -> balance = 2M
  [3] withdraw -1M  -> balance = 1M

Nếu [2] và [3] vào 2 partition khác nhau:
  Consumer A xử lý [3] trước [2]
  -> Số dư tính sai! Thậm chí âm!

Giải pháp: key = accountId
  -> Cùng account luôn vào cùng partition
  -> Ordering đảm bảo cho từng tài khoản
```

**Chọn số partition phù hợp:**

```
Quy tắc: partitions >= số consumer instance tối đa bạn muốn chạy

Ví dụ: cần scale tối đa 12 instances -> tạo ít nhất 12 partitions
       throughput_mục_tiêu / throughput_1_partition (thường ~50MB/s)

CẢNH BÁO:
  - Không thể GIẢM số partition sau khi tạo
  - Tăng partition sẽ phá vỡ ordering theo key (hash(key) % N thay đổi)
  -> Tính toán kỹ trước khi tạo topic trong production
  -> Bắt đầu với 6-12, scale sau khi có data thực tế
```

---

### 4.3 Broker & Cluster

**Broker** là một Kafka server node — lưu trữ partition data và phục vụ producer/consumer.

```
Kafka Cluster 3 brokers, Topic với 6 partitions, replication-factor=3:

  Broker 0:  [P0-Leader] [P1-Follow] [P2-Follow] [P3-Leader] [P4-Follow] [P5-Follow]
  Broker 1:  [P0-Follow] [P1-Leader] [P2-Follow] [P3-Follow] [P4-Leader] [P5-Follow]
  Broker 2:  [P0-Follow] [P1-Follow] [P2-Leader] [P3-Follow] [P4-Follow] [P5-Leader]

  - Mỗi partition có 1 Leader + 2 Follower
  - Leader xử lý tất cả read/write từ client
  - Follower chỉ sync data từ Leader (passive)
  - Nếu Broker 0 chết -> controller bầu Broker 1 hoặc 2 làm Leader cho P0, P3
```

**Bootstrap servers** — Producer/Consumer chỉ cần biết 1-3 broker ban đầu. Kafka tự trả về metadata toàn bộ cluster:

```javascript
const kafka = new Kafka({
  brokers: ['broker-1:9092', 'broker-2:9092'],  // chỉ cần vài broker để bootstrap
  // Kafka tự discover phần còn lại qua metadata API
});
```

---

### 4.4 Replication & ISR

**ISR (In-Sync Replicas)** — tập hợp replica đang sync kịp với Leader. Đây là khái niệm quan trọng nhất cho reliability của Kafka.

```
Cấu hình an toàn cho production:
  replication-factor: 3      (3 bản sao)
  min.insync.replicas: 2     (tối thiểu 2 replica phải ACK)
  acks: all                  (producer đợi tất cả ISR)

Luồng write an toàn:
  Producer --> Leader (Broker 0) --> ACK về Producer
                    |
                    |--> Follower (Broker 1) ---+
                    |                           |--> cả 2 đã ACK -> OK
                    +--> Follower (Broker 2) ---+

  Chỉ sau khi min.insync.replicas (=2) đã ACK thì Leader mới ACK về Producer
  -> Nếu Leader chết ngay sau đó, Broker 1 hoặc 2 vẫn có đủ data
```

**3 kịch bản failure thường gặp:**

```
Kịch bản 1: Broker 1 chậm (network issue)
  ISR = [Broker0, Broker2]   <- Broker1 bị kick khỏi ISR sau replica.lag.time.max.ms
  Write vẫn tiếp tục (ISR size = 2 = min.insync.replicas)
  Broker 1 catch up -> được thêm lại vào ISR
  -> Không ảnh hưởng gì đến client

Kịch bản 2: Leader (Broker 0) chết đột ngột
  Controller phát hiện (leader.imbalance.check.interval.seconds)
  Bầu leader mới từ ISR (Broker 1 hoặc 2) trong < 30 giây
  Producer/Consumer tự reconnect đến leader mới
  -> Có thể có độ trễ 15-30 giây trong thời gian failover

Kịch bản 3: 2 broker chết (chỉ còn 1)
  ISR size = 1 < min.insync.replicas (= 2)
  Kafka từ chối write -> NotEnoughReplicasException
  -> Tốt hơn là báo lỗi ngay thay vì âm thầm mất data
```

---

### 4.5 ZooKeeper & KRaft

```
Lịch sử:
  Kafka < 2.8: bắt buộc ZooKeeper để quản lý:
    - Cluster metadata (broker list, topic configs, partition leaders)
    - Leader election khi broker chết
    - Consumer group coordination

  Vấn đề với ZooKeeper:
    - Là hệ thống riêng biệt, cần maintain và monitor riêng
    - Bottleneck khi cluster lớn (giới hạn ~200,000 partitions)
    - Phức tạp hóa deployment và operation

  Kafka >= 3.3 (KRaft mode): Kafka tự quản lý metadata
    - Không cần ZooKeeper
    - Metadata operations nhanh hơn ~10x
    - Hỗ trợ cluster scale lớn hơn
    - Mặc định từ Kafka 3.7 (KRaft only)
```

```yaml
# docker-compose với KRaft — không cần ZooKeeper service riêng
kafka:
  image: confluentinc/cp-kafka:7.6.0
  environment:
    KAFKA_PROCESS_ROLES: broker,controller   # cùng node làm cả 2 vai trò
    KAFKA_NODE_ID: 1
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
    CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qg'   # UUID cố định cho cluster
```



---

## 5. Producer — Hiểu sâu

### 5.1 Những gì xảy ra khi producer.send()

```
Producer gửi message — luồng bên trong:

  1. serialize(key, value)  -> bytes
  2. Chọn partition:
       key != null  ->  Murmur2Hash(key) % numPartitions
       key == null  ->  Round-robin hoặc Sticky batch
  3. Thêm vào RecordBatch (buffer in-memory, per partition)
  4. Batch đủ lớn (batch.size) HOẶC đủ thời gian (linger.ms)?
  5. Compress batch (LZ4/GZIP/SNAPPY/ZSTD)
  6. Gửi đến Leader của partition
  7. Leader ghi xuống local log
  8. Leader forward đến các ISR Follower
  9. Khi đủ ISR đã ACK -> Leader gửi ACK về Producer
  10. Producer callback / resolve Promise
```

### 5.2 Cấu hình production-ready

```javascript
const { Kafka, CompressionTypes, logLevel } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  retry: {
    initialRetryTime: 100,   // ms
    retryFactor: 2,           // exponential backoff: 100, 200, 400...
    maxRetryTime: 30_000,
    retries: 10,
  },
  logLevel: process.env.NODE_ENV === 'production' ? logLevel.ERROR : logLevel.WARN,
});

const producer = kafka.producer({
  // --- Reliability ---
  acks: 'all',               // đợi tất cả ISR ACK (an toàn nhất)
  idempotent: true,          // tự dedup ở broker level (PID + sequence number)
                             // -> bật tự động khi acks='all' + retries > 0

  // --- Retry ---
  retries: 10,
  maxInFlightRequests: 5,    // PHẢI set 1 nếu idempotent=false + cần strict ordering
                             // idempotent=true -> để 5 vẫn đảm bảo ordering

  // --- Performance ---
  compression: CompressionTypes.LZ4,  // LZ4: nhanh nhất
                                       // SNAPPY: cân bằng speed/ratio
                                       // GZIP: nén tốt nhất nhưng CPU cao
  // lingerMs: 5,            // chờ 5ms gom thêm message vào batch
  // batchSize: 32768,        // 32KB per batch per partition
});
```

**Giải thích `acks` — quan trọng nhất:**

```
acks = 0:
  Producer -> Broker  (không đợi gì cả)
  Nhanh nhất, nhưng không biết message có đến không
  Mất data nếu broker crash ngay lúc nhận
  Dùng cho: metrics, logs không quan trọng

acks = 1:
  Producer -> Leader  (đợi Leader ghi xong, Follower chưa cần sync)
  Nếu Leader crash TRƯỚC KHI Follower sync -> mất data!
  Dùng cho: hầu hết use case chấp nhận được

acks = all (-1):
  Producer -> Leader + tất cả ISR  (đợi min.insync.replicas đã ghi)
  Chậm hơn ~1-2ms, nhưng AN TOÀN NHẤT
  Nếu Leader crash -> Follower vẫn có đủ data
  Dùng cho: order, payment, inventory - data quan trọng
```

**Giải thích `idempotent` — tại sao cần thiết:**

```
Vấn đề khi KHÔNG có idempotent:
  1. Producer gửi message #5
  2. Broker ghi xong nhưng ACK bị mất trên mạng
  3. Producer timeout, retry gửi lại message #5
  4. Broker ghi lần 2 -> DUPLICATE!

Với idempotent = true:
  - Producer có PID (Producer ID) duy nhất từ Broker
  - Mỗi message có sequence number tăng dần
  - Broker track PID + sequence -> reject duplicate tự động
  -> Producer có thể retry thoải mái mà không lo duplicate
```

### 5.3 Transaction — Exactly-once writes

```javascript
// Dùng khi cần ghi vào nhiều topic là một atomic operation (all or nothing)
const producer = kafka.producer({
  transactionalId: 'order-service-tx-1',  // ID cố định, unique per producer instance
  idempotent: true,
  maxInFlightRequests: 1,
});

await producer.connect();

await producer.transaction(async (tx) => {
  // Ghi vào topic 1
  await tx.send({
    topic: 'order-events',
    messages: [{ key: orderId, value: JSON.stringify(orderEvent) }],
  });

  // Ghi vào topic 2 trong cùng transaction
  await tx.send({
    topic: 'audit-log',
    messages: [{ key: orderId, value: JSON.stringify(auditEvent) }],
  });

  // Nếu có exception -> tx tự động abort
  // Cả 2 topic đều không nhận message
});
// Commit thành công -> cả 2 topic đều nhận message
```

---

## 6. Consumer & Consumer Group — Hiểu sâu

### 6.1 Consumer Group — Scale out và Fan-out

Consumer Group là cơ chế cho phép **scale out** (tăng throughput) và **fan-out** (nhiều use case) đồng thời.

```
Topic "order-events" có 6 partitions:

--- Scale out (1 group, nhiều consumer xử lý song song) ---

Group "order-processor":
  Consumer-1  ->  P0, P1   (xử lý 2 partitions)
  Consumer-2  ->  P2, P3
  Consumer-3  ->  P4, P5
  -> 3x throughput so với 1 consumer
  -> Mỗi message chỉ được xử lý BỞI MỘT consumer trong group

--- Fan-out (nhiều groups, mỗi group có use case khác) ---

Group "analytics"   ->  đọc tất cả 6 partitions (6 consumers)
Group "ml-pipeline" ->  đọc tất cả 6 partitions (độc lập với analytics)
Group "audit"       ->  đọc tất cả 6 partitions (độc lập với cả 2)

-> Mỗi group có offset riêng -> không ảnh hưởng nhau
-> Analytics xử lý chậm không ảnh hưởng gì đến ML Pipeline
```

**Quy tắc vàng: consumer count <= partition count**

```
6 partitions, 8 consumers trong cùng group:

  Consumer-1 -> P0
  Consumer-2 -> P1
  Consumer-3 -> P2
  Consumer-4 -> P3
  Consumer-5 -> P4
  Consumer-6 -> P5
  Consumer-7 -> (IDLE - không có partition!)
  Consumer-8 -> (IDLE - không có partition!)

-> Consumer-7, 8 lãng phí resource, không nhận bất kỳ message nào
-> Muốn 8 consumers hoạt động đồng thời -> cần tăng partition lên >= 8
```

### 6.2 Rebalancing — Vừa hay, vừa đau đầu

**Rebalancing** xảy ra khi topology của consumer group thay đổi. Đây là tính năng quan trọng nhưng cũng là nguồn gốc của nhiều vấn đề performance.

```
Trigger rebalancing:
  - Consumer mới join group (scale out)
  - Consumer chết (crash, network timeout, slow processing)
  - Consumer rời group (graceful shutdown)
  - Partition mới được thêm vào topic

Quá trình rebalancing:
  1. Group Coordinator (một broker) phát hiện topology thay đổi
  2. Gửi "REBALANCE" signal cho tất cả consumer trong group
  3. Tất cả consumer DỪNG xử lý và revoke partitions
                  ^^^ đây là lúc consumer lag tăng
  4. Coordinator chọn một consumer làm "Group Leader"
  5. Group Leader tính toán assignment mới
  6. Coordinator phân phối assignment
  7. Consumer resume với partitions mới

Thời gian rebalance: 10-60 giây tùy cluster size và config
```

**Giảm thiểu rebalancing:**

```javascript
const consumer = kafka.consumer({
  groupId: 'analytics-service',

  // Tăng session timeout (mặc định 10s là quá thấp cho processing nặng)
  sessionTimeout: 30_000,          // consumer có 30s để gửi heartbeat

  // Heartbeat interval PHẢI < sessionTimeout / 3
  heartbeatInterval: 3_000,        // 3s < 30s/3 = 10s -> OK

  // Max thời gian giữa 2 lần poll (mặc định 5 phút)
  // Nếu processing mỗi batch > maxPollInterval -> trigger rebalance!
  maxPollInterval: 300_000,

  // Static membership: consumer có ID cố định
  // -> restart trong vòng sessionTimeout không trigger rebalance
  groupInstanceId: `analytics-${process.env.POD_NAME}`,
});

// Trong long-running processing: gọi heartbeat() định kỳ
await consumer.run({
  eachMessage: async ({ message, heartbeat }) => {
    for (const item of parseLargePayload(message)) {
      await processItem(item);
      await heartbeat();  // báo cho Coordinator: "tôi vẫn alive"
    }
  },
});
```

### 6.3 Offset Management — Nơi bug thường xảy ra nhất

Offset xác định consumer đã đọc đến đâu. **Quản lý offset sai = mất data hoặc duplicate**.

```
Partition 0 của "order-events":

offset:   0      1      2      3      4      5
msg:    [A]    [B]    [C]    [D]    [E]    [F]
                              ^
              Consumer committed offset = 3
              (đã đọc và xử lý A, B, C)
              Lần restart sau sẽ đọc từ D
```

**Auto-commit — dễ dùng nhưng có bug ẩn:**

```javascript
// autoCommit = true (mặc định)
// autoCommitInterval = 5000ms

// Bug 1: LOST MESSAGE
// t=0:    đọc offset 5, 6, 7
// t=5s:   autoCommit -> commit offset 8 (đã "đọc" đến 7)
// t=5.5s: đang xử lý offset 6 -> CRASH
// t=restart: consumer đọc từ offset 8 -> MẤT offset 6, 7!

// Bug 2: DUPLICATE (ít phổ biến hơn)
// t=0:    đọc offset 5
// t=1s:   xử lý xong, chưa đến 5s để autoCommit
// t=2s:   CRASH
// t=restart: consumer đọc lại từ offset 5 -> DUPLICATE
```

**Manual commit — khuyến nghị cho production:**

```javascript
await consumer.run({
  autoCommit: false,  // TẮT auto-commit

  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value.toString());

    // Xử lý TRƯỚC, commit SAU
    await processEvent(event);  // nếu throw -> offset không commit -> đọc lại

    // Commit offset TIẾP THEO (không phải offset hiện tại)
    await consumer.commitOffsets([{
      topic,
      partition,
      offset: (BigInt(message.offset) + 1n).toString(),
    }]);
  },
});
```

**Batch processing với manual commit — hiệu quả hơn:**

```javascript
await consumer.run({
  autoCommit: false,
  eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
    for (const message of batch.messages) {
      try {
        await processMessage(message);
        resolveOffset(message.offset);   // đánh dấu: offset này đã xử lý OK
        await heartbeat();               // tránh session timeout khi batch lớn
      } catch (err) {
        logger.error('Failed', { offset: message.offset, err: err.message });
        // Không resolveOffset -> message sẽ đọc lại sau restart
        // Cẩn thận: poison pill message -> infinite loop -> cần dead letter topic
        break;
      }
    }
    await commitOffsetsIfNecessary();  // commit tất cả resolved offsets một lần
  },
});
```

**Replay từ timestamp cụ thể:**

```javascript
// Đọc lại tất cả event từ ngày 1/1/2026
const admin = kafka.admin();
const offsets = await admin.fetchTopicOffsetsByTimestamp(
  'order-events',
  new Date('2026-01-01T00:00:00Z').getTime()
);

// Seek đến các offset đó
await consumer.seek({
  topic: 'order-events',
  partition: 0,
  offset: offsets[0].offset,
});
```

---

## 7. Delivery Semantics — Đảm bảo giao nhận

### At-most-once — Có thể mất, không duplicate

```
Dùng cho: metrics, telemetry, logs không quan trọng
          (mất 1 metric không ai biết, nhưng duplicate làm đếm sai)

Producer: { acks: 0, retries: 0 }

Consumer: commit offset TRƯỚC khi xử lý
  t=1: đọc offset 5
  t=2: commit offset 6   <- đã commit!
  t=3: đang process()
  t=4: CRASH
  t=5: restart -> đọc từ offset 6 (offset 5 BỊ MẤT)
```

### At-least-once — Không mất, có thể duplicate ← Phổ biến nhất

```
Dùng cho: order, inventory, payment
          (chấp nhận xử lý 2 lần, không bao giờ bỏ sót)

Producer: { acks: 'all', retries: 10 }

Consumer: commit offset SAU khi xử lý
  t=1: đọc offset 5
  t=2: process() -> thành công
  t=3: CRASH trước khi commit!
  t=4: restart -> đọc từ offset 5 -> process lại (DUPLICATE!)

Consumer PHẢI idempotent:
```

```javascript
// Idempotency pattern với Redis
async function processOrderEvent(event) {
  const key = `processed:${event.messageId}`;

  if (await redis.exists(key)) {
    logger.info('Duplicate, skipping:', event.messageId);
    return;  // đã xử lý rồi, bỏ qua
  }

  await reserveInventory(event);
  await redis.set(key, '1', 'EX', 86400);  // TTL 24h
}
```

### Exactly-once — Không mất, không duplicate ← Khó nhất

```
Dùng cho: bank transactions, financial ledger
          (không được mất VÀ không được duplicate)

Yêu cầu: Kafka Transactions (idempotent producer + transactional consumer)

Cơ chế:
  - Producer có transactionalId cố định
  - Broker gán ProducerEpoch (tăng dần) -> reject producer với epoch cũ
  - Atomic: ghi message + commit offset trong 1 transaction
  - Consumer chỉ đọc COMMITTED transactions (isolation.level: read_committed)

Chi phí: latency cao hơn ~50-100ms, throughput giảm ~10-30%
         Chỉ dùng khi thực sự cần (financial, ledger)
```

**Bảng so sánh:**

| Semantic | Mất message? | Duplicate? | Latency | Dùng cho |
|---|---|---|---|---|
| At-most-once | Có thể | Không | Thấp nhất | Metrics, logs |
| At-least-once | Không | Có thể | Trung bình | Hầu hết use case |
| Exactly-once | Không | Không | Cao nhất | Bank, finance |



---

## 8. Kafka vs RabbitMQ — Khi nào dùng cái nào?

| Tiêu chí | Apache Kafka | RabbitMQ |
|---|---|---|
| **Mô hình cốt lõi** | Distributed commit log (pull) | Message broker (push) |
| **Message sau khi đọc** | Vẫn còn theo retention | Xóa ngay sau khi ACK |
| **Replay** | ✅ Đọc lại từ bất kỳ offset | ❌ Không hỗ trợ |
| **Multiple consumers** | ✅ Nhiều group, hoàn toàn độc lập | ⚠️ Fanout exchange có, nhưng không replay |
| **Throughput** | Rất cao (~1M msg/s/node) | Vừa (~50K msg/s/node) |
| **Latency** | ~5-15ms | < 1ms |
| **Ordering** | ✅ Trong partition | ✅ Trong queue |
| **Routing linh hoạt** | ❌ Chỉ theo topic/partition | ✅ 4 loại exchange |
| **Priority queue** | ❌ Không hỗ trợ | ✅ Hỗ trợ |
| **Dead Letter** | Dead Letter Topic (thủ công) | Dead Letter Exchange (tự động) |
| **RPC/Request-Reply** | ❌ Phức tạp, không phù hợp | ✅ Dễ với reply queues |
| **Vận hành** | Phức tạp (broker, KRaft, monitoring) | Đơn giản hơn |

**Cây quyết định chọn Kafka hay RabbitMQ:**

```
Nhiều service khác nhau cần đọc CÙNG event không?
  Có -> Kafka (hoặc RabbitMQ fanout, nhưng không replay được)

Cần đọc lại data từ quá khứ không?
  Có -> Kafka (bắt buộc, RabbitMQ không có)

Throughput > 100K msg/s không?
  Có -> Kafka

Routing phức tạp (header/pattern/priority)?
  Có -> RabbitMQ

Cần RPC / request-reply (như gRPC qua message)?
  Có -> RabbitMQ, gRPC, hoặc REST

Startup nhỏ, < 3 service, traffic thấp?
  Có -> RabbitMQ (overhead vận hành Kafka quá lớn)
```

**Trong dự án này — tại sao dùng cả hai:**

```
RabbitMQ: order.created (task queue)
  - 1 producer (order-service)
  - 1 consumer (inventory-service) xử lý trừ kho
  - Cần ACK/NACK/DLX để đảm bảo xử lý đúng
  - Routing: order -> inventory (1-to-1 command)

Kafka: user-behavior-logs (stream analytics)
  - 1 producer (order-service)
  - N consumers (analytics-service, ml-service, audit-service)
  - Cần multiple groups đọc độc lập
  - Cần replay để re-train ML model từ historical data
```

> **Quy tắc thực tế:** Bắt đầu với RabbitMQ. Khi cần replay, multiple consumers, hoặc throughput vượt quá RabbitMQ — lúc đó migrate sang Kafka.

---

## 9. Các Pattern thực tế

### 9.1 Event Sourcing — Lưu lịch sử thay đổi, không chỉ state hiện tại

**Vấn đề với "chỉ lưu state hiện tại":**

```
Bảng accounts trong DB:

  acc_id  | balance
  --------+---------
  ACC-001 | 3,500K

Câu hỏi không thể trả lời:
  - Tại sao balance là 3.5 triệu?
  - Ai đã thay đổi, lúc mấy giờ, từ IP nào?
  - Ngày 5/6 balance là bao nhiêu?
  - Nếu có bug tính lãi -> làm sao tính lại cho 10,000 tài khoản?
```

**Event Sourcing — lưu từng sự kiện:**

```
Kafka topic "account.events" (key = accountId):

  offset  event_type            amount    timestamp
  ------  --------------------  --------  -------------------
  0       account.opened         +5,000K  2026-01-01 09:00
  1       money.deposited        +2,000K  2026-02-15 14:30
  2       money.withdrawn        -1,500K  2026-03-20 10:00
  3       money.transferred      -2,000K  2026-04-01 16:00
  4       money.deposited          +500K  2026-05-10 11:00

  Balance = replay: +5000 +2000 -1500 -2000 +500 = 4,000K
```

**Tại sao bank BẮT BUỘC dùng Event Sourcing:**

```
Scenario 1: Khách hàng khiếu nại "Tôi không rút 1.5 triệu"
  -> Lấy ra event offset 2:
     { amount: 1500K, ip: "118.70.xx.xx", device: "iPhone15", location: "Hà Nội" }
  -> Có đầy đủ bằng chứng để phân xử

Scenario 2: Ngân hàng Nhà nước yêu cầu sao kê 5 năm
  -> Replay từ offset 0 của tài khoản đó
  -> Xuất đầy đủ (data được archive vào cold storage, không bao giờ xóa)

Scenario 3: Bug tính lãi suất sai cho 10,000 tài khoản
  -> Sửa bug trong code
  -> Replay lại toàn bộ event từ thời điểm bug xảy ra
  -> Tính lại số dư đúng -> bù tiền chênh lệch
  -> Không có Event Sourcing: KHÔNG THỂ làm được!

Điểm mấu chốt:
  State (balance = 3.5M) là thứ CÓ THỂ SAI nếu có bug
  Event (giao dịch đã xảy ra) là SỰ THẬT KHÔNG THỂ THAY ĐỔI
```

```javascript
// Producer: ghi mỗi state change là 1 event
async function recordTransaction(accountId, type, amount, metadata) {
  await producer.send({
    topic: 'account.events',
    messages: [{
      key: accountId,    // cùng account -> cùng partition -> ordering đảm bảo
      value: JSON.stringify({
        messageId: crypto.randomUUID(),
        aggregateId: accountId,
        type,            // 'money.deposited', 'money.withdrawn', ...
        version: await getNextVersion(accountId),  // tăng dần, detect conflict
        payload: { amount, ...metadata },
        timestamp: new Date().toISOString(),
      }),
    }],
  });
}

// Consumer: rebuild state từ event history
async function getAccountBalance(accountId) {
  let balance = 0;
  for await (const event of getEvents(accountId, { fromBeginning: true })) {
    switch (event.type) {
      case 'account.opened':    balance += event.payload.amount; break;
      case 'money.deposited':   balance += event.payload.amount; break;
      case 'money.withdrawn':   balance -= event.payload.amount; break;
      case 'money.transferred': balance -= event.payload.amount; break;
    }
  }
  return balance;
}
// Optimization: lưu snapshot mỗi 100 events -> chỉ replay từ snapshot cuối
```

---

### 9.2 CQRS với Kafka — Tách Read/Write

**CQRS (Command Query Responsibility Segregation)** — tách luồng write và read. Kafka là backbone trung gian.

```
Vấn đề: Cùng một DB cho cả write và read
  - Write cần ACID transaction (chậm khi có index)
  - Read cần performance (index làm chậm write)
  -> Conflict không thể giải quyết khi scale

CQRS với Kafka:

  [Write API]                                    [Read API]
  POST /orders                                   GET /orders
       |                                              ^
       v                                              |
  [Order Service] -- publish --> [Kafka] --> [Projection Builder]
                   "order.created"               consume events
                                                 denormalize
                                                 write to Read DB
                                                 (Elasticsearch/Redis)
```

**Thực tế tại Amazon:**

```
Trang product page của Amazon tổng hợp từ 20+ services:
  "Customers also bought"   -> Recommendation service (Read DB riêng)
  "In stock, ships 2 days"  -> Inventory service (Read DB riêng)
  "4.3 stars, 1,234 reviews" -> Review service (Read DB riêng)
  "Prime eligible"          -> Fulfillment service (Read DB riêng)

Mỗi Read DB được cập nhật qua Kafka events:
  order.completed     -> recommendation service update model
  inventory.updated   -> inventory read DB update
  review.submitted    -> review aggregates update

Kết quả:
  - Trang product load < 100ms dù tổng hợp từ 20+ sources
  - Write path (đặt hàng) không liên quan đến read path (xem sản phẩm)
  - Mỗi service scale độc lập
```


---

### 9.3 Choreography-based SAGA — Distributed Transactions không cần coordinator

**Vấn đề:** Trong microservices, một business transaction span nhiều service. Nếu bước giữa fail, làm sao rollback?

```
Orchestration SAGA (coordinator-based):
  [SAGA Orchestrator] -> inventory: "reserve stock"
                      <- "stock reserved"
                      -> payment: "charge card"
                      <- "payment failed"
                      -> inventory: "release stock"  <- compensating action

  Nhược điểm:
  - Orchestrator là điểm fail tập trung
  - Tạo coupling: orchestrator biết tất cả service
  - Khó scale: orchestrator thành bottleneck

Choreography SAGA (event-driven, không có coordinator):
  order-service -> publish "order.created"
                                    |
              inventory-service subscribe
              trừ kho
              publish "inventory.reserved"
                                    |
              payment-service subscribe
              thu tiền
              publish "payment.completed" hoặc "payment.failed"
                                    |
  IF "payment.failed":
  inventory-service subscribe
  hoàn trả kho
  publish "inventory.rollback.completed"
```

**Mỗi service chỉ:**
1. Subscribe topic của mình
2. Làm phần việc của mình
3. Publish event kết quả
4. **Không biết** service nào sẽ đọc tiếp

```
Thêm fraud-detection service vào flow?
  -> Chỉ cần subscribe "order.created"
  -> Không sửa một dòng code nào ở order-service
  -> Đây là loose coupling thực sự
```

---

### 9.4 Data Pipeline & CDC — Tự động sync dữ liệu

**CDC (Change Data Capture)** — tự động theo dõi mọi thay đổi trong DB mà không cần sửa code ứng dụng.

```
MySQL Production DB
      |
      | (Debezium đọc binlog - MySQL binary log ghi mọi thay đổi)
      v
    Kafka topic: "mysql.orders.orders_table"
      |               |               |
      v               v               v
  Elasticsearch   BigQuery        Redis Cache
  (search index)  (analytics DW)  (invalidate cache
  tự cập nhật     tự cập nhật      khi data thay đổi)
```

**Lợi ích của CDC:**
- **Zero code change** trong application — Debezium đọc binlog, không cần thêm code
- Propagate DB change trong < 1 giây
- Tất cả downstream systems sync real-time

```yaml
# Debezium Kafka Connector config
{
  "name": "mysql-orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.include.list": "orders_db",
    "table.include.list": "orders_db.orders",
    "topic.prefix": "mysql"
  }
}
```

---

### 9.5 Dead Letter Topic — Xử lý message thất bại

Kafka không có DLX tự động như RabbitMQ — phải implement thủ công nhưng linh hoạt hơn.

```
Luồng xử lý:

  "order-events"
        |
  Consumer xử lý thất bại
        |
        +-- retry < 3 --> "order-events.retry"
        |                       |
        |                 Consumer retry với delay
        |                       |
        |                 thất bại lần 4
        |                       |
        +-- retry >= 3 --> "order-events.dlt"
                                |
                          Alert / Manual review / Archive
```

```javascript
const MAX_RETRY = 3;

eachMessage: async ({ topic, message }) => {
  const retryCount = parseInt(message.headers?.['x-retry-count'] ?? '0');

  try {
    await processMessage(message);
  } catch (err) {
    if (retryCount < MAX_RETRY) {
      await producer.send({
        topic: `${topic}.retry`,
        messages: [{
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'x-retry-count': String(retryCount + 1),
            'x-original-topic': topic,
            'x-error-message': err.message,
            'x-failed-at': new Date().toISOString(),
          },
        }],
      });
    } else {
      // Đưa vào Dead Letter Topic sau khi hết retry
      await producer.send({
        topic: `${topic}.dlt`,
        messages: [{
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'x-final-error': err.message,
            'x-dead-at': new Date().toISOString(),
          },
        }],
      });
      logger.error({ message: '[DLT] Message dead lettered', topic: `${topic}.dlt` });
    }
  }
}
```

---

### 9.6 Compacted Topic — State Store

Kafka giữ **chỉ message mới nhất cho mỗi key** — hoạt động như distributed key-value store.

```
Topic "product-inventory" (cleanup.policy=compact):

Trước compaction:
  offset 0: key=P001, value={ stock: 100 }
  offset 1: key=P002, value={ stock: 50 }
  offset 2: key=P001, value={ stock: 95 }
  offset 3: key=P001, value={ stock: 90 }

Sau compaction (background process):
  offset 3: key=P001, value={ stock: 90 }  <- chỉ giữ mới nhất P001
  offset 1: key=P002, value={ stock: 50 }
```

**Dùng khi:** Cần sync state giữa các service mà không dùng shared database. Consumer mới join → đọc từ đầu → tự rebuild state hiện tại.



---

## 10. Các ông lớn dùng Kafka như thế nào

### LinkedIn — Nơi Kafka ra đời

```
Scale hiện tại:
  7 nghìn tỷ (7 trillion) message/ngày
  7,000+ broker nodes
  110,000+ topic
  trên 22 datacenter toàn cầu

Dùng cho:
  - Activity tracking (view, click, connection request)
  - Metrics & monitoring
  - Search index updates
  - Notification pipeline
  - Data pipeline: Kafka -> Hadoop cho analytics
```

**Kafka Streams trong LinkedIn — User Timeline:**

```
user.followed     -> graph service cập nhật
post.created      -> fan-out vào feed của followers
post.engaged      -> cập nhật ranking algorithm

Tất cả qua Kafka -> fan-out 10 triệu user trong < 1 giây
```

---

### Netflix — Microservice Event Bus

```
Scale: 700 tỷ event/ngày, 1,000+ topic, 200+ triệu subscribers

Dùng cho:
  - Microservice communication backbone
  - Keystone pipeline (real-time data -> Elasticsearch, Hadoop, S3)
  - Content delivery network signals
  - A/B testing và personalization
  - Phát hiện video chất lượng kém trong < 2 phút
```

**Keystone Pipeline — tại sao không dùng direct HTTP:**

```
Vấn đề nếu video-service gọi HTTP trực tiếp:
  video-service
    +-> HTTP recommendation-service  (recommendation chết -> cả luồng lỗi)
    +-> HTTP analytics-service       (analytics chậm -> video-service chờ)
    +-> HTTP cdn-service             (thêm service mới = sửa video-service)
    +-> HTTP billing-service

Với Kafka:
  video-service -> publish "video.watched" -> done
                           |
            +--------------+---------------+
            v              v               v
  recommendation-svc  analytics-svc   cdn-svc
  (tự subscribe)      (tự subscribe)  (tự subscribe)

  - video-service không biết recommendation-service tồn tại
  - Thêm A/B testing service? Chỉ cần subscribe, không sửa gì
  - analytics-service chết? video-service không hay biết
```

---

### Uber — Real-time Pricing và Fraud Detection

```
Scale: 1 triệu+ chuyến đi/ngày, toàn bộ geospatial events real-time

Use case 1 — Surge pricing (định giá động real-time):
  [Tài xế app] -> location update -> Kafka "driver-locations"
  [User app]   -> booking request -> Kafka "rider-requests"
                                           |
                              [Geofencing Stream Processor]
                              nhóm theo hex cell (H3 algorithm)
                              tính ratio demand/supply mỗi 30 giây
                              publish -> "surge-pricing-updates"
                                              |
                              [Pricing Service] cập nhật giá real-time

Use case 2 — Fraud Detection:
  "trip.completed" -> [Fraud ML Service] chạy model (< 100ms)
                             |
                    IF suspicious -> alert + suspend account
  Phát hiện gian lận trong < 30 giây (so với batch cũ: 24 giờ!)
```

---

### Airbnb — Dynamic Pricing Engine

```
Giá 1 phòng phụ thuộc vào nhiều signal real-time:
  - Số lượng booking gần đây (demand)
  - Events lớn sắp diễn ra (concert, conference)
  - Historical occupancy rate

Signal Producers:
  booking events     -> Kafka
  search events      -> Kafka
  calendar updates   -> Kafka
         |
  [Kafka Streams Aggregator]
  window 1 giờ, aggregate theo listing_id
         |
  [ML Pricing Model Consumer]
  tính giá tối ưu
  publish "pricing.recommendations"
         |
  [Listing Service] cập nhật giá
```

---

## 11. Production Guide — Cấu hình thực tế

### Topic Configuration

```bash
# Topic cho data quan trọng (order, payment)
kafka-topics.sh --create \
  --topic order-events \
  --partitions 12 \                        # 12 = scale tối đa 12 consumer instance
  --replication-factor 3 \                 # chịu được 1 broker chết
  --config retention.ms=2592000000 \       # 30 ngày
  --config min.insync.replicas=2 \
  --config compression.type=lz4

# Topic cho high-volume metrics (ít quan trọng)
kafka-topics.sh --create \
  --topic system-metrics \
  --partitions 6 \
  --replication-factor 2 \
  --config retention.ms=86400000 \         # 1 ngày
  --config retention.bytes=10737418240     # hoặc khi > 10GB
```

### Producer Configuration

```javascript
const producer = kafka.producer({
  acks: 'all',
  idempotent: true,
  retries: 10,
  maxInFlightRequests: 5,
  compression: CompressionTypes.LZ4,
  // lingerMs: 5,          // uncomment nếu OK với 5ms latency
  // batchSize: 32768,     // 32KB per batch per partition
});
```

### Consumer Configuration

```javascript
const consumer = kafka.consumer({
  groupId: 'order-processor',
  sessionTimeout: 30_000,    // 30s (mặc định 10s quá thấp)
  heartbeatInterval: 3_000,  // phải < sessionTimeout / 3
  maxPollInterval: 300_000,  // 5 phút — tăng nếu processing mỗi batch lâu
  groupInstanceId: `processor-${process.env.POD_NAME}`, // static membership
});
```

### Graceful Shutdown

```javascript
// Không shutdown đột ngột — sẽ trigger rebalance mất ~30s
const shutdown = async (signal) => {
  logger.info(`[Kafka] Graceful shutdown: ${signal}`);
  try {
    await consumer.stop();        // dừng nhận message mới
    await consumer.disconnect();  // commit pending offsets, leave group gracefully
    await producer.disconnect();  // flush pending messages
  } catch (err) {
    logger.error('[Kafka] Error during shutdown:', err);
  }
  process.exit(0);
};

['SIGTERM', 'SIGINT'].forEach(sig => process.once(sig, () => shutdown(sig)));
process.on('uncaughtException', async (err) => {
  logger.error('[Kafka] Uncaught exception:', err);
  await shutdown('uncaughtException');
});
```

### Monitoring Metrics quan trọng nhất

| Metric | Ý nghĩa | Alert khi |
|---|---|---|
| **consumer_lag** | Số message chưa xử lý | > 10,000 |
| **messages_in_per_sec** | Throughput producer | Giảm > 50% baseline |
| **under_replicated_partitions** | Partition thiếu replica | > 0 |
| **active_controller_count** | Số controller | ≠ 1 |
| **offline_partitions_count** | Partition không có leader | > 0 |
| **request_handler_idle_percent** | CPU idle broker thread | < 30% |

**Consumer lag là metric quan trọng nhất:**

```
lag = 0:       Consumer đang bắt kịp producer (healthy)
lag = 1,000:   Còn 1,000 message chưa xử lý (có thể OK)
lag = 100,000: Consumer đang tụt hậu nghiêm trọng

lag tăng liên tục -> Consumer throughput < Producer throughput -> scale out!

# Kiểm tra lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group my-group
```

### Production Checklist

- [ ] `replication-factor >= 3` cho topic quan trọng
- [ ] `acks: 'all'` + `idempotent: true` cho producer
- [ ] Tắt `autoCommit`, commit thủ công sau khi xử lý
- [ ] Consumer phải **idempotent** (at-least-once delivery có thể duplicate)
- [ ] Implement Dead Letter Topic cho message lỗi
- [ ] `retention.ms` phù hợp (tránh disk đầy)
- [ ] Monitor consumer lag với alert
- [ ] Partition count >= max consumer instance
- [ ] Graceful shutdown để tránh rebalance không cần thiết
- [ ] `groupInstanceId` (static membership) cho stable consumer instances

---

## 12. Anti-patterns & Lỗi hay gặp

### Anti-pattern 1: Dùng Kafka như database

```
SAI:
  Lưu product catalog vào Kafka -> consumer query "tìm sản phẩm theo tên"
  -> Kafka không có query language, không có index
  -> Consumer phải đọc TOÀN BỘ topic rồi filter trong memory

ĐÚNG:
  Kafka là transport layer, không phải storage
  Kafka -> consumer lưu vào DB/Elasticsearch/Redis
  Query từ DB/Elasticsearch, không từ Kafka
```

### Anti-pattern 2: Consumer không idempotent

```
SAI:
  Kafka at-least-once -> consumer CÓ THỂ nhận duplicate messages
  user.points += event.points;  // nhận 2 lần -> cộng điểm 2 lần!

ĐÚNG:
  const key = `processed:${event.messageId}`;
  if (await redis.exists(key)) return;  // skip duplicate
  user.points += event.points;
  await redis.set(key, '1', 'EX', 86400);
```

### Anti-pattern 3: Quá nhiều partitions không cần thiết

```
SAI:
  "Cứ tạo 100 partitions cho chắc"
  100 partitions x 3 replicas x 100 topics = 30,000 files trên disk
  -> Metadata overhead lớn, rebalance chậm, memory tăng

ĐÚNG:
  Tính dựa trên throughput thực tế
  Bắt đầu với 6-12 partitions
  Scale up khi có data thực tế (nhưng không thể reduce!)
```

### Anti-pattern 4: Dùng Kafka cho RPC

```
SAI:
  // order-service "hỏi" inventory có hàng không qua Kafka
  producer.send({ topic: 'inventory.check', message: { orderId } });
  // Chờ response trên reply topic... (phức tạp, latency cao)

ĐÚNG:
  Dùng gRPC hoặc REST cho synchronous request-reply
  Kafka chỉ dành cho fire-and-forget events
```

### Anti-pattern 5: Không monitor consumer lag

```
SAI:
  Deploy consumer, không setup alert cho lag

3 ngày sau: consumer lag = 5 triệu message
-> Order đặt 3 ngày trước, kho mới giảm hôm nay
-> Oversell!

ĐÚNG:
  Alert khi consumer lag > threshold phù hợp với SLA
  Grafana + Prometheus Kafka exporter
```

### Anti-pattern 6: sessionTimeout quá thấp

```
SAI:
  sessionTimeout = 10s (mặc định)
  Processing mỗi message mất 8s
  -> Consumer bị coi là dead -> rebalance liên tục!

ĐÚNG:
  sessionTimeout = 30s
  heartbeatInterval = 3s (< sessionTimeout/3)
  Gọi heartbeat() trong long-running processing
```

---

## 13. Mental Models — Cách tư duy đúng về Kafka

### Model 1: Kafka là nhật ký, không phải hộp thư

```
Hộp thư (RabbitMQ):              Nhật ký (Kafka):

[A][B][C][D]  <- queue           offset: 0   1   2   3   4
                                 msg:   [A] [B] [C] [D] [E]
Lấy A -> A bị xóa
Chỉ 1 người nhận được            ^         ^
                               Group1    Group2
                             (offset=0) (offset=2)
                             đọc độc lập, A vẫn còn
```

### Model 2: Kafka là băng chuyền, không phải kho

```
Hiểu nhầm: "Kafka giữ data lâu dài như database"

Đúng: Kafka là băng chuyền tại sân bay
  - Hành lý (event) chạy qua để được lấy đi (xử lý)
  - Sau khi hết vòng (retention), hành lý biến mất
  - Kafka mặc định: giữ 7 ngày -> tự xóa

Data "thật" nằm ở database, S3, BigQuery
Kafka chỉ là phương tiện vận chuyển giữa chúng
```

### Model 3: Event, không phải Command

```
Command (RabbitMQ phù hợp):
  "ReserveStock(orderId=123)"   <- hướng dẫn làm gì
  -> Chỉ inventory-service nhận

Event (Kafka phù hợp):
  "order.created { orderId=123 }"  <- sự thật đã xảy ra
  -> Bất kỳ service nào quan tâm đều subscribe
  -> inventory, analytics, fraud, notification, ...

Nguyên tắc: Kafka publish FACTS, không phải INSTRUCTIONS
```

### Model 4: Producer không biết Consumer tồn tại

```
RabbitMQ routing key:
  Producer biết routing key -> map đến queue cụ thể
  -> Producer gián tiếp biết ai nhận

Kafka:
  Producer chỉ biết: topic name + partition key
  Không biết có bao nhiêu consumer groups
  Không biết consumer groups đang làm gì

Thêm service mới?
  -> Chỉ cần tạo consumer group mới, subscribe topic
  -> Không sửa producer một dòng nào
  -> Đây là loose coupling thực sự
```

### Model 5: Kafka không thay thế database

| Hệ thống | Lưu gì | Bao lâu | Mục đích chính |
|---|---|---|---|
| **Kafka** | Event stream | 7–30 ngày | Real-time processing, vận chuyển |
| **PostgreSQL** | State hiện tại | Mãi mãi | App đọc/ghi hàng ngày |
| **S3 / GCS** | Raw event archive | 5–10 năm | Compliance, replay lớn |
| **BigQuery** | Structured history | 5–10 năm | Analytics, báo cáo, audit |
| **Redis** | Hot state, cache | Giờ / ngày | Truy vấn cực nhanh |

> Kafka không cạnh tranh với database hay cold storage. Nó là **lớp vận chuyển** nối tất cả hệ thống trên lại với nhau.
