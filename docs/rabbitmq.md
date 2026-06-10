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

---

### 2.1 Decoupling Services

**Vấn đề khi không có RabbitMQ:**

Order service cần thông báo cho nhiều service khác sau khi tạo đơn hàng. Cách đơn giản nhất là gọi HTTP tuần tự:

```
order-service
  ├──► HTTP POST inventory-service/reserve     (200ms)
  ├──► HTTP POST notification-service/send     (300ms)
  └──► HTTP POST analytics-service/record      (150ms)
  Tổng: ~650ms user phải chờ
```

**Vấn đề thực tế:**
- notification-service đang down → toàn bộ luồng đặt hàng bị lỗi
- analytics-service chậm → user phải chờ thêm 150ms vô nghĩa
- Thêm loyalty-service mới → phải sửa code order-service

**Với RabbitMQ:**

```
order-service ──publish "order.created"──► RabbitMQ   (~5ms, xong)
                                               │
                    ┌──────────────────────────┼──────────────────────┐
                    ▼                          ▼                      ▼
          inventory-service          notification-service     analytics-service
          (xử lý ngầm)               (xử lý ngầm)             (xử lý ngầm)
```

- User nhận response sau ~5ms thay vì 650ms
- notification-service down → message nằm trong queue, tự xử lý khi recover
- Thêm loyalty-service → chỉ cần subscribe, **không sửa order-service**

**Trade-off cần biết:** Bạn đánh đổi tính nhất quán tức thì. Khi order-service trả `202`, inventory chưa chắc đã trừ kho xong. Nếu hệ thống cần biết kết quả ngay → dùng HTTP sync, không dùng cách này.

---

### 2.2 Task Queue

**Bài toán:** Xử lý các công việc nặng, tốn thời gian mà không làm block HTTP request của user.

**Ví dụ thực tế — xử lý ảnh khi upload:**

```
User upload ảnh 10MB
      ↓  (request ~200ms)
[API Service] ──đẩy task──► [Queue: image-processing]   ← trả 202 ngay
                                      │
                    ┌─────────────────┼──────────────────┐
                    ▼                 ▼                  ▼
               Worker 1          Worker 2           Worker 3
               (resize)          (watermark)        (generate thumbnail)
               ~3s               ~2s                ~1s
```

User không phải chờ 3 giây. API trả về ngay, ảnh được xử lý ngầm.

**Tình huống bị block — Worker chết giữa chừng:**

```
Worker 1 nhận task "resize ảnh X"
  ↓ đang xử lý (chưa ack)
  ↓ Worker 1 crash (server mất điện)
  ↓ RabbitMQ không nhận được ack
  ↓ Sau timeout → RabbitMQ redeliver cho Worker 2
  ✅ Ảnh vẫn được xử lý, user không hay biết
```

**Tình huống task queue đầy — Traffic spike:**

```
Black Friday: 100,000 user upload ảnh cùng lúc
  ↓ API nhận request → đẩy vào queue → trả 202 ngay  (API không bị sập)
  ↓ Queue đang có 100,000 task
  ↓ 10 worker xử lý 500 task/giây
  ↓ User đầu tiên chờ ~0 giây, user cuối chờ ~200 giây
  ✅ Hệ thống không sập, tự drain queue theo tốc độ xử lý thực tế
```

Nếu không có queue, 100,000 request đồng thời → server API quá tải → **503 Service Unavailable** cho toàn bộ user.

**Khi nào Task Queue không phù hợp:**
- User cần biết kết quả ngay (ví dụ: check số dư tài khoản) → dùng HTTP sync
- Task phải xử lý theo đúng thứ tự → cần đảm bảo một queue một consumer hoặc dùng partition key

---

### 2.3 Pub/Sub (Publish/Subscribe)

**Định nghĩa:** Một event được publish một lần, **nhiều subscriber độc lập** nhận và xử lý theo cách riêng của mình.

**Ví dụ: User đăng ký tài khoản mới**

```
auth-service publish: "user.registered"
{ userId, email, name, registeredAt }
                    ↓
            [Fanout Exchange]
                    │
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
email-service   onboarding-service  analytics-service
(gửi email      (tạo profile        (ghi nhận
chào mừng)      mặc định)           new user event)
```

**Điểm quan trọng:** Ba service xử lý **song song, độc lập**. email-service không biết onboarding-service tồn tại. Nếu analytics-service down → email và onboarding vẫn chạy bình thường, analytics tự xử lý khi recover.

**So sánh với Decoupling Services:**

| | Decoupling | Pub/Sub |
|---|---|---|
| **Exchange type** | Direct hoặc Topic | Fanout |
| **Message đến** | Một service cụ thể | Tất cả subscriber |
| **Routing** | Theo routing key | Broadcast |
| **Dùng khi** | Giao việc cho đúng service | Thông báo cho nhiều service |

**Tình huống lỗi — Một subscriber xử lý fail:**

```
"user.registered" publish thành công
email-service     → ack ✅ (gửi email xong)
onboarding-service → nack ❌ (DB đang lỗi)
analytics-service  → ack ✅

→ Chỉ onboarding-service retry, hai service kia không bị ảnh hưởng
→ Mỗi service có queue riêng, lỗi không lan sang nhau
```

---

### 2.4 RPC Bất đồng bộ

**Vấn đề:** Đôi khi bạn cần kết quả từ service khác, nhưng không muốn gọi HTTP trực tiếp (vì muốn loose coupling, hoặc service đó chỉ nói chuyện qua queue).

**Cơ chế hoạt động:**

```
[Client Service]                RabbitMQ              [Server Service]
      │                              │                        │
      │── tạo reply queue tạm ──────►│                        │
      │                              │                        │
      │── publish request ──────────►│──────────────────────► │
      │   + correlationId: "abc-123" │                        │ xử lý
      │   + replyTo: "temp-queue-xyz"│                        │
      │                              │                        │
      │                              │◄── publish response ───│
      │◄── đọc từ reply queue ───────│   + correlationId: "abc-123"
      │
      │ so khớp correlationId → đây là response cho request của mình
```

**Tại sao cần `correlationId`?**

Client có thể gửi 100 request đồng thời. Tất cả response đổ về cùng một reply queue. `correlationId` là cách để client biết response nào thuộc về request nào.

**Khi nào dùng RPC qua RabbitMQ thay vì HTTP:**
- Service B không expose HTTP endpoint, chỉ lắng nghe queue
- Cần load balancing tự động giữa nhiều instance của service B
- Muốn retry tự động khi service B tạm thời down

**Tình huống bị block — Server không trả lời:**

```
Client gửi request, chờ response từ reply queue...
Server crash, không gửi response
→ Client chờ mãi → timeout

Giải pháp: Luôn đặt timeout khi chờ reply queue
```

```javascript
const response = await Promise.race([
  waitForReply(correlationId),
  new Promise((_, reject) =>
    setTimeout(() => reject(new Error('RPC timeout')), 5000)
  )
]);
```

---

### 2.5 Rate Limiting

**Bài toán:** Service downstream (database, external API) chỉ chịu được N request/giây. Nhưng traffic đến có thể gấp 10 lần con số đó.

**Ví dụ: Gọi external SMS API giới hạn 100 SMS/giây**

```
Traffic đến:  500 yêu cầu gửi SMS/giây
              ↓
        [Queue: sms-tasks]   ← buffer ở đây
              ↓
    Consumer với prefetch(1)
    + xử lý tuần tự hoặc có delay
              ↓
    SMS API: nhận đúng 100 request/giây
```

**Không có queue:**
```
500 request/giây → SMS API
SMS API rate limit → 400 request bị từ chối (HTTP 429)
→ Phải tự implement retry logic ở client
→ Phức tạp, dễ mất request
```

**Có queue:**
```
500 request/giây → Queue (buffer)
Consumer đọc với tốc độ 100/giây → SMS API
→ Queue tự làm buffer, không mất request
→ Tất cả được xử lý, chỉ delay thêm thời gian
```

**Cách điều chỉnh tốc độ consumer:**

```javascript
channel.prefetch(1); // chỉ xử lý 1 message tại một thời điểm

channel.consume('sms-tasks', async (msg) => {
  await sendSMS(JSON.parse(msg.content.toString()));
  await sleep(10); // giới hạn 100 message/giây (1000ms / 10ms = 100)
  channel.ack(msg);
});
```

**Tình huống queue bị đầy:**

Nếu traffic quá lớn kéo dài, queue tích tụ hàng triệu message → tốn bộ nhớ. Cần đặt `x-max-length` để giới hạn:

```javascript
await channel.assertQueue('sms-tasks', {
  durable: true,
  arguments: {
    'x-max-length': 100000,          // tối đa 100k message
    'x-overflow': 'reject-publish',  // từ chối publish mới khi đầy (báo lỗi về producer)
    // hoặc 'drop-head' để xóa message cũ nhất
  }
});
```

---

### 2.6 Retry & Dead Letter

**Bài toán:** Consumer xử lý message bị lỗi. Không thể bỏ qua (mất data), không thể retry ngay lập tức (có thể tạo infinite loop hoặc làm quá tải service đang lỗi).

**Các loại lỗi và cách xử lý:**

| Loại lỗi | Ví dụ | Cách xử lý |
|---|---|---|
| **Lỗi tạm thời** | DB đang restart, timeout mạng | Retry sau vài giây |
| **Lỗi vĩnh viễn** | Data sai format, business rule vi phạm | Đưa vào DLQ để xem xét thủ công |
| **Lỗi lặp lại** | Bug trong code consumer | Retry có giới hạn, sau đó vào DLQ |

**Luồng retry hoàn chỉnh:**

```
[Main Queue]
      ↓ consumer nhận message
      ↓ xử lý lỗi lần 1
      ↓ nack(requeue=false) + tăng x-retry-count lên 1
      ↓
[DLX Exchange] → [Retry Queue] (TTL = 5 giây, không có consumer)
      ↓ sau 5 giây, message hết TTL trong Retry Queue
      ↓ message vào DLX của Retry Queue = Main Queue
[Main Queue]
      ↓ consumer nhận lại message (lần 2)
      ↓ xử lý lỗi lần 2
      ↓ nack + tăng x-retry-count lên 2
      ... (lặp tối đa 3 lần)
      ↓ x-retry-count = 3 → nack vào Dead Letter Queue
[Dead Letter Queue]
      ↓ không có consumer tự động
      ↓ team kỹ thuật vào xem xét, xử lý thủ công hoặc trigger alert
```

**Tại sao không dùng `nack(requeue=true)` để retry?**

```
nack(requeue=true):
  Message về đầu queue → consumer nhận ngay lập tức → lỗi ngay lập tức
  → Vòng lặp vô hạn trong mili-giây
  → CPU 100%, queue không xử lý được message nào khác

Retry qua DLX + TTL:
  Message chờ 5 giây trong Retry Queue mới quay lại
  → Cho phép service lỗi có thời gian recover
  → Không ảnh hưởng các message khác trong Main Queue
```

**Tình huống thực tế — External payment API down:**

```
10:00:00  Payment service nhận message "process payment"
10:00:00  Gọi Stripe API → 503 Service Unavailable
10:00:00  nack(requeue=false), x-retry-count = 1
10:00:05  (sau 5s) Message quay lại Main Queue
10:00:05  Gọi Stripe API → vẫn 503
10:00:05  nack, x-retry-count = 2
10:00:10  (sau 5s) Message quay lại lần 3
10:00:10  Stripe đã recover → xử lý thành công ✅
          channel.ack(msg)
```

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
[Producer] ──publish──► │  [Exchange] ──binding──► [Queue]     │ ──deliver──► [Consumer]
                        │                                      │
                        └─────────────────────────────────────┘
```

### 4.1 Producer & Consumer

**Producer** là bất kỳ ứng dụng nào **tạo ra và gửi message**. Producer chỉ biết đến **Exchange** — nó không biết, không quan tâm, và không cần biết có bao nhiêu queue đang chờ nhận, hay ai sẽ xử lý message đó.

**Consumer** là bất kỳ ứng dụng nào **lắng nghe và xử lý message từ Queue**. Consumer không biết message đến từ đâu, do service nào tạo ra.

Sự tách biệt này là **mục đích cốt lõi của RabbitMQ**: Producer và Consumer không phụ thuộc vào nhau. Bạn có thể tắt Consumer đi, Producer vẫn gửi bình thường — message chờ trong Queue. Bạn có thể thêm Consumer mới mà không cần sửa một dòng code nào ở Producer.

```
Producer chỉ biết:  "Tôi publish lên exchange tên X"
Consumer chỉ biết:  "Tôi lắng nghe queue tên Y"
RabbitMQ chịu trách nhiệm kết nối X với Y
```

### 4.2 Queue

Queue là **hàng đợi lưu trữ message** cho đến khi có consumer xử lý. Đây là nơi message thực sự "nằm chờ" — Exchange không lưu gì cả, chỉ Queue mới lưu.

Một Queue hoạt động theo nguyên tắc **FIFO** (First In, First Out) — message vào trước ra trước. Khi consumer xử lý xong và gửi `ack`, message mới bị xóa khỏi Queue vĩnh viễn.

> **Điểm quan trọng:** Nhiều consumer có thể cùng lắng nghe một Queue — khi đó RabbitMQ phân phối message lần lượt cho từng consumer (round-robin), mỗi message chỉ đến **một** consumer duy nhất. Đây là cơ chế **Work Queue / Load Balancing** tự nhiên.

**Các thuộc tính khai báo:**
```
Queue properties:
  - name        : tên queue (unique trong vhost)
  - durable     : true → tồn tại sau khi broker restart (lưu xuống disk)
                  false → mất khi RabbitMQ restart (chỉ dùng cho dev/test)
  - exclusive   : true → chỉ connection hiện tại dùng được, tự xóa khi connection đóng
                  (dùng cho temporary queue trong pattern RPC)
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

**Câu hỏi hợp lý:** *Tại sao không để Producer publish thẳng vào Queue? Tại sao cần thêm Exchange vào giữa?*

**Trả lời:** Nếu publish thẳng vào Queue, Producer phải biết tên Queue. Khi hệ thống lớn lên, thêm một service mới đồng nghĩa phải sửa code Producer để gửi thêm vào Queue mới — **coupling chặt**.

Exchange giải quyết điều này: Producer chỉ publish lên Exchange với một **routing key**, Exchange tự quyết định chuyển vào Queue nào dựa trên **binding rules**. Thêm Queue mới chỉ cần tạo binding mới — Producer không cần biết, không cần sửa.

```
Không có Exchange:                     Có Exchange:
Producer ──► Queue A  (biết tên A)     Producer ──► Exchange ──► Queue A
Producer ──► Queue B  (biết tên B)                          ──► Queue B
Producer ──► Queue C  (biết tên C)                          ──► Queue C (thêm mới, ko sửa Producer)
```

Exchange là **bộ định tuyến** — nhận message từ Producer và quyết định chuyển vào Queue nào dựa trên **type** và **routing key**.

> **Quan trọng:** Exchange **không lưu message**. Nó chỉ định tuyến rồi chuyển đi ngay. Nếu không có queue nào phù hợp, message bị **dropped** — mất vĩnh viễn (hoặc trả lại Producer nếu set `mandatory: true`).

### 4.4 Binding & Routing Key

Đây là hai khái niệm hay bị nhầm lẫn vì cùng mang giá trị chuỗi tương tự nhau.

**Routing Key** là **nhãn do Producer gắn vào message** khi publish. Nó mô tả *message này là gì* hoặc *message này đến từ đâu*.

**Binding Key** là **rule do admin cấu hình** khi nối Exchange với Queue. Nó mô tả *Queue này muốn nhận loại message nào*.

Exchange so khớp **Routing Key của message** với **Binding Key của từng binding** để quyết định chuyển vào Queue nào.

```
                    Producer gắn vào message:
                    routing_key = "order.created"
                                ↓
                          Exchange
                    so khớp với các binding:
                                │
          ┌─────────────────────┼──────────────────────┐
          ▼                     ▼                      ▼
binding "order.created"  binding "order.shipped"  binding "user.*"
→ Queue inventory ✅      → Queue shipping ❌       → Queue user ❌
(khớp)                    (không khớp)              (không khớp)
```

> **Binding** là hành động nối Queue vào Exchange, kèm theo Binding Key. Một Queue có thể bind vào nhiều Exchange. Một Exchange có thể bind tới nhiều Queue.

### 4.5 Channel & Connection

**Connection** là một **TCP connection thực sự** giữa ứng dụng và RabbitMQ broker. Tạo TCP connection tốn kém — phải bắt tay 3 bước (TCP handshake), xác thực AMQP, tốn bộ nhớ ở cả hai đầu. Vì vậy mỗi ứng dụng chỉ nên tạo **một Connection duy nhất** và giữ mãi.

**Channel** là **kênh ảo (virtual channel) được multiplexed bên trong một Connection**. Nhiều Channel chia sẻ cùng một TCP connection thực, nhưng hoạt động độc lập với nhau.

*Tại sao cần Channel thay vì dùng Connection trực tiếp?*

Vì các thao tác AMQP (publish, consume, ack) cần được cô lập. Nếu dùng cùng một kênh cho cả publish lẫn consume, các message có thể chen nhau gây lỗi. Channel giúp mỗi luồng công việc có một kênh riêng mà không cần tạo thêm TCP connection.

```
TCP Connection (1 cái duy nhất)
  ├── Channel 1 → dùng để PUBLISH (order-service gửi event)
  ├── Channel 2 → dùng để CONSUME (inventory-service nhận event)
  └── Channel 3 → dùng cho thao tác admin (assertQueue, assertExchange)
```

```javascript
// Best practice: 1 connection, nhiều channel
const connection = await amqp.connect('amqp://localhost');
const publishChannel  = await connection.createChannel(); // dùng để publish
const consumeChannel  = await connection.createChannel(); // dùng để consume
```

> **Lưu ý thực tế:** Khi Channel bị lỗi (ví dụ nack một message sai cách), Channel đó bị đóng nhưng Connection vẫn sống. Bạn chỉ cần tạo Channel mới, không mất Connection.

### 4.6 Virtual Host (vhost)

**Bài toán thực tế:** Bạn có 1 server RabbitMQ. Bạn muốn dùng nó cho cả môi trường `production` và `staging` mà không để chúng can thiệp vào nhau. Bạn cũng muốn team A và team B dùng chung server nhưng không thấy queue của nhau.

**vhost** (Virtual Host) giải quyết điều này — nó là một **vùng cô lập hoàn toàn** bên trong RabbitMQ. Mỗi vhost có tập Exchange, Queue, Binding, và phân quyền User riêng biệt. Một Exchange tên `order_events` trong vhost `production` và Exchange tên `order_events` trong vhost `staging` là **hai thực thể hoàn toàn khác nhau**, không liên quan gì đến nhau.

```
RabbitMQ Server (1 instance)
  ├── vhost: /production
  │     ├── exchange: order_events
  │     ├── queue: inventory_queue
  │     └── user: prod_user (chỉ có quyền ở đây)
  │
  ├── vhost: /staging
  │     ├── exchange: order_events  ← cùng tên nhưng hoàn toàn độc lập
  │     ├── queue: inventory_queue
  │     └── user: dev_user (chỉ có quyền ở đây)
  │
  └── vhost: /   ← vhost mặc định (dùng cho dev local)
```

```
amqp://user:pass@localhost:5672/production   ← kết nối vào vhost "production"
amqp://user:pass@localhost:5672/staging      ← kết nối vào vhost "staging"
amqp://guest:guest@localhost:5672            ← kết nối vào vhost "/" (mặc định)
```

### 4.7 Message & Properties

Mỗi message trong RabbitMQ gồm hai phần: **body** (nội dung thực) và **properties** (metadata đi kèm). Properties không phải là data nghiệp vụ — chúng là thông tin để RabbitMQ và consumer biết cách xử lý message.

| Property | Ý nghĩa | Khi nào dùng |
|---|---|---|
| `persistent` | `true` → lưu xuống disk | Message quan trọng, không muốn mất khi broker restart |
| `contentType` | Loại nội dung | Giúp consumer biết cách parse (json, xml, ...) |
| `messageId` | ID duy nhất của message | Deduplicate — tránh xử lý 2 lần cùng message |
| `timestamp` | Thời điểm tạo message | Debug, audit, tính age của message |
| `expiration` | TTL của message (ms, dạng string) | Message tự hết hạn nếu chưa được xử lý |
| `headers` | Key-value tùy ý | Truyền metadata như trace_id, retry_count |
| `correlationId` | ID để ghép request-response | Dùng trong pattern RPC |
| `replyTo` | Tên queue để trả lời | Dùng trong pattern RPC |

```javascript
channel.publish(exchange, routingKey, content, {
  persistent: true,           // lưu xuống disk (cần durable queue để có hiệu lực)
  contentType: 'application/json',
  messageId: randomUUID(),    // ID duy nhất, consumer dùng để check duplicate
  timestamp: Date.now(),
  expiration: '60000',        // message tự vào DLX sau 60s nếu chưa được consume
  headers: {
    'x-trace-id': traceId,    // theo dõi request xuyên suốt các service
    'x-retry-count': 0        // đếm số lần đã retry
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

**Tại sao cần Acknowledgement?**

Khi consumer nhận message, RabbitMQ **không xóa message ngay** — nó chỉ đánh dấu là "đang được xử lý" (unacknowledged). Message chỉ bị xóa khỏi Queue khi consumer gửi `ack` xác nhận xử lý xong.

*Nếu consumer chết giữa chừng (crash, mất điện, network lỗi) mà chưa kịp `ack`?* → RabbitMQ tự động đưa message về lại Queue và giao cho consumer khác. **Không mất message.**

```
Consumer nhận message
        ↓
  [Đang xử lý...]
        ↓
  ┌─────────────────────────────────────────────────┐
  │ Thành công → channel.ack(msg)                   │
  │   → RabbitMQ xóa message khỏi Queue vĩnh viễn  │
  │                                                 │
  │ Thất bại → channel.nack(msg, false, true)       │
  │   requeue=true → message về lại Queue           │
  │   requeue=false → message vào DLX (nếu có)      │
  │                                                 │
  │ Consumer chết (không gửi gì cả)                 │
  │   → RabbitMQ tự redeliver sau khi detect chết   │
  └─────────────────────────────────────────────────┘
```

**4 chế độ:**

#### `ack` — Xử lý thành công, xóa message khỏi queue
```javascript
channel.consume('orders', (msg) => {
  try {
    processOrder(JSON.parse(msg.content));
    channel.ack(msg); // ✅ báo RabbitMQ: xong rồi, xóa đi
  } catch (err) {
    channel.nack(msg, false, true); // ❌ thất bại, requeue = true → thử lại
  }
});
```

#### `nack` với requeue=true — thất bại, thử lại
```javascript
channel.nack(msg,
  false,   // multiple: false = chỉ nack message này, không phải tất cả unacked
  true     // requeue: true = đưa về đầu Queue để consumer khác thử lại
);
// ⚠️ Cẩn thận: nếu message luôn lỗi → infinite loop, consumer cứ nhận rồi nack mãi
```

#### `nack` với requeue=false — từ bỏ, đẩy sang DLX
```javascript
channel.nack(msg, false, false);
// Message không về Queue nữa → vào DLX nếu được cấu hình, mất hẳn nếu không có DLX
```

#### `noAck` mode — RabbitMQ xóa message ngay khi giao, không chờ confirm
```javascript
channel.consume('queue', handler, { noAck: true });
// Nhanh nhất nhưng nguy hiểm:
// Consumer nhận message → crash trước khi xử lý xong → message mất vĩnh viễn
// Chỉ dùng cho data không quan trọng (metrics, log)
```

---

## 7. Dead Letter Exchange (DLX)

**Bài toán:** Consumer xử lý một message nhưng liên tục thất bại — lỗi data sai format, service phụ thuộc đang down, bug trong code. Nếu dùng `nack(requeue=true)`, message quay về Queue và consumer lại nhận ngay, lại lỗi, lại nack — **infinite loop**, CPU chạy 100%, Queue không xử lý được gì khác.

Nếu dùng `nack(requeue=false)` và không có DLX — message **mất hẳn**, không có cách nào audit hay retry sau.

**DLX (Dead Letter Exchange)** là giải pháp: message "chết" (bị từ chối hoặc hết hạn) thay vì mất đi, được **chuyển sang một Exchange khác** để xử lý riêng — retry sau một khoảng thời gian, gửi alert, hoặc lưu để audit.

```
[Main Queue] → consumer xử lý lỗi
                    ↓
             nack(requeue=false)
                    ↓
             [DLX Exchange]    ← "nghĩa địa" message
                    ↓
             [Dead Letter Queue]
               (lưu để xem lại, retry, hoặc alert)
```

### Các trường hợp message tự vào DLX:
1. `nack` hoặc `reject` với `requeue = false`
2. Message hết TTL (`x-message-ttl`) — chờ quá lâu trong Queue
3. Queue đầy (`x-max-length`) — message mới đẩy message cũ vào DLX

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

**Cơ chế mặc định của RabbitMQ khi không set Prefetch:**

RabbitMQ sẽ đẩy **tất cả message trong Queue** vào consumer ngay lập tức, lưu chúng trong buffer của consumer (unacknowledged). Consumer A có thể đang ôm 1000 message trong khi Consumer B idle không có việc làm.

**Prefetch** (hay QoS — Quality of Service) giới hạn: *"RabbitMQ chỉ được gửi tối đa N message cho consumer này khi chưa nhận được ack."* Khi consumer đã ôm đủ N message chưa ack, RabbitMQ dừng gửi thêm cho consumer đó — và gửi cho consumer khác thay thế.

```javascript
await channel.prefetch(1);
// Consumer này tối đa chỉ có 1 message "trên tay" tại một thời điểm
// Xử lý xong (ack) → RabbitMQ mới gửi cái tiếp theo
```

### Tại sao quan trọng?

```
Không có prefetch (mặc định):
  Queue: [m1][m2][m3]...[m100]
  Consumer A ← nhận hết 100 message (đang xử lý chậm vì I/O nặng)
  Consumer B ← idle, không có việc dù Consumer A đang bận
  → Mất cân bằng tải

Với prefetch(1):
  Queue: [m1][m2][m3]...[m100]
  Consumer A ← nhận m1, đang xử lý
  Consumer B ← nhận m2, đang xử lý song song
  Consumer A xử lý xong m1 → ack → nhận m3
  Consumer B xử lý xong m2 → ack → nhận m4
  → Load phân phối đều theo tốc độ thực của từng consumer
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
