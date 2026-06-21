# Redis — Interview Q&A + Bài toán thực tế

---

## Bức tranh tổng quan

```
Redis = Remote Dictionary Server
      = In-memory data store
      = Single-threaded (1 thread xử lý commands)
      = Mọi operation đều atomic

Dùng cho:
  Cache          → giảm tải DB, tăng tốc read
  Session Store  → lưu session user
  Pub/Sub        → message broker đơn giản
  Rate Limiting  → giới hạn request
  Distributed Lock → lock across multiple servers
  Leaderboard    → sorted set, ranking real-time
  Queue / Job    → list làm queue đơn giản
  Bloom Filter   → check tồn tại không cần query DB
```

---

## PHẦN 1 — DATA TYPES

### String
```redis
SET user:123:name "Thong Nguyen"
GET user:123:name
SET counter 0
INCR counter          -- atomic increment → 1
INCRBY counter 5      -- → 6
SETEX session:abc 3600 "user-data"   -- set + expire 1 giờ
```

### Hash (object/dict)
```redis
HSET user:123 name "Thong" email "thong@x.com" age 28
HGET user:123 name
HGETALL user:123
HMSET user:123 name "Thong" email "thong@x.com"   -- set nhiều field
HINCRBY user:123 age 1                              -- tăng field
```

### List (linked list — FIFO queue hoặc stack)
```redis
RPUSH queue:orders "order-1"   -- push vào cuối
RPUSH queue:orders "order-2"
LPOP queue:orders              -- pop từ đầu → "order-1" (FIFO)

LPUSH stack:history "page-1"   -- push vào đầu
LPOP stack:history             -- pop từ đầu → "page-1" (LIFO)

BLPOP queue:orders 30          -- blocking pop, chờ tối đa 30 giây
```

### Set (unique values, không thứ tự)
```redis
SADD online:users "user-1" "user-2" "user-3"
SISMEMBER online:users "user-1"   -- check tồn tại → 1
SMEMBERS online:users             -- lấy tất cả
SCARD online:users                -- đếm số phần tử

-- Phép toán tập hợp
SUNION  set1 set2   -- hợp
SINTER  set1 set2   -- giao
SDIFF   set1 set2   -- hiệu
```

### Sorted Set / ZSet (unique values + score, có thứ tự)
```redis
ZADD leaderboard 1500 "player-A"
ZADD leaderboard 2300 "player-B"
ZADD leaderboard 800  "player-C"

ZRANGE leaderboard 0 -1 WITHSCORES      -- tăng dần
ZREVRANGE leaderboard 0 2 WITHSCORES    -- top 3 giảm dần
ZRANK leaderboard "player-A"            -- vị trí (0-indexed)
ZINCRBY leaderboard 100 "player-A"      -- tăng score
```

### Bitmap
```redis
-- Dùng để track user activity rất tiết kiệm memory
-- Bit thứ userId = 1 nếu user đã login hôm nay
SETBIT active:2025-06-21 userId 1
GETBIT active:2025-06-21 userId
BITCOUNT active:2025-06-21   -- đếm user active hôm nay
```

### Stream (log/event stream — như Kafka mini)
```redis
XADD orders * orderId order-123 product IPHONE qty 1
XREAD COUNT 10 STREAMS orders 0
XGROUP CREATE orders processor-group $ MKSTREAM
```

---

## PHẦN 2 — EXPIRY & EVICTION

### TTL (Time To Live)
```redis
EXPIRE key 3600          -- hết hạn sau 3600 giây
EXPIREAT key 1718931600  -- hết hạn vào timestamp cụ thể
TTL key                  -- còn bao nhiêu giây (-1 = không expire, -2 = không tồn tại)
PERSIST key              -- xóa TTL, key tồn tại mãi
```

### Eviction Policy (khi RAM đầy)
Cấu hình trong `redis.conf`:

| Policy | Hành vi |
|---|---|
| `noeviction` | Từ chối write mới, trả lỗi (default) |
| `allkeys-lru` | Xóa key ít dùng gần nhất trong toàn bộ keys |
| `volatile-lru` | Xóa key ít dùng gần nhất trong keys có TTL |
| `allkeys-lfu` | Xóa key ít dùng nhất (frequency) |
| `volatile-ttl` | Xóa key có TTL ngắn nhất |
| `allkeys-random` | Xóa random |

```
Cache thông thường   → allkeys-lru (phổ biến nhất)
Session store        → volatile-lru (chỉ xóa key có TTL)
Không muốn mất data  → noeviction + monitor memory
```

---

## PHẦN 3 — PERSISTENCE (lưu xuống disk)

### RDB (Redis Database Snapshot)
```
Chụp toàn bộ data xuống file dump.rdb theo định kỳ
  save 900 1    → nếu có ≥1 thay đổi trong 15 phút
  save 300 10   → nếu có ≥10 thay đổi trong 5 phút

✅ File nhỏ, restore nhanh, không ảnh hưởng performance nhiều
❌ Có thể mất data giữa 2 lần snapshot (ví dụ mất điện)
```

### AOF (Append Only File)
```
Ghi mỗi write command vào file appendonly.aof

appendfsync always    → ghi ngay mỗi command (an toàn nhất, chậm nhất)
appendfsync everysec  → ghi mỗi giây (cân bằng — khuyến nghị)
appendfsync no        → OS quyết định (nhanh nhất, kém an toàn)

✅ Mất tối đa 1 giây data (everysec)
❌ File lớn hơn RDB, restore chậm hơn
```

### Kết hợp RDB + AOF
```
Dùng cả hai:
  RDB → backup nhanh, restore nhanh khi crash nặng
  AOF → minimize data loss
Redis sẽ dùng AOF để restore (đầy đủ hơn)
```

---

## PHẦN 4 — REPLICATION & CLUSTERING

### Master-Replica (Replication)
```
Master ──write──► Replica 1
       ──write──► Replica 2
                  Replica 3

Read  → Replica (scale reads)
Write → Master only
Replica bị lag → eventual consistency
```

### Redis Sentinel (High Availability)
```
Sentinel 1 ─┐
Sentinel 2 ─┼── giám sát Master
Sentinel 3 ─┘

Master chết → Sentinels vote → tự động promote Replica lên Master
Client kết nối qua Sentinel, không cần biết Master đang ở đâu
```

### Redis Cluster (Horizontal Sharding)
```
Data chia thành 16384 hash slots
  Node 1: slot 0-5460
  Node 2: slot 5461-10922
  Node 3: slot 10923-16383

Key → CRC16(key) % 16384 → đến đúng node
Tự động re-shard khi thêm/xóa node
```

---

## PHẦN 5 — BÀI TOÁN KINH ĐIỂN

### 5.1 Caching (Cache-Aside Pattern)

```javascript
// Luồng: Check cache → hit thì trả về, miss thì query DB rồi cache lại
async function getProduct(productId) {
  const cacheKey = `product:${productId}`;

  // 1. Check cache
  const cached = await redis.get(cacheKey);
  if (cached) return JSON.parse(cached);   // Cache HIT

  // 2. Cache MISS → query DB
  const product = await db.query('SELECT * FROM products WHERE id = $1', [productId]);

  // 3. Lưu vào cache, TTL 1 giờ
  await redis.setex(cacheKey, 3600, JSON.stringify(product));

  return product;
}

// Khi update DB → xóa cache (cache invalidation)
async function updateProduct(productId, data) {
  await db.update(productId, data);
  await redis.del(`product:${productId}`);  // xóa để lần sau lấy fresh data
}
```

**Cache patterns:**
```
Cache-Aside (Lazy Loading) → app tự quản lý cache, phổ biến nhất
Write-Through              → write vào cache và DB cùng lúc, luôn consistent
Write-Behind (Write-Back)  → write vào cache trước, async flush xuống DB
Read-Through               → cache tự fetch DB khi miss (cần cache library)
```

---

### 5.2 Rate Limiting (giới hạn request)

```javascript
// Sliding window rate limit: tối đa 100 req / 60 giây
async function rateLimit(userId) {
  const key = `rate:${userId}`;
  const now = Date.now();
  const window = 60 * 1000;  // 60 giây

  const pipeline = redis.pipeline();
  pipeline.zremrangebyscore(key, 0, now - window);  // xóa request cũ
  pipeline.zadd(key, now, `${now}`);                // thêm request hiện tại
  pipeline.zcard(key);                              // đếm request trong window
  pipeline.expire(key, 60);                         // TTL cleanup

  const results = await pipeline.exec();
  const count = results[2][1];

  if (count > 100) throw new Error('Rate limit exceeded');
}

// Simple fixed window (đơn giản hơn)
async function simpleRateLimit(userId) {
  const key = `rate:${userId}:${Math.floor(Date.now() / 60000)}`;  // key theo phút
  const count = await redis.incr(key);
  if (count === 1) await redis.expire(key, 60);  // set TTL lần đầu
  if (count > 100) throw new Error('Rate limit exceeded');
}
```

---

### 5.3 Distributed Lock (lock across multiple servers)

```javascript
// Vấn đề: nhiều server cùng chạy, cần đảm bảo chỉ 1 instance xử lý 1 task
// Ví dụ: cron job gửi email, chỉ 1 server được gửi

const lockKey = `lock:send-email:${jobId}`;
const lockValue = `${serverId}:${Date.now()}`;  // unique value để identify owner

// Acquire lock
const acquired = await redis.set(lockKey, lockValue, 'NX', 'EX', 30);
// NX = chỉ set nếu key CHƯA tồn tại (atomic)
// EX 30 = tự release sau 30 giây (phòng server crash)

if (!acquired) {
  console.log('Lock held by another server, skip');
  return;
}

try {
  await sendEmails();
} finally {
  // Release lock — chỉ xóa nếu MÌNH đang giữ (tránh xóa lock của server khác)
  const script = `
    if redis.call('get', KEYS[1]) == ARGV[1] then
      return redis.call('del', KEYS[1])
    else return 0 end
  `;
  await redis.eval(script, 1, lockKey, lockValue);
}
```

**Redlock** — distributed lock across 5 Redis nodes (tránh single point of failure).

---

### 5.4 Session Store

```javascript
// Login → tạo session
app.post('/login', async (req, res) => {
  const user = await authenticate(req.body);
  const sessionId = crypto.randomUUID();

  await redis.hset(`session:${sessionId}`,
    'userId', user.id,
    'email',  user.email,
    'role',   user.role
  );
  await redis.expire(`session:${sessionId}`, 86400);  // 24 giờ

  res.cookie('sessionId', sessionId, { httpOnly: true, secure: true });
  res.json({ ok: true });
});

// Middleware check session
async function authMiddleware(req, res, next) {
  const sessionId = req.cookies.sessionId;
  const session = await redis.hgetall(`session:${sessionId}`);

  if (!session?.userId) return res.status(401).json({ error: 'Unauthorized' });

  // Sliding expiry — reset TTL mỗi lần active
  await redis.expire(`session:${sessionId}`, 86400);

  req.user = session;
  next();
}
```

---

### 5.5 Leaderboard (Sorted Set)

```javascript
// Game leaderboard — top players real-time
const LEADERBOARD = 'game:leaderboard:season-1';

// Cộng điểm
await redis.zincrby(LEADERBOARD, points, userId);

// Top 10
const top10 = await redis.zrevrange(LEADERBOARD, 0, 9, 'WITHSCORES');

// Rank của user (1-indexed)
const rank = await redis.zrevrank(LEADERBOARD, userId);
const userRank = rank !== null ? rank + 1 : null;

// Score của user
const score = await redis.zscore(LEADERBOARD, userId);

// Top xung quanh user (rank -2 đến +2)
const myRank = await redis.zrevrank(LEADERBOARD, userId);
const around = await redis.zrevrange(LEADERBOARD, myRank - 2, myRank + 2, 'WITHSCORES');
```

---

### 5.6 Pub/Sub

```javascript
// Publisher (order-service)
await redis.publish('order:created', JSON.stringify({ orderId, productId }));

// Subscriber (notification-service)
const sub = redis.duplicate();  // cần connection riêng cho subscribe
await sub.subscribe('order:created');

sub.on('message', (channel, message) => {
  const event = JSON.parse(message);
  sendPushNotification(event.orderId);
});
```

**Hạn chế của Redis Pub/Sub:**
- Không persist — subscriber offline thì miss message
- Không có consumer group, replay
- **Dùng Redis Streams** nếu cần durability, hoặc dùng Kafka

---

### 5.7 Job Queue (List làm queue)

```javascript
// Producer
await redis.rpush('queue:emails', JSON.stringify({ to, subject, body }));

// Consumer (worker)
while (true) {
  const [, job] = await redis.blpop('queue:emails', 0);  // block chờ có job
  await sendEmail(JSON.parse(job));
}

// Reliable queue — dùng BRPOPLPUSH để atomic move sang processing list
const job = await redis.brpoplpush('queue:emails', 'queue:emails:processing', 0);
await processJob(job);
await redis.lrem('queue:emails:processing', 1, job);  // xóa sau khi done
```

---

### 5.8 Bloom Filter (check tồn tại — false positive, no false negative)

```
Bài toán: Kiểm tra email đã đăng ký chưa — 100 triệu email
Query DB mỗi lần → chậm
Lưu Set Redis → tốn quá nhiều memory

Bloom Filter:
  → Dùng ~1.2 GB thay vì 10 GB
  → "Không tồn tại" → chắc chắn không tồn tại ✅
  → "Tồn tại" → có thể sai (false positive ~1%) → check lại DB nếu cần
```

```javascript
// RedisBloom module
await redis.call('BF.ADD', 'emails:bloom', 'user@example.com');
const exists = await redis.call('BF.EXISTS', 'emails:bloom', 'user@example.com');
// 0 = chắc chắn không tồn tại, 1 = có thể tồn tại
```

---

## PHẦN 6 — PIPELINE & TRANSACTION

### Pipeline (batch commands, giảm network round-trip)
```javascript
// Không dùng pipeline: 3 round-trips
await redis.set('a', 1);
await redis.set('b', 2);
await redis.set('c', 3);

// Dùng pipeline: 1 round-trip gửi tất cả, nhận tất cả kết quả
const pipeline = redis.pipeline();
pipeline.set('a', 1);
pipeline.set('b', 2);
pipeline.set('c', 3);
const results = await pipeline.exec();
// Lưu ý: pipeline KHÔNG đảm bảo atomic — command khác có thể xen vào giữa
```

### Transaction (MULTI/EXEC — atomic)
```javascript
// MULTI/EXEC: tất cả commands chạy liên tiếp, không command nào xen vào
const result = await redis
  .multi()
  .incr('balance:user-1')
  .decr('balance:user-2')
  .exec();
// Nếu exec trả về null → có WATCH conflict (xem dưới)
```

### WATCH (Optimistic Locking)
```javascript
// Chuyển tiền — đảm bảo balance không thay đổi giữa chừng
async function transfer(fromId, toId, amount) {
  const fromKey = `balance:${fromId}`;

  await redis.watch(fromKey);  // watch key, nếu có ai thay đổi → transaction fail

  const balance = await redis.get(fromKey);
  if (balance < amount) throw new Error('Insufficient funds');

  const result = await redis
    .multi()
    .decrby(fromKey, amount)
    .incrby(`balance:${toId}`, amount)
    .exec();

  if (result === null) throw new Error('Transaction conflict, retry');
}
```

---

## PHẦN 7 — CÁC VẤN ĐỀ THƯỜNG GẶP

### Cache Stampede (Thundering Herd)
```
Vấn đề: 1 key hot expire → 10.000 request cùng lúc miss cache
         → 10.000 query DB cùng lúc → DB chết

Fix 1: Mutex Lock — chỉ 1 request query DB, rest chờ
Fix 2: Stale-while-revalidate — trả data cũ, async refresh cache
Fix 3: Probabilistic Early Expiration — tự refresh trước khi hết hạn
Fix 4: Jitter TTL — thêm random vào TTL để key không expire cùng lúc
         ttl = 3600 + Math.random() * 600  (3600-4200 giây)
```

### Cache Penetration (query key không tồn tại)
```
Vấn đề: query product id=-1 → cache miss → DB miss → lặp lại
         Hacker gửi 1M request với ID không tồn tại → bypass cache → DB chết

Fix 1: Cache null result
         redis.setex(`product:-1`, 60, 'NULL')  // cache luôn kết quả null
Fix 2: Bloom Filter — check tồn tại trước khi query DB
```

### Cache Avalanche (nhiều key expire cùng lúc)
```
Vấn đề: Deploy lại → cache warm-up → tất cả key cùng TTL → expire cùng lúc
         → tất cả request hit DB

Fix: Jitter TTL + gradual warm-up + circuit breaker
```

### Hot Key (1 key quá nhiều request)
```
Vấn đề: key "product:IPHONE-15" có 100k req/s
         Redis single-threaded → bottleneck

Fix 1: Local cache — cache ở application layer (in-memory Map)
Fix 2: Key sharding — "product:IPHONE-15:shard-{1..10}"
         random shard khi đọc → spread load
Fix 3: Read replica — route reads sang replica
```

---

## PHẦN 8 — CÂU HỎI INTERVIEW

### Q: Redis single-threaded nhưng tại sao vẫn nhanh?

**Trả lời:**
1. **In-memory** — không I/O disk khi read/write
2. **Single-threaded** — không context switching, không lock contention
3. **I/O Multiplexing** — 1 thread xử lý nhiều connection qua epoll/kqueue
4. **Simple data structures** — O(1) cho GET/SET
5. Throughput đạt ~100k-1M ops/second tùy hardware

---

### Q: Khi nào dùng Redis thay vì DB?

**Trả lời:**
- Data cần truy cập nhanh, chấp nhận mất nếu crash (cache)
- Data có TTL tự nhiên (session, OTP, rate limit counter)
- Data cần atomic increment (counter, leaderboard)
- Pub/Sub, ephemeral queue
- **Không dùng Redis** làm primary store khi cần ACID, complex queries, joins

---

### Q: Cache invalidation — bài toán khó nhất là gì?

**Trả lời:** Đảm bảo cache và DB **consistent** khi có write.

```
Strategies:
1. Delete on write  → đơn giản, lazy reload khi cần
2. Update on write  → có thể race condition nếu nhiều server
3. TTL-based        → accept stale data trong khoảng TTL
4. Event-driven     → DB change → event → invalidate cache (phức tạp nhất, chính xác nhất)

Race condition khi delete:
  Thread A read DB → Thread B update DB + delete cache → Thread A write stale data vào cache
  Fix: Write-through hoặc delay double delete
```

---

### Q: Distributed Lock với Redis có vấn đề gì?

**Trả lời:**

```
Vấn đề 1: Server giữ lock bị pause (GC, network) quá TTL
           → Lock expire → Server B acquire lock
           → Server A "tỉnh lại" → 2 server cùng giữ lock

Vấn đề 2: Redis Master crash trước khi sync sang Replica
           → Replica promote lên → lock bị mất
           → 2 server cùng acquire lock

Fix: Redlock — acquire lock trên 5 Redis node độc lập
     Cần majority (3/5) thành công mới tính là có lock
     Tuy nhiên Martin Kleppmann cho rằng Redlock vẫn không safe 100%
     trong distributed system với clock skew
```

---

### Q: Khác nhau giữa Redis Pub/Sub và Redis Streams?

| | Pub/Sub | Streams |
|---|---|---|
| Persist | ❌ Không | ✅ Có |
| Replay | ❌ Không | ✅ Có (từ offset) |
| Consumer group | ❌ Không | ✅ Có |
| Offline subscriber | Miss message | Đọc lại khi online |
| Dùng khi | Ephemeral broadcast | Durable event log |

---

### Q: Giải thích MULTI/EXEC vs Pipeline?

**Trả lời:**
- **Pipeline**: batch nhiều command vào 1 round-trip → giảm latency. Không atomic — command khác có thể xen vào giữa.
- **MULTI/EXEC**: transaction — tất cả command được queue lại, khi EXEC thì chạy liên tiếp, không ai xen vào. Atomic.
- Kết hợp: có thể pipeline MULTI/EXEC để vừa atomic vừa ít round-trip.

---

### Q: Memory tối ưu thế nào trong Redis?

**Trả lời:**
```
1. Chọn data type phù hợp
   Hash thay vì nhiều String:
     SET user:1:name "Thong"    → tốn nhiều overhead
     SET user:1:email "x@x.com"
     HSET user:1 name "Thong" email "x@x.com"  → ít overhead hơn

2. Đặt TTL cho mọi key cache

3. maxmemory + eviction policy phù hợp

4. Compress value lớn ở application layer (gzip JSON trước khi SET)

5. Dùng integer thay string khi có thể (INCR thay SET "123")

6. ziplist encoding cho small hash/list/zset (Redis tự optimize)
```

---

## PHẦN 9 — TỔNG KẾT USE CASES

```
Bài toán                     Data Type         Command chính
─────────────────────────────────────────────────────────────────
Cache object                 String            GET/SET/SETEX
User session                 Hash              HSET/HGETALL/EXPIRE
Rate limiting                String/ZSet       INCR hoặc ZADD+ZCARD
Distributed lock             String            SET NX EX
Leaderboard/Ranking          Sorted Set        ZADD/ZREVRANGE/ZRANK
Online users (unique)        Set               SADD/SCARD/SISMEMBER
Job queue                    List              RPUSH/BLPOP
Pub/Sub messaging            Pub/Sub           PUBLISH/SUBSCRIBE
Durable event stream         Stream            XADD/XREAD/XGROUP
Daily active users           Bitmap            SETBIT/BITCOUNT
Membership check (scale)     Bloom Filter      BF.ADD/BF.EXISTS
```
