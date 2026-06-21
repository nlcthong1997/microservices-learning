# Streaming nội dung cho hàng triệu người — Không sập server

> Tại sao xem bóng đá trực tiếp trên internet mượt mà dù hàng triệu người cùng xem? Server ở đâu đủ băng thông? Tài liệu này giải thích toàn bộ kiến trúc — từ camera ở sân vận động đến màn hình điện thoại của bạn.

---

## Mục lục

1. [Vấn đề cốt lõi — Tại sao 1 server không đủ?](#1-vấn-đề-cốt-lõi--tại-sao-1-server-không-đủ)
2. [CDN — Giải pháp nền tảng](#2-cdn--giải-pháp-nền-tảng)
3. [HLS — HTTP Live Streaming (cách các site bóng đá hoạt động)](#3-hls--http-live-streaming)
4. [Pipeline đầy đủ: Camera → Màn hình](#4-pipeline-đầy-đủ-camera--màn-hình)
5. [Adaptive Bitrate Streaming](#5-adaptive-bitrate-streaming)
6. [WebRTC — Ultra-low latency (dưới 1 giây)](#6-webrtc--ultra-low-latency)
7. [Tại sao WebSocket không scale cho video streaming?](#7-tại-sao-websocket-không-scale-cho-video-streaming)
8. [Redis Pub/Sub — Scale WebSocket ngang](#8-redis-pubsub--scale-websocket-ngang)
9. [Kiến trúc thực tế — Twitch, YouTube Live, Shopee Live](#9-kiến-trúc-thực-tế)
10. [Các vấn đề phức tạp trong production](#10-các-vấn-đề-phức-tạp-trong-production)
11. [Bảng so sánh các protocol streaming](#11-bảng-so-sánh-các-protocol-streaming)

---

## 1. Vấn đề cốt lõi — Tại sao 1 server không đủ?

Tưởng tượng World Cup chung kết: **50 triệu người** cùng xem online.

```
Mỗi stream 1080p HD = ~5 Mbps

50,000,000 người × 5 Mbps = 250,000,000 Mbps = 250 Tbps

Bandwidth record của toàn bộ internet backbone ~500 Tbps
→ 1 trận bóng chiếm nửa bandwidth của internet!
```

**1 server vật lý không thể:**

```
Bandwidth:     1 server tốt nhất = 10 Gbps uplink
               Cần:               250,000,000 Mbps
               → cần 25,000 server chỉ để giải quyết bandwidth

RAM:           1 connection TCP = ~50KB state
               50 triệu connections × 50KB = 2.5 TB RAM
               → không server nào chứa được

CPU:           Encode video real-time cực kỳ nặng
               1 stream 1080p 30fps ≈ 1 CPU core
               → không khả thi cho hàng triệu người

Latency:       Server ở US, viewer ở VN
               Round-trip ~200ms → lag không xem được
```

**Giải pháp: Không để server trực tiếp phục vụ viewer — phân tán data ra edge.**

---

## 2. CDN — Giải pháp nền tảng

**CDN (Content Delivery Network)** = mạng lưới hàng nghìn server đặt ở khắp nơi trên thế giới.

```
Không có CDN:
  Viewer Hà Nội ────────────────────────────────► Origin Server (US)
                         ~200ms latency
                         tất cả traffic dồn vào 1 điểm

Có CDN:
  Viewer Hà Nội ────────► CDN Edge Singapore (~10ms)
  Viewer TP.HCM ────────► CDN Edge Singapore (~15ms)
  Viewer Jakarta ───────► CDN Edge Singapore (~20ms)
  Viewer Tokyo ─────────► CDN Edge Tokyo     (~5ms)

  CDN Edge chỉ hỏi Origin lần đầu → cache → phục vụ local
```

### Caching hoạt động thế nào với streaming?

```
Static content (dễ cache):
  video.mp4, image.jpg, bundle.js
  → CDN cache 24 giờ → 100% requests served từ edge

Live streaming (cache từng segment nhỏ):
  Origin server tạo ra liên tục:
    segment_480.ts (giây 0-6)    ← đã cũ → cache lâu
    segment_481.ts (giây 6-12)   ← đã cũ → cache lâu
    segment_482.ts (giây 12-18)  ← mới nhất → cache ngắn (6s)
    playlist.m3u8                ← index file → cache rất ngắn (2s)

  1,000 viewers cùng xem:
    → CDN nhận 1,000 requests cho segment_482.ts
    → CDN hỏi Origin 1 lần, serve 999 từ cache
    → Origin chỉ nhận ~1/1000 requests!
```

### CDN Origin Shield

```
Thông thường (segment mới vừa tạo):
  Edge Singapore ──miss──► Origin (US)
  Edge Tokyo     ──miss──► Origin (US)   ← cả 3 cùng miss = thundering herd
  Edge Frankfurt ──miss──► Origin (US)

Origin Shield (thêm 1 cache layer trung gian):
  Edge Singapore ──miss──┐
  Edge Tokyo     ──miss──┼──► Shield (Tokyo) ──miss──► Origin (US)
  Edge Frankfurt ──miss──┘          │
                                 cache
                                    │
                         ──hit──► Edge (tất cả)

  → Chỉ 1 request tới Origin, Shield phục vụ các edge khác
```

**CDN providers phổ biến:**

| Provider | Đặc điểm | Dùng cho |
|---|---|---|
| Cloudflare | Free tier, DDoS protection tốt | Startup, web app |
| AWS CloudFront | Tích hợp tốt với AWS | AWS-native stack |
| Akamai | Lớn nhất thế giới, enterprise | Sự kiện lớn, broadcast |
| Fastly | Low latency, real-time purge | Dynamic content |
| BunnyCDN | Rẻ, tốt cho video | Video hosting |

---

## 3. HLS — HTTP Live Streaming

**HLS** do Apple phát minh (2009), giờ là standard phổ biến nhất cho live streaming.

**Ý tưởng cốt lõi:** Chia video thành các segment nhỏ (2-10 giây) → phục vụ qua HTTP thông thường.

```
Streaming protocol cũ (RTSP, RTMP tới viewer):
  Server duy trì 1 TCP connection liên tục per viewer
  → 1 triệu viewer = 1 triệu TCP connections đang mở = server sập

HLS:
  Mỗi 6 giây viewer gửi HTTP GET mới: "cho tôi segment tiếp theo"
  → Stateless! Server không nhớ ai đang xem
  → CDN cache segment → scale vô hạn
```

### Cấu trúc HLS

```
Playlist file (playlist.m3u8) — viewer download mỗi 6 giây:
  #EXTM3U
  #EXT-X-VERSION:3
  #EXT-X-TARGETDURATION:6
  #EXT-X-MEDIA-SEQUENCE:483

  #EXTINF:6.006,
  https://cdn.example.com/live/segment_480.ts    ← 30 giây trước
  #EXTINF:6.006,
  https://cdn.example.com/live/segment_481.ts
  #EXTINF:6.006,
  https://cdn.example.com/live/segment_482.ts    ← mới nhất

Segment file (segment_482.ts):
  MPEG-TS container
  Video: H.264 hoặc H.265
  Audio: AAC
  Duration: 6 giây

Viewer player:
  1. Download playlist.m3u8 mỗi 6 giây
  2. So sánh với lần trước → segment nào mới
  3. Download segment mới → decode → phát
  4. Buffer 3 segment (~18 giây) trước để tránh lag
```

### Latency của HLS

```
Camera → encode → cắt segment (6s) → upload → CDN → player buffer (18s)
= 25-45 giây latency

→ Đây là lý do bạn biết bàn thắng qua điện thoại trước khi thấy trên stream!

Low-Latency HLS (Apple 2019):
  - Segment nhỏ hơn: 0.5-1 giây
  - HTTP/2 push: server push segment trước khi player hỏi
  - Partial segment: player download phần đầu segment trong khi phần sau đang encode
  → Latency: 2-5 giây

DASH (MPEG-DASH) — alternative:
  - Cơ chế tương tự HLS nhưng open standard
  - Dùng .mpd file thay .m3u8, fMP4 thay MPEG-TS
  - YouTube, Netflix dùng DASH
```

---

## 4. Pipeline đầy đủ: Camera → Màn hình

```
═══════════════════════════════════════════════════════════════
  SÂN VẬN ĐỘNG
═══════════════════════════════════════════════════════════════
Camera → Encoder cứng/OBS
              │
              │ RTMP   rtmp://ingest.platform.com/live/KEY
              ▼
═══════════════════════════════════════════════════════════════
  INGEST LAYER   (ít server, chỉ nhận input)
═══════════════════════════════════════════════════════════════
Ingest Server (nhận RTMP, forward tới transcoding)
              │
              │ internal
              ▼
═══════════════════════════════════════════════════════════════
  TRANSCODING FARM   (nhiều server GPU/CPU)
═══════════════════════════════════════════════════════════════
  Worker 1 → 1080p 60fps  8.0 Mbps  H.264
  Worker 2 → 720p  30fps  3.0 Mbps  H.264
  Worker 3 → 480p  30fps  1.5 Mbps  H.264
  Worker 4 → 360p  30fps  0.8 Mbps  H.264
  Worker 5 → audio only   128 kbps  AAC
              │
              │ upload segment files mỗi 6 giây
              ▼
═══════════════════════════════════════════════════════════════
  STORAGE   (S3 / GCS — vô hạn, tự scale)
═══════════════════════════════════════════════════════════════
  segment_480.ts (1080p), segment_480_720p.ts, ...
  playlist.m3u8  (cập nhật mỗi 6 giây)
              │
              │ CDN origin pull
              ▼
═══════════════════════════════════════════════════════════════
  CDN NETWORK   (hàng nghìn edge server toàn cầu)
═══════════════════════════════════════════════════════════════
  Edge Singapore ──► Viewer Hà Nội, TP.HCM, Jakarta
  Edge Tokyo     ──► Viewer Japan, Korea
  Edge Frankfurt ──► Viewer Europe
  Edge São Paulo ──► Viewer Brazil
              │
              ▼
═══════════════════════════════════════════════════════════════
  VIEWER PLAYER   (browser, iOS app, Android app, Smart TV)
═══════════════════════════════════════════════════════════════
  1. Load master playlist → biết các chất lượng available
  2. Đo bandwidth → chọn chất lượng phù hợp
  3. Download segment mới mỗi 6 giây từ CDN gần nhất
  4. Decode H.264/H.265 → render → hiển thị
```

### RTMP — Real-Time Messaging Protocol

```
RTMP: protocol để STREAMER đẩy video lên server (không phải cho viewer)
  - Dùng TCP, persistent connection
  - OBS → RTMP → Ingest Server
  - Twitch:   rtmp://live.twitch.tv/live/YOUR_KEY
  - YouTube:  rtmp://a.rtmp.youtube.com/live2/YOUR_KEY

Tại sao không dùng RTMP cho viewer?
  - Cần TCP connection duy trì → không scale
  - Flash plugin đã chết từ 2020 → browser không hỗ trợ
  - CDN không cache RTMP stream

Vì vậy:
  RTMP → Ingest (nhận từ streamer)
  HLS  → CDN → Viewer (phân phối)
```

---

## 5. Adaptive Bitrate Streaming

Đây là lý do Netflix không buffer khi mạng yếu — tự động giảm chất lượng:

```
Master Playlist (stream.m3u8):
  #EXTM3U

  #EXT-X-STREAM-INF:BANDWIDTH=8000000,RESOLUTION=1920x1080
  1080p/playlist.m3u8

  #EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720
  720p/playlist.m3u8

  #EXT-X-STREAM-INF:BANDWIDTH=1500000,RESOLUTION=854x480
  480p/playlist.m3u8

  #EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360
  360p/playlist.m3u8
```

**ABR algorithm (đơn giản):**

```
Player đo bandwidth sau mỗi segment download:
  bandwidth > 8 Mbps  → switch lên 1080p
  3 - 8 Mbps          → dùng 720p
  1.5 - 3 Mbps        → dùng 480p
  < 1.5 Mbps          → dùng 360p

  bandwidth giảm đột ngột → giảm ngay
  bandwidth tăng → tăng chậm (conservative, tránh oscillation)
```

**ABR nâng cao — Buffer-based:**

```
Nhìn vào buffer hiện tại (số giây video đã download nhưng chưa phát):
  buffer > 30s  → tăng chất lượng (có dự phòng nhiều)
  buffer 10-30s → giữ nguyên
  buffer < 10s  → giảm chất lượng
  buffer < 5s   → giảm gấp 2 bậc (nguy cơ rebuffer)

→ Ổn định hơn pure throughput-based (ít switching hơn)
→ Netflix BOLA: kết hợp buffer + throughput + utility function
```

---

## 6. WebRTC — Ultra-low latency

HLS latency 5-45 giây — quá chậm cho video call, live auction, interactive stream.

**WebRTC** giải quyết điều này:

```
Latency so sánh:
  HLS standard:   25-45 giây
  LLHLS:          2-5 giây
  DASH low-lat:   3-7 giây
  WebRTC:         < 500ms  (nửa giây!)

Tại sao WebRTC nhanh vậy?
  - UDP thay vì TCP: không retransmit gói mất → ưu tiên speed over reliability
  - Peer-to-peer: không qua server trung gian (ideal)
  - SRTP (Secure RTP): stream trực tiếp, không buffer
  - Bỏ qua gói mất nhỏ: chấp nhận artifact nhỏ thay vì chờ retransmit

WebRTC P2P flow:
  Browser A ──Offer SDP──► Signaling Server ──► Browser B
  Browser B ──Answer SDP─► Signaling Server ──► Browser A
  [ICE: tìm đường kết nối P2P qua STUN/TURN]
  Browser A ◄══════ UDP stream (SRTP) ════════► Browser B
  (P2P trực tiếp sau khi setup, không qua server)

Các thành phần:
  SDP:  Session Description Protocol — mô tả codec, IP, port
  ICE:  Interactive Connectivity Establishment — tìm đường P2P
  STUN: giúp client biết public IP của mình (NAT traversal)
  TURN: relay server khi P2P không được (corporate firewall)
```

**WebRTC cho 1 → nhiều người — SFU:**

```
P2P không scale cho live streaming (1 streamer → 1000 viewer):
  Streamer phải gửi 1000 streams riêng → upload bandwidth cạn

SFU (Selective Forwarding Unit) — server trung gian:
  Streamer ──WebRTC──► SFU ──WebRTC──► Viewer 1
                           ──WebRTC──► Viewer 2
                           ──WebRTC──► Viewer 3 ...

  SFU chỉ FORWARD packet (không decode/re-encode) → ít CPU
  Mỗi SFU instance handle ~1,000 concurrent viewers
  Scale: cluster nhiều SFU instances

  Dùng bởi: Discord, Google Meet, LiveKit, Mediasoup

Khi nào dùng WebRTC vs HLS?
  WebRTC: video call, live auction, interactive, < 500ms latency
  HLS:    1 → triệu người, CDN, bóng đá, concert
```

---

## 7. Tại sao WebSocket không scale cho video streaming?

```
WebSocket — thiết kế cho messages nhỏ, bidirectional:
  Connection state: server RAM cho mỗi WS connection
  1,000,000 viewers × ~50KB state = 50 GB RAM chỉ để giữ connections
  
  Node.js tối đa: ~100,000 WS connections/server
  → Cần 10 server chỉ để giữ connections, chưa tính bandwidth

HLS/HTTP — stateless:
  Viewer request segment HTTP GET mỗi 6 giây
  Server trả file, đóng connection ngay (hoặc HTTP keep-alive ngắn)
  ZERO state giữa các requests
  CDN cache → 99% không tới origin
  Origin chỉ cần handle ~1% × 1,000,000 = 10,000 requests/6s = 1,667 RPS
  → 1 server Node.js handle thoải mái

WebSocket phù hợp cho:
  ✅ Chat, notification, game state (messages nhỏ)
  ✅ Typing indicator, presence
  ❌ Video streaming (binary lớn, không cache được)
```

**WebSocket VẪN được dùng trong live streaming — nhưng cho layer khác:**

```
Streaming platform dùng 2 hệ thống song song:

  Video:       HLS/DASH → CDN ────────────────────► Viewer
                                                      │
  Metadata:    WebSocket Server ──────────────────► Viewer
                    │
              ┌─────┼──────┐
          Live chat  Reactions  Viewer count
         (tin nhắn) (tim/clap)  (số người xem)

→ Video đi qua CDN (scale vô hạn)
→ Chat/reaction đi qua WebSocket (cần realtime, nhỏ hơn nhiều)
```

---

## 8. Redis Pub/Sub — Scale WebSocket ngang

**Vấn đề khi scale WebSocket server:**

```
Không có Redis:
  User A ──WS──► Server 1  (room: "match-vn-thai")
  User B ──WS──► Server 2  (room: "match-vn-thai")

  User A gửi comment → Server 1 broadcast tới room
  → Server 2 không biết → User B không nhận được!

Với Redis Pub/Sub:
  User A ──WS──► Server 1 ──PUBLISH──► Redis channel "match-vn-thai"
  User B ──WS──► Server 2
                Server 2 ──SUBSCRIBE─► Redis channel "match-vn-thai"

  User A gửi → Server 1 → Redis PUBLISH → Server 2 nhận → User B nhận ✅
  → Tất cả server instances "nói chuyện" qua Redis
```

```
Scale-out architecture:

  Load Balancer (sticky session)
        │
        ├──► Chat Server 1 ──┐
        ├──► Chat Server 2 ──┼──► Redis Pub/Sub ←── Kafka Bridge
        ├──► Chat Server 3 ──┤         (1 Redis cluster)
        └──► Chat Server 4 ──┘

Sticky session: LB gửi cùng user về cùng server
→ Tránh reconnect liên tục
→ Nếu server đó down → user reconnect → LB chuyển sang server khác OK

Socket.IO: socket.io-redis adapter làm tự động
  io.adapter(createAdapter(redisClient));
  // Từ đây: chatNS.to('match-vn-thai').emit('comment', data)
  // Tự động fan-out qua Redis tới tất cả server instances
```

---

## 9. Kiến trúc thực tế

### Twitch

```
Streamer (OBS) ──RTMP──► Ingest Edge (~20 địa điểm toàn cầu, gần streamer)
                               │
                         Transcoding (AWS EC2 GPU)
                          → 1080p60, 720p60, 480p30, 360p30
                               │
                         S3 (segment storage)
                               │
                         CloudFront CDN (~500 Points of Presence)
                               │
                         Viewer Player (HLS)

Chat system:
  Viewer ──WebSocket──► Chat Server (IRC over WebSocket)
                               │
                         Redis Pub/Sub (fan-out tới tất cả servers)
                               │
                         Kafka (lưu history, moderation pipeline)

Scale (2024):
  ~3 triệu concurrent viewers lúc peak
  ~1 triệu messages/phút trong sự kiện lớn
  ~40,000 concurrent live channels
```

### YouTube Live

```
Khác biệt:
  - Dùng DASH thay HLS (MPD file, fMP4 segment)
  - fMP4 tốt hơn với HTTP/2 multiplexing
  - Ultra-low latency mode: ~3 giây (segment 1s)
  - Low latency: ~7 giây
  - Normal: ~25 giây

DVR (tua lại trong khi đang live):
  Segment cũ vẫn được giữ trong GCS
  Playlist window mở rộng (giữ 4 giờ thay vì chỉ 3 segment cuối)
  Viewer có thể tua từ đầu buổi stream
```

### Shopee Live / TikTok Live (E-commerce)

```
Đặc điểm riêng:
  - Interactive: viewer mua hàng ngay trong khi xem
  - Gift: gửi quà ảo → animation cho tất cả viewer thấy
  - Product overlay: sản phẩm bán hiển thị chồng lên video
  - Low latency: 5-10 giây là đủ (không cần < 1s)

Architecture:
  Video:           RTMP → Transcoding → S3 → CDN → HLS → Viewer
  Chat/Gift/React: WebSocket + Redis Pub/Sub
  Purchase flow:   REST API → order-service → Kafka SAGA (như project này!)
  Analytics:       Kafka → analytics-service → real-time dashboard

Luồng mua hàng trong live:
  Viewer click "Mua ngay" → POST /orders/kafka-saga
    → order.created (Kafka)
    → inventory.reserved (Kafka)
    → payment.completed (Kafka)
    → WebSocket notify: "Bạn đặt hàng thành công!"
    → WebSocket broadcast to room: "Vừa có người mua sản phẩm này"
```

---

## 10. Các vấn đề phức tạp trong production

### Thundering Herd — Bầy thú điên

```
Problem:
  CDN cache segment hết hạn (sau 6 giây)
  1,000,000 viewers cùng lúc request segment mới
  → 1,000,000 cache miss → 1,000,000 request tới origin cùng lúc → origin sập

Giải pháp:

1. Request Coalescing (CDN tự làm):
   1,000,000 viewers request cùng segment_483.ts
   CDN nhận được → kiểm tra đang fetch từ origin rồi → hold các request sau
   Origin trả về 1 lần → CDN serve cho cả 1,000,000

2. Origin Shield:
   Không phải CDN edge nào cũng tự làm coalescing
   Shield layer đảm bảo chỉ 1 request lên origin

3. Staggered Cache:
   Segment mới: cache-control max-age=2
   Sau 10 giây: max-age=3600 (cache lâu, segment đó không thay đổi nữa)
```

### Segment Availability Race Condition

```
Problem:
  1. Encoder tạo xong segment_500.ts (1.5MB)
  2. Cập nhật playlist.m3u8 để list segment_500
  3. Player download playlist → thấy segment_500 → request ngay
  4. CDN miss → fetch origin → S3 trả file...
     Nhưng S3 upload chưa xong propagate → 404!

Giải pháp:
  Upload segment TRƯỚC → đợi xác nhận từ S3 → mới cập nhật playlist
  
  Hoặc: playlist chỉ list segment đã tồn tại ≥ 2 giây
  → Thêm 2 giây latency nhưng không bao giờ 404
```

### Geo-restriction & DRM

```
Geo-restriction (bản quyền theo vùng):
  CDN check IP → MaxMind GeoIP DB → "IP này từ VN"
  Nội dung có phép ở VN? → serve
  Không có phép?          → 403 hoặc redirect tới trang mua bản quyền

DRM (bảo vệ nội dung):
  HLS Encryption (cơ bản):
    Encoder mã hóa segment bằng AES-128
    Key Server cấp key chỉ cho viewer đã authenticate
    
  Widevine/FairPlay/PlayReady (mạnh hơn):
    Key không bao giờ expose ra JS
    Cần CDM (Content Decryption Module) trong OS/browser
    Netflix, Disney+ dùng multi-DRM (cả 3 để cover mọi device)

  Flow:
    Player load segment (encrypted) → gửi license request tới Key Server
    Key Server verify JWT → trả AES key
    CDM decrypt (trong secure enclave) → decode → play
    → Dù download được .ts file cũng không decrypt được nếu không có license
```

### Transcoding Cost Optimization

```
Live stream của 1 channel:
  Transcoding 4 quality levels = 4 CPU/GPU cores liên tục
  40,000 channels (Twitch peak) × 4 cores = 160,000 cores

  → Chi phí khổng lồ!

Optimization:
  1. Per-title encoding: phân tích video content → chọn encode settings phù hợp
     Phim hoạt hình (ít detail) → có thể dùng bitrate thấp hơn với chất lượng tương đương
     
  2. Ladder optimization: không cần đủ 5 chất lượng cho mọi stream
     720p stream → không cần encode 1080p output
     
  3. Spot instances (AWS): dùng server rẻ hơn, chấp nhận bị terminate
     Transcoding job chia nhỏ → nếu instance bị kill → retry segment đó
     
  4. AV1 codec (thế hệ mới):
     30-50% nhỏ hơn H.264 cùng chất lượng → tiết kiệm bandwidth CDN
     Nhưng encode chậm hơn H.264 → chỉ dùng cho VOD, không phải live
```

---

## 11. Bảng so sánh các protocol streaming

| | HLS | DASH | WebRTC | RTMP |
|---|---|---|---|---|
| Latency | 5-45s (LLHLS: 2-5s) | 3-10s (LL-DASH: 2-5s) | < 500ms | ~2s |
| Scale | Triệu viewer | Triệu viewer | ~1,000/SFU node | Chỉ ingest |
| CDN-friendly | ✅ HTTP | ✅ HTTP | ❌ UDP | ❌ TCP stream |
| Browser native | ✅ Safari/iOS | ✅ Chrome/FF/Edge | ✅ Built-in | ❌ Plugin |
| Adaptive bitrate | ✅ | ✅ | Hạn chế | ❌ |
| DVR/tua lại | ✅ | ✅ | ❌ | ❌ |
| DRM | ✅ FairPlay | ✅ Widevine | ❌ | ❌ |
| Use case | Live + VOD | Live + VOD | Video call | Ingest từ OBS |

```
Chọn protocol theo use case:

Bóng đá / concert cho 1 triệu người:
  → HLS + CDN (Akamai/Cloudflare) + LLHLS nếu cần latency thấp hơn

Video call (Zoom/Meet style):
  → WebRTC + SFU (LiveKit, Mediasoup, Janus)

E-commerce live (Shopee Live style):
  → HLS cho video + WebSocket cho chat/reaction/gift
  → Redis Pub/Sub để scale WS ngang

Twitch clone (build từ đầu):
  RTMP ingest → FFmpeg transcode → HLS output → S3 → CDN
  Chat → WebSocket + Redis Pub/Sub
  Analytics → Kafka + stream processing (như project này!)

Streamer muốn push lên server:
  → RTMP (OBS, hardware encoder) — ổn định nhất
  → WebRTC (browser-based) — không cần phần mềm thêm
```

---

## Tóm tắt — Tại sao server không sập khi hàng triệu người xem?

```
Nguyên tắc 1: KHÔNG để origin server trực tiếp phục vụ viewer
  → CDN edge phục vụ ~99% requests
  → Origin chỉ nhận ~1% (segment mới nhất)

Nguyên tắc 2: Stateless protocol (HLS/DASH)
  → Mỗi segment = 1 HTTP request độc lập
  → Server không giữ state per viewer
  → CDN cache được → scale vô hạn

Nguyên tắc 3: Tách biệt các concerns:
  Ingest server:    nhận RTMP từ streamer (ít server)
  Transcoding:      encode nhiều chất lượng (auto-scale)
  Storage (S3):     lưu segment (unlimited, tự scale)
  CDN:              phân phối tới viewer (hàng nghìn edge)
  Player:           decode trên device của viewer

Nguyên tắc 4: Adaptive bitrate
  → Mạng yếu → giảm chất lượng tự động → không buffer
  → Server luôn có nhiều resolution để chọn

Nguyên tắc 5: WebSocket chỉ cho metadata, không cho video
  → Chat, reaction, viewer count → WebSocket + Redis Pub/Sub
  → Video content → HLS/CDN, không bao giờ WS
```
