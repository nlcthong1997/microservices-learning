# WebSocket & Socket.IO — Khi nào dùng trong E-commerce

## 1. WebSocket là gì (so với SSE và HTTP)?

```
HTTP (REST):
  Browser ──GET /orders──────────────────► Server
  Browser ◄──200 OK {data}────────────────Server
  Connection đóng ngay — muốn data mới phải gọi lại (polling)

SSE — Server-Sent Events (ui-service :3004):
  Browser ──GET /stream──────────────────► Server
  Browser ◄──data: event1\n\n─────────────Server  ← connection GIỮ MÃI
  Browser ◄──data: event2\n\n─────────────Server
  Browser KHÔNG GỬI được qua connection này → ONE-WAY

WebSocket (chat-service :3006):
  Browser ──GET /socket.io/?...──────────► Server   ← bắt đầu bằng HTTP
           ◄──101 Switching Protocols──────Server   ← server đồng ý UPGRADE
           ══════════ WebSocket Frame ══════════     ← TCP connection mới
  Browser ──frame: "join room"───────────► Server   ← browser gửi bất cứ lúc nào
  Browser ◄──frame: "user joined"─────────Server   ← server gửi bất cứ lúc nào
  Browser ──frame: "typing: true"────────► Server   ← BIDIRECTIONAL
  Browser ◄──frame: "user:typing"─────────Server
```

**Điểm mấu chốt của WebSocket:**
- Sau bước `101 Switching Protocols`, HTTP kết thúc — WebSocket tiếp quản
- Không còn request/response — chỉ là hai đầu truyền frame cho nhau bất cứ lúc nào
- 1 TCP connection duy nhất cho cả hai chiều

---

## 2. So sánh chi tiết SSE vs WebSocket

| Tiêu chí | SSE | WebSocket |
|---|---|---|
| Chiều dữ liệu | Server → Client (1 chiều) | Server ↔ Client (2 chiều) |
| Protocol | HTTP thường (giữ connection) | HTTP upgrade → TCP frame |
| Browser API | `EventSource` (built-in) | `WebSocket` (built-in) hoặc Socket.IO |
| Reconnect tự động | ✅ Built-in (EventSource tự retry) | ❌ Phải tự xử lý (Socket.IO làm thay) |
| Qua HTTP proxy/firewall | ✅ Dễ (là HTTP) | ⚠️ Có thể bị block, cần config |
| Multiplexing | ❌ Mỗi topic 1 connection | ✅ Rooms + Namespaces (Socket.IO) |
| Phù hợp cho | Notifications, feed, live metrics | Chat, typing, presence, bidirectional |
| Binary data | ❌ Chỉ text | ✅ Text và binary |
| Độ phức tạp | Thấp (là HTTP) | Cao hơn (state machine, reconnect) |

---

## 3. Bản đồ use case trong E-commerce

### Dùng SSE (one-way — đủ rồi):

```
Order status tracking
  Kafka SAGA → payment-service publish "payment.completed"
             → ui-service consume → SSE → browser hiển thị "Đã thanh toán"
  Không cần browser gửi gì ngược lại ✅ SSE là đủ

Live stock counter ("Còn 3 sản phẩm")
  inventory-service publish stock_update
             → ui-service → SSE → browser cập nhật số lượng
  Browser chỉ xem, không gửi lại ✅ SSE là đủ

Flash sale countdown timer
  Server broadcast timestamp đồng bộ → tất cả browser đếm ngược cùng nhau
  ✅ SSE là đủ

Admin sales dashboard
  Kafka stream → analytics-service → SSE → real-time chart
  Admin chỉ đọc ✅ SSE là đủ

Push notifications (order confirm, shipped)
  ✅ SSE là đủ
```

### Dùng WebSocket (cần 2 chiều):

```
Live support chat
  User gõ → socket.emit('message') → server broadcast to room → agent nhận
  Typing indicator: user gõ → socket.emit('typing') → agent thấy "đang gõ"
  ❌ SSE không làm được — browser cần GỬI liên tục

Live bidding / Đấu giá
  User đặt giá → socket.emit('bid', { price }) → server validate → broadcast giá mới
  Real-time: tất cả bidder thấy giá mới trong < 100ms
  ❌ Không thể dùng SSE + POST riêng (delay, không atomic)

Collaborative wishlist
  User thêm item trên mobile → sync sang laptop ngay lập tức
  Cần user gửi action + nhận updates ❌ SSE không làm được

Admin ACK / interactive dashboard
  Admin nhận Kafka event qua WS → nhấn "ACK" → gửi về server → update DB
  ❌ SSE không có chiều ngược

Multiplayer features
  Flash sale: nhiều user cùng "grab" item → server xử lý thứ tự → broadcast kết quả
  ❌ SSE không làm được
```

---

## 4. Socket.IO là gì (vs raw WebSocket)?

Socket.IO là thư viện **build on top of WebSocket** với nhiều tính năng thêm:

```
Raw WebSocket:
  new WebSocket('ws://localhost:3006')
  - Chỉ send/receive string hoặc binary
  - Không có rooms, namespaces, events
  - Reconnect phải tự xử lý
  - Server down → connection drop → bạn tự handle

Socket.IO:
  io('/chat')
  - Events: socket.emit('join', data) / socket.on('message', handler)
  - Rooms:  socket.join('room-123') → chỉ broadcast trong room đó
  - Namespaces: /chat, /admin → tách logic, cùng 1 server
  - Auto-reconnect: Socket.IO tự retry khi mất kết nối
  - Fallback: nếu WebSocket bị block → tự dùng HTTP long-polling
```

**Khi nào dùng raw WebSocket?**
- Cần performance tối đa (Socket.IO có overhead nhỏ)
- Client không phải browser (IoT device, mobile native app)
- Giao thức binary tự định nghĩa

**Khi nào dùng Socket.IO?**
- Web app cần rooms, namespaces, auto-reconnect
- Team muốn API đơn giản hơn
- Cần fallback cho môi trường hạn chế WebSocket

---

## 5. Pattern: WebSocket Gateway trong Microservice

```
                    ┌─────────────────────────────────┐
Internal services   │         chat-service             │   Browser clients
                    │                                  │
order-service   ──► │  Kafka consumer                  │
inventory-service ─►│    order-events      adminNS ◄──►│ Admin tabs
payment-service ──► │    inventory-events  (/admin)    │
                    │    payment-events                │
                    │                                  │
                    │  Socket.IO server   chatNS  ◄──► │ User tabs
                    │                    (/chat)       │
                    └─────────────────────────────────┘
```

**chat-service đóng vai "WebSocket Gateway":**
- Internal: consume Kafka (service-to-service communication)
- External: Socket.IO (browser-facing, bidirectional)
- Tách biệt: internal protocol (Kafka binary) khỏi external protocol (WebSocket)

Đây là pattern phổ biến trong production:
- Netflix: internal events → WebSocket gateway → mobile/web clients
- Shopee: order status Kafka → WS gateway → app notifications
- Tiki: inventory events → SSE/WS → product page live stock

---

## 6. Architecture tổng của project này

```
Communication patterns đã học:

Stage 1: HTTP REST        order-service ──REST──► inventory-service
Stage 2: HTTP Resilient   + timeout + retry + circuit breaker
Stage 3: Kafka Stream     order-service ──Kafka──► analytics-service
Stage 4: gRPC             order-service ──gRPC──► inventory-service
Stage 5: Kafka SAGA       order → inventory → payment (choreography)
Stage 6: WebSocket        chat-service ──WS──► browsers (bidirectional)
                          Kafka → chat-service → Socket.IO bridge

                   Browser
                      │
              ┌───────┼────────┐
             SSE    WebSocket  REST
              │        │        │
         ui-service  chat-   order-
          :3004      service  service
              │      :3006    :3001
              │        │        │
              └────────┴────────┘
                      │
                    Kafka / RabbitMQ
                      │
         ┌────────────┼────────────┐
    inventory-     payment-   analytics-
    service        service    service
    :3002          :3003       :3005
```

---

## 7. Microservice KHÔNG bắt buộc phải có WebSocket

Quan niệm sai: "microservice = phải có WebSocket"

Thực tế:
- WebSocket là **1 trong nhiều communication protocol**
- Phần lớn microservice giao tiếp qua REST hoặc Kafka (internal)
- WebSocket chỉ cần ở "edge" — nơi browser cần realtime bidirectional
- 90% e-commerce features dùng SSE (simpler) là đủ
- Chỉ thêm WebSocket khi có use case rõ ràng cần 2 chiều

Stack phổ biến trong thực tế:
```
Browser
  ├── REST (CRUD, form submit)
  ├── SSE (notifications, status updates)    ← đơn giản hơn WS
  └── WebSocket (chat, live bidding, games)  ← chỉ khi cần

Backend
  ├── REST API (public facing)
  ├── gRPC (internal service-to-service)
  └── Kafka/RabbitMQ (async event bus)
```
