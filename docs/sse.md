# Server-Sent Events (SSE) — Cơ chế hoạt động

## Tại sao SSE trông giống WebSocket?

Cả hai đều cho cảm giác "realtime" — server đẩy data về browser mà không cần browser hỏi. Nhưng bên dưới hoàn toàn khác nhau.

---

## SSE là gì thật ra?

SSE **là một HTTP request bình thường** — nhưng server không bao giờ đóng connection, và cứ có data mới thì ghi thêm vào response stream đó.

```
Browser                          Server
  │                                │
  ├─── GET /stream ───────────────►│  HTTP request thường
  │    Accept: text/event-stream   │
  │                                │
  │◄── HTTP/1.1 200 OK ────────────┤
  │    Content-Type: text/event-stream
  │    Cache-Control: no-cache     │
  │    Connection: keep-alive      │
  │                                │  ← connection không đóng
  │◄── data: {"type":"order"}\n\n ─┤  server ghi chunk khi có event
  │                                │
  │    (im lặng vài giây...)       │
  │                                │
  │◄── data: {"type":"payment"}\n\n┤  server ghi tiếp
  │                                │
  │    (mãi mãi...)                │
```

**Điểm mấu chốt:** Đây vẫn là HTTP — browser dùng `EventSource` API tự xử lý việc đọc stream theo từng chunk.

---

## So sánh HTTP thường vs SSE vs WebSocket

```
HTTP thường (REST):
  Browser ──request──► Server ──response──► Browser
  Connection đóng ngay sau response
  Muốn data mới → phải gọi lại (polling)

SSE:
  Browser ──request──► Server
  Connection GIỮ MÃI
  Server ──data──► Browser (bất cứ lúc nào)
  Browser KHÔNG gửi gì được qua connection này
  → ONE-WAY: Server → Browser

WebSocket:
  Browser ──upgrade handshake──► Server
  Connection đặc biệt (không phải HTTP nữa)
  Browser ◄──► Server (hai chiều, bất cứ lúc nào)
  → TWO-WAY: Server ↔ Browser
```

---

## Tại sao "giữ connection mãi" được?

HTTP/1.1 có tính năng **chunked transfer encoding** — server có thể gửi response theo từng mảnh (chunk) thay vì gửi hết một lần rồi đóng.

```javascript
// Server side — Express
app.get('/stream', (req, res) => {
    // Set headers báo browser: "đây là stream, đừng timeout"
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    // Ghi chunk ngay lập tức — browser nhận được
    res.write('data: {"msg":"connected"}\n\n');

    // Sau 5 giây ghi thêm — browser nhận tiếp
    setTimeout(() => {
        res.write('data: {"msg":"hello again"}\n\n');
    }, 5000);

    // res.end() KHÔNG được gọi → connection không đóng
    // Khi browser disconnect → req.on('close') được trigger
    req.on('close', () => {
        // cleanup
    });
});
```

```javascript
// Browser side
const es = new EventSource('/stream');

es.onmessage = (event) => {
    const data = JSON.parse(event.data); // tự parse từng chunk
    console.log(data);
};

es.onerror = () => {
    // Browser TỰ ĐỘNG reconnect sau vài giây nếu mất kết nối
};
```

---

## Format message của SSE

SSE có định dạng text đơn giản, **không phải binary**:

```
data: {"type":"order.created","productId":"IPHONE-15"}\n\n
```

Có thể thêm `id:` và `event:` để phân loại:

```
id: 42
event: payment
data: {"status":"completed","amount":1500000}

```

- `\n\n` (2 dòng trống) = kết thúc 1 message
- `\n` (1 dòng trống) = vẫn trong cùng 1 message (multi-line data)
- `id:` = browser dùng để `Last-Event-ID` header khi reconnect → không bỏ lỡ event

---

## Cơ chế reconnect tự động

Đây là điểm SSE vượt trội so với polling thủ công:

```
Browser ──GET /stream──► Server
         ◄── streaming ──┤
         
[Server crash hoặc network drop]

Browser tự detect → đợi 3 giây (mặc định) → reconnect
Browser ──GET /stream──► Server
         Header: Last-Event-ID: 42   ← gửi kèm ID đã nhận cuối
         ◄── streaming từ ID 43 ──── ┤  server có thể resume từ đó
```

Trong code ui-service của project:

```javascript
(function connect() {
    const es = new EventSource('/stream');
    es.onopen  = () => { statusEl.textContent = '● Connected'; };
    es.onerror = () => {
        statusEl.textContent = '● Reconnecting...';
        es.close();
        setTimeout(connect, 3000); // tự reconnect sau 3s
    };
})();
```

---

## Tại sao project dùng SSE thay vì WebSocket?

| Tiêu chí | SSE | WebSocket |
|---|---|---|
| Hướng data | Server → Browser (1 chiều) | 2 chiều |
| Nhu cầu của dashboard | Chỉ cần nhận event, không cần gửi lại | Overkill |
| Proxy / Load balancer | Hoạt động tốt (vẫn là HTTP) | Cần config thêm |
| Tự reconnect | ✅ Browser tự xử lý | ❌ Phải tự code |
| Độ phức tạp | Thấp — `res.write()` là đủ | Cao hơn — cần upgrade protocol |
| Thư viện cần | Không cần (Web API có sẵn) | Cần `ws` hoặc `socket.io` |

**Quy tắc chọn:**
- Dashboard hiển thị realtime → **SSE**
- Chat, game, collaborative editing → **WebSocket**
- Kiểm tra data theo khoảng thời gian cố định → **Polling**

---

## Luồng trong project này

```
Kafka consumer (inventory-events)
          │
          ▼
ui-service/server.js
    kafkaConsumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value);
            broadcast(event);          // gửi đến TẤT CẢ browser đang connect
        }
    });
          │
          ▼
    res.write(`data: ${JSON.stringify(event)}\n\n`)
          │  (cho mỗi SSE connection đang mở)
          ▼
Browser (EventSource)
    es.onmessage = ({ data }) => {
        processEvent(JSON.parse(data)); // animate packet trên SVG
    };
```

```javascript
// broadcast() trong ui-service/server.js
const clients = new Set(); // danh sách tất cả browser đang connect

app.get('/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    clients.add(res);                     // đăng ký connection mới
    req.on('close', () => clients.delete(res)); // dọn dẹp khi disconnect
});

function broadcast(data) {
    const payload = `data: ${JSON.stringify(data)}\n\n`;
    for (const res of clients) {
        res.write(payload);               // ghi vào TẤT CẢ connection đang mở
    }
}
```

Mỗi tab browser mở dashboard = 1 entry trong `clients` Set. Khi có event Kafka → `broadcast()` → tất cả tab nhận cùng lúc.

---

## SSE vs Polling — tại sao SSE hiệu quả hơn

```
Polling (cứ 1 giây hỏi 1 lần):
  Browser ──GET /events──► Server   t=0s
           ◄── [] ─────────┤  (không có gì)
  Browser ──GET /events──► Server   t=1s
           ◄── [] ─────────┤  (không có gì)
  Browser ──GET /events──► Server   t=2s
           ◄── [event] ────┤  (có event)
  → 3 request, 2 request lãng phí, độ trễ tối đa 1s

SSE:
  Browser ──GET /stream──► Server   t=0s  (1 lần duy nhất)
  Server ──────────────────────────────── giữ connection
           ◄── event ──────┤   t=2.3s    ngay khi có
  → 1 request, 0 request lãng phí, độ trễ ~0ms
```

**Với 1000 user:**
- Polling 1s: 1000 request/giây liên tục dù không có gì
- SSE: 1000 connection mở, server chỉ tốn bandwidth khi thật sự có event
