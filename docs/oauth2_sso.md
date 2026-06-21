# OAuth2 / SSO / Authentication — Interview Q&A

---

## Bức tranh tổng quan trước

```
Có 3 câu hỏi cần phân biệt:

  Authentication (AuthN) = "Mày là ai?"         → Login, xác minh danh tính
  Authorization  (AuthZ) = "Mày được làm gì?"   → Phân quyền, scope
  OAuth2                 = protocol cho AuthZ    → Nhưng thường dùng kèm AuthN
  SSO                    = "Login 1 lần, dùng nhiều app"
  OpenID Connect (OIDC)  = OAuth2 + AuthN layer  → thêm ID Token
```

---

## PHẦN 1 — CÁC LOẠI TOKEN

### Access Token
- Dùng để **gọi API** thay mặt user
- Thường là JWT, thời gian ngắn (15 phút - 1 giờ)
- Gửi trong header: `Authorization: Bearer <token>`

### Refresh Token
- Dùng để **lấy Access Token mới** khi hết hạn
- Thời gian dài (7-30 ngày), lưu an toàn hơn
- Chỉ gửi đến Authorization Server, không gửi đến Resource Server

### ID Token (OIDC)
- Chứa thông tin **về user** (name, email, sub)
- Chỉ dùng để hiển thị thông tin, không dùng để gọi API
- Là JWT, app đọc để biết user là ai

```
Access Token  → gọi API (ai là tao, tao được làm gì)
Refresh Token → lấy Access Token mới (không cần login lại)
ID Token      → biết user là ai (tên, email)
```

---

## PHẦN 2 — CẤU TRÚC JWT

```
Header.Payload.Signature

eyJhbGciOiJSUzI1NiJ9  .  eyJzdWIiOiJ1c2VyMTIzIn0  .  <signature>
      Header                    Payload                   Signature
```

```json
// Header (decode base64)
{ "alg": "RS256", "typ": "JWT" }

// Payload (decode base64)
{
  "sub":   "user-123",          // subject — user ID
  "name":  "Thong Nguyen",
  "email": "thong@example.com",
  "iss":   "https://auth.company.com",  // issuer — ai cấp token
  "aud":   "my-api",            // audience — token dùng cho app nào
  "iat":   1718928000,          // issued at
  "exp":   1718931600           // expires at (1 giờ sau)
}
```

**Signature** = `RS256(base64(header) + "." + base64(payload), privateKey)`
- Server ký bằng private key
- Client/API verify bằng public key
- Không thể giả mạo nếu không có private key

> **JWT không encrypt** — ai cũng đọc được payload. Không bao giờ để secret trong JWT.

---

## PHẦN 3 — GRANT TYPES (loại flow xin token)

### 3.1 Authorization Code Flow (phổ biến nhất — Web App)

**Dùng khi:** Web app có backend, user login qua browser

```
User        Browser          App Server       Auth Server       API
 │              │                │                 │              │
 │──Click Login─►│                │                 │              │
 │              │──Redirect to──►│                 │              │
 │              │  auth server   │                 │              │
 │              │◄───────────────┼──────Redirect───│              │
 │              │  (login page)  │                 │              │
 │──Enter pass──►│                │                 │              │
 │              │────────────────┼─────────────────►│              │
 │              │                │                 │              │
 │              │◄───────────────┼──Redirect with──│              │
 │              │  callback URL  │  ?code=ABC123   │              │
 │              │                │                 │              │
 │              │────code ABC────►│                 │              │
 │              │                │──POST /token────►│              │
 │              │                │  code=ABC123    │              │
 │              │                │  client_secret  │              │
 │              │                │◄────────────────│              │
 │              │                │  access_token   │              │
 │              │                │  refresh_token  │              │
 │              │                │─────────────────┼─GET /api────►│
 │              │                │                 │  Bearer token│
```

**Tại sao cần 2 bước (code rồi mới đổi token)?**
- `code` xuất hiện trên URL → có thể bị lộ qua browser history, logs
- `code` chỉ dùng được 1 lần, hết hạn nhanh (60 giây)
- Đổi code → token xảy ra ở **server-to-server** (có `client_secret`) → an toàn hơn

---

### 3.2 Authorization Code + PKCE (cho SPA / Mobile)

**Vấn đề:** SPA (React, Vue) không có backend → không giữ được `client_secret`

**PKCE** (Proof Key for Code Exchange) — thay `client_secret` bằng cặp key tạm:

```
1. App tạo:
   code_verifier  = random string 43-128 ký tự
   code_challenge = BASE64URL(SHA256(code_verifier))

2. Gửi code_challenge khi redirect đến Auth Server
3. Auth Server trả về code
4. App đổi code → token, gửi kèm code_verifier
5. Auth Server hash code_verifier → so sánh với code_challenge đã lưu
   → khớp → cấp token ✅
   → không khớp → từ chối ❌
```

Kẻ tấn công lấy được `code` cũng không đổi được token vì không có `code_verifier`.

---

### 3.3 Client Credentials Flow (không có user — machine to machine)

**Dùng khi:** Service A gọi Service B, cron job, background worker — không có user thật

```
Service A                    Auth Server              Service B (API)
    │                             │                        │
    │──POST /token────────────────►│                        │
    │  grant_type=client_credentials                        │
    │  client_id=svc-a                                      │
    │  client_secret=xxx                                    │
    │◄────────────────────────────│                        │
    │  access_token (no user info) │                        │
    │                             │                        │
    │─────────────────────────────┼──GET /api ────────────►│
    │                             │  Bearer token           │
```

Token không có `sub` (user), chỉ có scope/permissions của service.

---

### 3.4 Implicit Flow (deprecated — không dùng nữa)

Token trả thẳng về URL fragment `#access_token=xxx` — không qua code exchange.
Bị deprecated vì token lộ trên URL, browser history. **Dùng PKCE thay thế.**

---

### 3.5 Resource Owner Password Flow (hạn chế dùng)

User gửi username/password thẳng cho App, App forward lên Auth Server.

```
App──POST /token──►Auth Server
     username=xxx
     password=yyy
```

**Vấn đề:** App nhìn thấy password của user → chỉ dùng được khi App và Auth Server **cùng công ty**, không dùng cho third-party.

---

### 3.6 Device Code Flow (TV, CLI tool)

Dùng cho thiết bị không có browser (Smart TV, CLI):

```
1. Device xin device_code + user_code từ Auth Server
2. Hiển thị: "Vào https://example.com/activate, nhập code: ABCD-1234"
3. User dùng điện thoại/máy tính vào URL đó, nhập code, login
4. Device polling Auth Server mỗi 5 giây: "Xong chưa?"
5. Sau khi user approve → Device nhận access_token
```

---

## PHẦN 4 — CALLBACK URL

**Callback** (hay Redirect URI) = URL mà Auth Server redirect về sau khi user login xong.

```
App đăng ký với Auth Server:
  redirect_uri = https://myapp.com/auth/callback

Luồng:
  1. App redirect user đến: https://auth.server.com/authorize
                             ?redirect_uri=https://myapp.com/auth/callback
                             &client_id=my-app
                             &response_type=code

  2. User login xong → Auth Server redirect về:
                             https://myapp.com/auth/callback?code=ABC123

  3. App nhận code tại endpoint /auth/callback → đổi lấy token
```

**Tại sao phải đăng ký trước?**
Auth Server chỉ redirect về URL đã đăng ký → ngăn kẻ tấn công redirect về URL của họ để lấy code.

---

## PHẦN 5 — SSO (Single Sign-On)

**Khái niệm:** Login 1 lần ở Auth Server → dùng được nhiều app mà không cần login lại.

```
User login vào App A:
  Browser ──► App A ──redirect──► Auth Server
              (chưa có session)    User login lần đầu
                                   Auth Server tạo SSO session (cookie)
              App A ◄──code────── Auth Server
              App A đổi token ✅

User sau đó vào App B:
  Browser ──► App B ──redirect──► Auth Server
              (chưa có session)    Auth Server thấy SSO cookie còn hạn
                                   KHÔNG cần login lại
              App B ◄──code────── Auth Server tự cấp code
              App B đổi token ✅  User không thấy màn hình login
```

**SSO Session** lưu ở Auth Server dưới dạng cookie `HttpOnly; Secure; SameSite=Lax`.

---

### Single Logout (SLO)

Logout khỏi 1 app → logout khỏi tất cả:

```
User logout App A
  → App A gọi Auth Server: POST /logout
  → Auth Server xóa SSO session
  → Auth Server notify App B, App C (back-channel logout)
  → Tất cả app xóa session local
```

---

## PHẦN 6 — ACCESS TOKEN vs SESSION COOKIE

| | Session Cookie | JWT Access Token |
|---|---|---|
| Lưu ở đâu | Server (DB/Redis) | Client (memory/localStorage) |
| Stateful/Stateless | Stateful | Stateless |
| Scale | Cần share session giữa servers | Không cần — verify bằng public key |
| Revoke | Xóa session khỏi DB ngay | Không thể revoke trước exp — cần blacklist |
| Kích thước | Small (session ID) | Lớn hơn (payload) |
| Dùng khi | Monolith, web truyền thống | Microservices, API, mobile |

---

## PHẦN 7 — CÂU HỎI INTERVIEW

### Q: OAuth2 là gì? Khác Authentication thế nào?

**Trả lời:**
OAuth2 là protocol **authorization** — cho phép app A truy cập resource của user trên app B mà không cần biết password. Bản thân OAuth2 không làm authentication — OpenID Connect (OIDC) build on top của OAuth2 để thêm tầng authentication (ID Token).

Ví dụ: "Login with Google" là OIDC — Google vừa xác minh bạn là ai (AuthN), vừa cấp quyền cho app đọc profile (AuthZ).

---

### Q: Authorization Code flow hoạt động thế nào? Tại sao cần 2 bước?

**Trả lời:**
1. App redirect user đến Auth Server kèm `client_id`, `redirect_uri`, `scope`
2. User login, Auth Server trả về `code` ngắn hạn qua redirect URL
3. App backend dùng `code` + `client_secret` đổi lấy `access_token`

Cần 2 bước vì `code` xuất hiện trên URL (có thể bị lộ), còn việc đổi token xảy ra server-to-server với `client_secret` → an toàn hơn.

---

### Q: PKCE là gì, tại sao SPA cần PKCE?

**Trả lời:**
SPA chạy trên browser không thể giữ `client_secret` an toàn (ai cũng inspect source được). PKCE thay thế bằng cặp `code_verifier` / `code_challenge` tạo ngẫu nhiên mỗi lần login — kể cả lấy được `code` cũng không đổi được token nếu không có `code_verifier`.

---

### Q: Client Credentials dùng khi nào?

**Trả lời:** Machine-to-machine communication — không có user. Ví dụ: order-service gọi inventory-service, cron job gọi internal API. Token không chứa thông tin user, chỉ chứa scope/permission của service.

---

### Q: Access Token bị lộ thì làm gì?

**Trả lời:**
- Ngắn hạn (15-60 phút) → tự hết hạn sớm
- Revoke Refresh Token ngay → không lấy được Access Token mới
- Nếu cần revoke Access Token ngay: dùng **token introspection** hoặc **blacklist** (Redis) — nhưng mất tính stateless của JWT

---

### Q: Khác nhau giữa `aud`, `iss`, `sub` trong JWT?

```
iss (issuer)   = ai cấp token           → "https://auth.company.com"
sub (subject)  = token đại diện cho ai  → user ID "user-123"
aud (audience) = token dùng cho app nào → "inventory-api"
```

API nên check cả 3: đúng issuer, đúng audience, sub hợp lệ.

---

### Q: Tại sao không nên lưu JWT trong localStorage?

**Trả lời:** XSS attack — JavaScript độc hại inject vào trang có thể đọc localStorage và đánh cắp token. Nên lưu Access Token trong **memory** (biến JS), Refresh Token trong **HttpOnly cookie** (JS không đọc được).

---

## PHẦN 8 — MICROSOFT / AZURE AD (MSAL)

Nếu công ty dùng Azure AD / Microsoft Entra:

```
Grant Type dùng: Authorization Code + PKCE (SPA) hoặc Authorization Code (Web)
Endpoint:
  Authorization: https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/authorize
  Token:         https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token

Scope đặc biệt:
  openid          → lấy ID Token
  profile         → thông tin user (name, username)
  email           → email
  offline_access  → lấy Refresh Token
  api://my-api/.default → scope của custom API
```

**MSAL.js flow:**
```javascript
// SPA dùng MSAL — tự handle PKCE, token cache, refresh
const msalInstance = new PublicClientApplication(config);

// Login
await msalInstance.loginRedirect({ scopes: ['openid', 'profile'] });

// Lấy token (tự refresh nếu hết hạn)
const result = await msalInstance.acquireTokenSilent({
  scopes: ['api://my-api/read'],
  account: msalInstance.getAllAccounts()[0]
});
// result.accessToken → gắn vào API call
```

---

## PHẦN 9 — TỔNG KẾT CHỌN GRANT TYPE

```
Có user + có backend server       → Authorization Code
Có user + SPA (không có backend)  → Authorization Code + PKCE
Không có user (service-to-service)→ Client Credentials
Thiết bị không có browser         → Device Code
─────────────────────────────────────────────────────
Implicit              → DEPRECATED, không dùng
Resource Owner Password→ Chỉ dùng khi app và auth server cùng công ty
```
