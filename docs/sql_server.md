# SQL Server — Interview Q&A + Demo

---

## NHÓM 1 — INDEX

### Q: Sự khác nhau giữa Clustered và Non-Clustered Index?

**Trả lời:**

Tưởng tượng bảng `orders` là một **cuốn sổ tay** ghi đơn hàng:

**Clustered Index = thứ tự sắp xếp vật lý của các trang trong sổ**
```
Sổ chính (clustered theo id):
  Trang 1: id=1, customer=42, amount=100
  Trang 2: id=2, customer=99, amount=300
  Trang 3: id=3, customer=17, amount=200
  ...
  → Tìm id=42 → nhảy thẳng trang 42 ✅
  → Tìm customer=42 → vẫn đọc hết vì sổ sắp theo id, không theo customer ❌
```
- Mỗi bảng **chỉ có 1** clustered index (vì data chỉ có 1 thứ tự vật lý)
- Mặc định SQL Server đặt clustered index trên PRIMARY KEY — nhưng không bắt buộc

**Non-Clustered Index = cuốn sổ phụ riêng, tra nhanh rồi quay lại sổ chính**
```
Sổ phụ (non-clustered theo customer_id):
  customer=17 → xem trang 3 sổ chính
  customer=42 → xem trang 1, 8, 15 sổ chính   ← chỉ lưu pointer
  customer=99 → xem trang 2 sổ chính

  Tìm customer=42:
    1. Tra sổ phụ → biết cần trang 1, 8, 15    ← Index Seek (nhanh)
    2. Lật sang sổ chính lấy data               ← Key Lookup
```
- Mỗi bảng có thể có **nhiều** non-clustered index
- Bước "lật sổ chính" gọi là **Key Lookup** — là điểm yếu cần fix bằng Covering Index

**Covering Index = sổ phụ chép thêm luôn các cột cần dùng**
```
Sổ phụ (covering — có thêm product_id, amount):
  customer=42 → trang 1, 8, 15 | product=IPHONE | amount=100, 500, 300

  Tìm customer=42, lấy product và amount:
    → Tra sổ phụ là đủ, KHÔNG cần lật sổ chính ✅
```

---

### Q: Khi nào Index làm chậm thay vì nhanh?

**Trả lời:**
- INSERT/UPDATE/DELETE phải cập nhật cả index → thêm overhead
- Index trên cột có ít giá trị phân biệt (ví dụ: status chỉ có 3 giá trị) → optimizer có thể bỏ qua index, dùng Table Scan vẫn nhanh hơn
- Quá nhiều index trên 1 bảng → write chậm, tốn disk

---

### Q: Composite Index — thứ tự cột quan trọng không?

**Trả lời:** Quan trọng — **Leftmost Prefix Rule**: index (a, b, c) chỉ được dùng khi query có cột `a`. Query chỉ có `b` hoặc `c` sẽ không dùng được index này.

```sql
CREATE INDEX idx_composite ON orders(customer_id, status, created_at);

WHERE customer_id = 42                          -- ✅ dùng index
WHERE customer_id = 42 AND status = 'pending'   -- ✅ dùng index
WHERE status = 'pending'                        -- ❌ không dùng được
WHERE created_at > '2025-01-01'                 -- ❌ không dùng được
```

---

### Q: Covering Index là gì, tại sao dùng?

**Trả lời:** Index chứa đủ tất cả cột mà query cần → không cần quay lại bảng gốc (tránh Key Lookup).

```sql
-- Non-covering: index chỉ có customer_id → phải Key Lookup để lấy product_id, amount
CREATE INDEX idx_customer ON orders(customer_id);

-- Covering: nhét luôn các cột cần vào INCLUDE
CREATE INDEX idx_customer_covering
ON orders(customer_id)
INCLUDE (product_id, amount, status, created_at);
-- Query SELECT product_id, amount WHERE customer_id=42 → chỉ đọc index, không quay lại bảng
```

---

### Demo — quan sát sự thay đổi

```sql
-- Setup 200k rows
USE learning_db;
WITH nums AS (
    SELECT TOP 200000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.objects a CROSS JOIN sys.objects b CROSS JOIN sys.objects c
)
INSERT INTO orders (customer_id, product_id, amount, status)
SELECT
    ABS(CHECKSUM(NEWID())) % 1000 + 1,
    'PROD-' + CAST(ABS(CHECKSUM(NEWID())) % 100 + 1 AS VARCHAR),
    ROUND(RAND(CHECKSUM(NEWID())) * 1000, 2),
    CASE ABS(CHECKSUM(NEWID())) % 3 WHEN 0 THEN 'pending' WHEN 1 THEN 'completed' ELSE 'failed' END
FROM nums;

SET STATISTICS IO ON;   -- bật đo logical reads

-- Bước 1: không có index → Clustered Index Scan
SELECT * FROM orders WHERE customer_id = 42;
-- Messages: logical reads ~892

-- Bước 2: thêm non-covering index → Index Seek + Key Lookup
CREATE INDEX idx_customer ON orders(customer_id);
SELECT * FROM orders WHERE customer_id = 42;
-- Messages: logical reads ~50, Execution Plan: Key Lookup Cost 99%

-- Bước 3: covering index → Index Seek only
DROP INDEX idx_customer ON orders;
CREATE INDEX idx_customer_covering
ON orders(customer_id)
INCLUDE (product_id, amount, status, created_at);
SELECT * FROM orders WHERE customer_id = 42;
-- Messages: logical reads ~6, Key Lookup biến mất ✅
```

**Đọc Execution Plan:** `Ctrl+M` trong SSMS → chạy query → tab Execution Plan

| Icon | Nghĩa | Tốt/Xấu |
|---|---|---|
| Table Scan / Clustered Index Scan | Đọc toàn bộ bảng | ❌ |
| Index Scan | Đọc toàn bộ index | ⚠️ |
| Index Seek | Nhảy thẳng đến đúng vị trí | ✅ |
| Key Lookup | Quay lại bảng gốc lấy thêm cột | ⚠️ Cần Covering Index |

---

---

## NHÓM 2 — ISOLATION LEVELS

### Khái niệm 3 loại vấn đề

| Vấn đề | Mô tả | Ví dụ thực tế |
|---|---|---|
| **Dirty Read** | Đọc data chưa commit của transaction khác | Thấy số dư 0đ dù người ta chưa thực sự chuyển tiền |
| **Non-repeatable Read** | Cùng row, đọc 2 lần trong 1 transaction ra kết quả khác nhau | Tính tổng đơn hàng lần 1: 100tr, lần 2: 95tr |
| **Phantom Read** | Row mới xuất hiện giữa chừng transaction | Đếm user VIP lần 1: 10 người, lần 2: 11 người |

---

### Q: 5 Isolation Levels — phát biểu chuẩn?

```
READ UNCOMMITTED  =  đọc cả data chưa commit của transaction khác
                     → nhanh nhất, nguy hiểm nhất

READ COMMITTED    =  chỉ đọc data đã commit
                     → bị block nếu row đang có uncommitted write từ transaction khác
                     → default SQL Server, đủ dùng cho hầu hết app

REPEATABLE READ   =  đọc data đã commit + giữ shared lock trên rows đã đọc
                     → đọc lần 2 vẫn ra cùng kết quả (không ai UPDATE/DELETE được)
                     → nhưng INSERT row mới vào range không bị block → Phantom lọt qua

SERIALIZABLE      =  đọc data đã commit + lock rows + lock cả range điều kiện
                     → an toàn tuyệt đối, chặn cả INSERT mới vào range, chậm nhất

SNAPSHOT          =  đọc snapshot tại thời điểm BEGIN TRAN (lưu version cũ trong tempdb)
                     → không bị block bởi lock của transaction khác
                     → không dirty read, nhưng phải BEGIN TRAN mới để thấy data mới
```

> **READ COMMITTED có lock không?**
> - Có — bị **block khi đọc** nếu row đang bị exclusive lock (transaction khác đang write chưa commit)
> - Khác REPEATABLE READ ở chỗ: READ COMMITTED **thả lock ngay sau khi đọc xong từng row**, không giữ đến cuối transaction
> - Nghĩa là: đọc xong → thả lock → transaction khác có thể sửa → đọc lần 2 có thể khác (Non-repeatable Read)

**Cách nhớ:**
```
REPEATABLE READ = "các row tao đang cầm, mày không được sửa/xóa"
SERIALIZABLE    = "cái vùng tao đang nhìn, mày không được thêm/sửa/xóa gì cả"
```

---

### Q: Mỗi level fix được vấn đề gì?

```
                  Dirty Read   Non-repeatable   Phantom   Block?
──────────────────────────────────────────────────────────────────
READ UNCOMMITTED   ✅ có        ✅ có            ✅ có     Không
READ COMMITTED     ❌           ✅ có            ✅ có     Có  ← default
REPEATABLE READ    ❌           ❌               ✅ có     Có
SERIALIZABLE       ❌           ❌               ❌        Có (mạnh nhất)
SNAPSHOT           ❌           ❌               ❌        Không ← tốt nhất
```

---

### Q: WITH (NOLOCK) khác SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED thế nào?

**Trả lời:** Kết quả tương đương — đều là READ UNCOMMITTED. Khác nhau về phạm vi:

```sql
-- Session level — ảnh hưởng TẤT CẢ query phía sau trong session
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT * FROM orders WHERE id = 1;   -- READ UNCOMMITTED
SELECT * FROM orders WHERE id = 2;   -- READ UNCOMMITTED
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;  -- nhớ reset!

-- Query level — chỉ áp dụng cho bảng đó trong query đó
SELECT o.status, p.name
FROM orders o WITH (NOLOCK)
JOIN products p ON o.product_id = p.id   -- products vẫn READ COMMITTED
WHERE o.id = 1;
```

**Thực tế:** `WITH (NOLOCK)` dùng nhiều hơn vì kiểm soát rõ ràng từng bảng.

---

### Q: Khi nào dùng NOLOCK? Rủi ro gì?

**Dùng khi:** Report dashboard, analytics — không cần chính xác tuyệt đối, ưu tiên không bị block.

**Không dùng khi:** Financial data, số dư, inventory — có thể đọc data sai (dirty read) hoặc bỏ sót row đang move giữa pages.

---

### Q: Reset isolation level về mặc định thế nào?

```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;  -- READ COMMITTED là default SQL Server

-- Kiểm tra level hiện tại
SELECT CASE transaction_isolation_level
    WHEN 1 THEN 'READ UNCOMMITTED'
    WHEN 2 THEN 'READ COMMITTED'
    WHEN 3 THEN 'REPEATABLE READ'
    WHEN 4 THEN 'SERIALIZABLE'
    WHEN 5 THEN 'SNAPSHOT'
END AS isolation_level
FROM sys.dm_exec_sessions
WHERE session_id = @@SPID;
```

---

### Demo 1 — Dirty Read (READ UNCOMMITTED)

```sql
-- ══ TAB A ══
BEGIN TRAN;
UPDATE orders SET status = 'PROCESSING' WHERE id = 1;
-- DỪNG, chưa COMMIT

-- ══ TAB B ══
-- Default READ COMMITTED → bị block, chờ Tab A
SELECT status FROM orders WHERE id = 1;

-- Đổi sang READ UNCOMMITTED → đọc ngay thấy 'PROCESSING'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT status FROM orders WHERE id = 1;   -- thấy 'PROCESSING' dù chưa commit!

-- ══ TAB A ══
ROLLBACK;   -- data thật vẫn là giá trị cũ → Tab B vừa đọc data không bao giờ tồn tại
```

---

### Demo 2 — Blocking (READ COMMITTED default)

```sql
-- ══ TAB A ══
BEGIN TRAN;
UPDATE orders SET amount = 9999 WHERE id = 1;
-- DỪNG

-- ══ TAB B ══
SELECT amount FROM orders WHERE id = 1;
-- Cursor xoay... TREO, chờ Tab A

-- ══ TAB A ══
COMMIT;
-- Tab B ngay lập tức trả kết quả: 9999
```

---

### Demo 3 — SNAPSHOT (không block, đọc version cũ)

```sql
-- Bật 1 lần cho DB
ALTER DATABASE learning_db SET ALLOW_SNAPSHOT_ISOLATION ON;
```

**Step 1** — Tab B bắt đầu SNAPSHOT transaction:
```sql
-- ══ TAB B ══
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRAN;
SELECT amount FROM orders WHERE id = 1;
-- Thấy: GIÁ TRỊ GỐC (snapshot chụp tại đây)
```

**Step 2** — Tab A update chưa commit:
```sql
-- ══ TAB A ══
BEGIN TRAN;
UPDATE orders SET amount = 88888 WHERE id = 1;
-- DỪNG
```

**Step 3** — Tab B đọc lại, không bị block:
```sql
-- ══ TAB B ══
SELECT amount FROM orders WHERE id = 1;
-- Thấy: GIÁ TRỊ GỐC — không bị block, không thấy 88888 ✅
```

**Step 4** — Tab A commit:
```sql
-- ══ TAB A ══
COMMIT;
```

**Step 5** — Tab B đọc lại (vẫn transaction cũ):
```sql
-- ══ TAB B ══
SELECT amount FROM orders WHERE id = 1;
-- Thấy: VẪN GIÁ TRỊ GỐC ← snapshot gắn với BEGIN TRAN, không đổi dù Tab A commit
```

**Step 6** — Tab B commit, mở transaction mới:
```sql
-- ══ TAB B ══
COMMIT;       -- đóng snapshot cũ
BEGIN TRAN;   -- snapshot mới chụp tại đây
SELECT amount FROM orders WHERE id = 1;
-- Thấy: 88888 ✅ giờ mới thấy data mới
COMMIT;
```

**Điểm cần nhớ:**
- SNAPSHOT không bị block bởi lock của transaction khác
- Snapshot gắn với thời điểm `BEGIN TRAN` — không phải từng câu SELECT
- Phải commit + BEGIN TRAN mới mới thấy data mới nhất

---

### Demo 4 — REPEATABLE READ vs SERIALIZABLE (INSERT bị block hay không)

```sql
-- ══ TAB A ══ REPEATABLE READ
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN TRAN;
SELECT COUNT(*) FROM orders WHERE customer_id = 42;   -- kết quả: N rows

-- ══ TAB B ══
INSERT INTO orders (customer_id, product_id, amount, status)
VALUES (42, 'PROD-99', 500, 'pending');
-- REPEATABLE READ → INSERT KHÔNG bị block, chạy thành công ✅
-- → Phantom Read: Tab A đọc lại sẽ thấy N+1

-- ══ TAB A ══ đổi sang SERIALIZABLE
ROLLBACK;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRAN;
SELECT COUNT(*) FROM orders WHERE customer_id = 42;

-- ══ TAB B ══
INSERT INTO orders (customer_id, product_id, amount, status)
VALUES (42, 'PROD-99', 500, 'pending');
-- SERIALIZABLE → INSERT BỊ BLOCK, ngồi chờ Tab A commit ❌

-- ══ TAB A ══
COMMIT;   -- Tab B mới chạy được
```

---

### Q: Thực tế dùng level nào?

| Tình huống | Level |
|---|---|
| Web app thông thường | READ COMMITTED (default) |
| Report dashboard | SNAPSHOT hoặc NOLOCK |
| Chuyển tiền, tài chính | SERIALIZABLE hoặc Optimistic Locking |
| REPEATABLE READ | Ít dùng — SNAPSHOT thường thay thế được |

---

---

## NHÓM 3 — QUERY OPTIMIZATION

### Q: SARGable query là gì?

**Trả lời:** Query có thể dùng Index Seek. Non-SARGable = function/transform trên cột index → buộc Table Scan.

```sql
-- ❌ Non-SARGable — function trên cột → Table Scan
SELECT * FROM orders WHERE YEAR(created_at) = 2025;
SELECT * FROM orders WHERE CONVERT(VARCHAR, customer_id) = '42';
SELECT * FROM orders WHERE amount * 2 > 1000;

-- ✅ SARGable — index được dùng
SELECT * FROM orders
WHERE created_at >= '2025-01-01' AND created_at < '2026-01-01';
SELECT * FROM orders WHERE customer_id = 42;
SELECT * FROM orders WHERE amount > 500;
```

---

### Q: Khi nào optimizer chọn Table Scan thay vì Index Seek?

**Trả lời:**
- Khi query trả về quá nhiều rows (>20-30% bảng) → scan cả bảng nhanh hơn
- Statistics lỗi thời → optimizer estimate sai row count
- Non-SARGable query

```sql
-- Cập nhật statistics khi plan bị sai
UPDATE STATISTICS orders;
```

---

---

## NHÓM 4 — WINDOW FUNCTIONS

### Q: Lấy top 3 đơn hàng lớn nhất theo từng customer?

```sql
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
    FROM orders
)
SELECT * FROM ranked WHERE rn <= 3;
```

---

### Q: RANK() vs DENSE_RANK() khác nhau thế nào?

```sql
-- RANK:       1, 2, 2, 4  (bỏ số 3)
-- DENSE_RANK: 1, 2, 2, 3  (không bỏ)
SELECT customer_id, amount,
    RANK()       OVER (ORDER BY amount DESC) AS rnk,
    DENSE_RANK() OVER (ORDER BY amount DESC) AS dense_rnk
FROM orders;
```

---

### Q: Running total và so sánh với row trước?

```sql
-- Running total
SELECT id, amount,
    SUM(amount) OVER (ORDER BY created_at
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders;

-- So sánh với row trước (LAG) và row sau (LEAD)
SELECT id, amount,
    LAG(amount, 1)  OVER (ORDER BY created_at) AS prev_amount,
    LEAD(amount, 1) OVER (ORDER BY created_at) AS next_amount,
    amount - LAG(amount, 1) OVER (ORDER BY created_at) AS diff_from_prev
FROM orders;
```

---

---

## NHÓM 5 — CTE vs TEMP TABLE

### Q: Khi nào dùng Temp Table thay CTE?

```sql
-- CTE — optimizer không biết row count trước → plan có thể không tối ưu
WITH top_customers AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders GROUP BY customer_id
)
SELECT * FROM top_customers WHERE total > 5000;

-- Temp Table — optimizer biết row count → plan tốt hơn khi CTE phức tạp
SELECT customer_id, SUM(amount) AS total
INTO #top_customers
FROM orders GROUP BY customer_id;

CREATE INDEX idx_tmp ON #top_customers(customer_id);  -- có thể index temp table
SELECT * FROM #top_customers WHERE total > 5000;
DROP TABLE #top_customers;
```

**Dùng Temp Table khi:**
- CTE được tham chiếu nhiều lần trong query
- Cần index trên kết quả trung gian
- Row count lớn và query phức tạp

---

---

## NHÓM 6 — DESIGN (câu hỏi system design)

### Q: Database per Service pattern là gì, trade-off?

**Trả lời:**
- Mỗi microservice sở hữu DB riêng, không service nào được kết nối trực tiếp vào DB của service khác
- Giao tiếp qua API hoặc message queue

```
✅ Độc lập deploy, scale riêng từng service
✅ Chọn DB phù hợp từng service (SQL, MongoDB, Redis...)
❌ Join across services không làm được → phải dùng API composition hoặc CQRS
❌ Distributed transaction phức tạp → dùng Saga pattern
```

---

### Q: Khi nào chọn NoSQL thay SQL?

| Tình huống | Chọn |
|---|---|
| Strong consistency, relations | SQL (PostgreSQL, SQL Server) |
| High write throughput, flexible schema | MongoDB |
| Cache, session, leaderboard | Redis |
| Time-series metrics | InfluxDB / TimescaleDB |
| Full-text search | Elasticsearch |
| Write → SQL, Read → Redis/Elastic | CQRS |

---

### Q: Optimistic vs Pessimistic Locking?

```sql
-- Pessimistic: lock ngay khi đọc, không ai sửa được
BEGIN TRAN;
SELECT * FROM orders WITH (UPDLOCK, ROWLOCK) WHERE id = 1;
-- ... xử lý ...
UPDATE orders SET status = 'processing' WHERE id = 1;
COMMIT;

-- Optimistic: không lock khi đọc, check conflict khi write
ALTER TABLE orders ADD row_ver ROWVERSION;

-- Đọc không lock
SELECT id, amount, row_ver FROM orders WHERE id = 1;

-- Update: nếu row_ver khác → ai đó đã sửa → 0 rows affected → retry
UPDATE orders
SET amount = 999
WHERE id = 1 AND row_ver = 0x0000000000001234;

IF @@ROWCOUNT = 0
    PRINT 'Conflict! Cần đọc lại và retry';
```

**Dùng Pessimistic khi:** conflict xảy ra thường xuyên, critical data (tài chính).
**Dùng Optimistic khi:** conflict hiếm, ưu tiên throughput (web app thông thường).
