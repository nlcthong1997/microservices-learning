# Interview Prep — .NET Core Senior / Retail System

---

## PHẦN 1 — C# / .NET CORE FUNDAMENTALS

---

### Q: async/await hoạt động thế nào? Deadlock xảy ra khi nào?

#### Trước tiên — Thread là gì trong context này?

```
Web server nhận request → cần 1 thread để xử lý
Thread pool có giới hạn (ví dụ 100 threads)
100 request đến cùng lúc → 100 threads đều bận

Nếu mỗi thread phải NGỒI CHỜ DB (500ms):
  → 100 threads đều block, không làm gì, chỉ chờ
  → Request thứ 101 đến → không có thread → timeout

async/await giải quyết: thread KHÔNG ngồi chờ
  → Gửi query DB → "tao đi làm việc khác, xong thì gọi tao"
  → Thread trả về pool → phục vụ request khác
  → DB xong → lấy 1 thread bất kỳ từ pool để tiếp tục
```

```csharp
// ❌ Synchronous — thread BỊ BLOCK 500ms chờ DB
public Order GetOrder(int id)
{
    // Thread A đứng im, không làm gì, chỉ chờ DB trả kết quả
    var order = _db.Orders.Find(id);   // blocking 500ms
    return order;
}

// ✅ Asynchronous — thread KHÔNG BỊ BLOCK
public async Task<Order> GetOrderAsync(int id)
{
    // Thread A bắt đầu query, rồi QUAY VỀ POOL ngay
    // Trong 500ms đó, Thread A có thể xử lý request khác
    var order = await _db.Orders.FindAsync(id);
    // Thread B (bất kỳ thread nào rảnh) tiếp tục từ đây
    return order;
}
```

#### Deadlock — khi nào xảy ra?

**Nguyên nhân:** dùng `.Result` hoặc `.Wait()` để đợi async method trong ASP.NET.

```csharp
// ❌ DEADLOCK — đừng bao giờ làm thế này trong ASP.NET
public IActionResult GetOrder(int id)
{
    var order = GetOrderAsync(id).Result;   // 💀 DEADLOCK
    return Ok(order);
}
```

**Tại sao deadlock?** — giải thích từng bước:

```
ASP.NET có SynchronizationContext — một "luật" yêu cầu:
  "Sau khi await xong, phải resume trên ĐÚNG thread ban đầu"

Bước 1: Thread A (request thread) gọi GetOrderAsync(id).Result
        → .Result ra lệnh: "Thread A hãy ĐỨNG IM, chờ task hoàn thành"
        → Thread A bị block, nhưng vẫn GIỮ quyền "thread context"

Bước 2: DB query xong, await cần resume
        → SynchronizationContext nói: "phải resume trên Thread A"
        → Nhưng Thread A đang đứng im chờ .Result
        → Thread A chờ task xong
        → Task cần Thread A để xong
        → DEADLOCK — 2 bên chờ nhau mãi mãi

Kết quả: Request bị treo vĩnh viễn cho đến khi timeout
```

```csharp
// ✅ Fix 1: async all the way — CÁCH TỐT NHẤT
// Cứ có async ở dưới → phải async lên trên hết
public async Task<IActionResult> GetOrder(int id)
{
    var order = await GetOrderAsync(id);   // không block, không deadlock
    return Ok(order);
}

// ✅ Fix 2: ConfigureAwait(false) — dùng trong CLASS LIBRARY, không phải ASP.NET app
// Nói với await: "tao không cần resume trên context gốc, thread nào cũng được"
public async Task<Order> GetOrderAsync(int id)
{
    // ConfigureAwait(false) → bỏ qua luật SynchronizationContext
    // → không cần Thread A cụ thể → không deadlock dù caller dùng .Result
    var order = await _db.Orders.FindAsync(id).ConfigureAwait(false);
    return order;
}
// Lưu ý: Trong ASP.NET Controller → KHÔNG dùng ConfigureAwait(false)
//         vì mất HttpContext sau await
//         Chỉ dùng trong shared library / NuGet package
```

#### ValueTask — khi nào dùng?

```csharp
// Task luôn tạo object mới trên heap dù kết quả có ngay lập tức
// → Gọi 1 triệu lần/giây → 1 triệu object → GC pressure

// ValueTask: nếu kết quả có ngay (synchronous) → không tạo object
//            nếu cần await thật → tạo object như Task bình thường

// Ví dụ thực tế: cache-first pattern
public async ValueTask<Product> GetProductAsync(int id)
{
    // Trường hợp 1: cache hit → trả về ngay, không allocate Task object
    if (_cache.TryGetValue(id, out var cached))
        return cached;

    // Trường hợp 2: cache miss → await DB → allocate như Task bình thường
    var product = await _db.Products.FindAsync(id);
    _cache.Set(id, product, TimeSpan.FromMinutes(5));
    return product;
}

// Dùng ValueTask khi:
//   - Method rất thường xuyên trả về synchronously (cache hit 90%)
//   - Performance critical (game, high-frequency trading)
// KHÔNG dùng khi:
//   - Luôn phải await thật → dùng Task bình thường cho đơn giản
//   - ValueTask không await được nhiều lần
```

---

### Q: Dependency Injection — 3 lifetime khác nhau thế nào?

#### Hiểu bằng ví dụ nhà hàng

```
Singleton  = Bếp trưởng — 1 người duy nhất, phục vụ cả ngày, mọi bàn đều dùng chung
Scoped     = Bồi bàn    — mỗi bàn (request) có 1 bồi bàn riêng, hết bữa thì về
Transient  = Đũa dùng 1 lần — mỗi lần dùng là lấy cái mới
```

```csharp
// Singleton — tạo 1 lần khi app start, dùng mãi đến khi app stop
// Dùng cho: config, memory cache, HttpClient factory, stateless service
services.AddSingleton<IMemoryCache, MemoryCache>();
services.AddSingleton<IConfiguration>(config);

// Scoped — tạo mới mỗi HTTP request, share trong cùng request đó
// Dùng cho: DbContext, Repository, Unit of Work (cần đảm bảo 1 transaction per request)
services.AddScoped<AppDbContext>();
services.AddScoped<IOrderRepository, SqlOrderRepository>();

// Transient — tạo mới mỗi lần được inject
// Dùng cho: lightweight stateless service, helper không có state
services.AddTransient<IEmailSender, SmtpEmailSender>();
services.AddTransient<IValidator<Order>, OrderValidator>();
```

#### Captive Dependency — lỗi phổ biến nhất với DI

```
Vấn đề:
  Singleton có lifetime MÃI MÃI
  Scoped    có lifetime 1 REQUEST

  Nếu Singleton inject Scoped:
    → Singleton giữ reference đến Scoped object
    → Request 1 xong → Scoped lẽ ra phải chết
    → Nhưng Singleton vẫn giữ nó → Scoped KHÔNG CHẾT
    → Request 2 đến → Singleton PHÁT LẠI Scoped cũ của Request 1
    → DbContext của Request 1 bị dùng cho Request 2 → DATA RACE
```

```csharp
// ❌ VÍ DỤ BUG — OrderNotificationService là Singleton
public class OrderNotificationService   // Singleton
{
    private readonly IOrderRepository _repo;  // Scoped — BUG!

    // Constructor inject: Scoped bị "nhốt" trong Singleton mãi mãi
    public OrderNotificationService(IOrderRepository repo)
    {
        _repo = repo;  // lần này là DbContext của Request 1
        // Request 2, 3, 4... đều dùng DbContext này → wrong data, thread-safety issues
    }
}

// ✅ Fix — inject IServiceScopeFactory, tự tạo scope khi cần
public class OrderNotificationService   // Singleton
{
    private readonly IServiceScopeFactory _scopeFactory;

    public OrderNotificationService(IServiceScopeFactory scopeFactory)
    {
        _scopeFactory = scopeFactory;  // Factory là Singleton → OK
    }

    public async Task ProcessAsync()
    {
        // Tự tạo scope → tạo DbContext mới → dùng → dispose
        using var scope = _scopeFactory.CreateScope();
        var repo = scope.ServiceProvider.GetRequiredService<IOrderRepository>();
        // repo dùng DbContext mới, sạch sẽ
        await repo.DoSomethingAsync();
    }   // scope Dispose → DbContext bị hủy đúng cách
}
```

**ASP.NET Core tự detect lỗi này:**
```
System.InvalidOperationException:
  Cannot consume scoped service 'IOrderRepository'
  from singleton 'OrderNotificationService'.
  → App sẽ crash khi start nếu bật ValidateScopes (mặc định trong Development)
```

---

### Q: IQueryable vs IEnumerable — khác nhau thế nào?

#### Khái niệm cốt lõi

```
IEnumerable = "Lấy data lên C# rồi xử lý tại đây"
IQueryable  = "Mô tả ý định, để DB xử lý, chỉ lấy kết quả cuối"
```

```csharp
// IEnumerable — thực thi TRONG BỘ NHỚ C#
// Dòng này chạy SQL ngay: SELECT * FROM orders (lấy HẾT)
IEnumerable<Order> allOrders = _db.Orders.ToList();

// Filter này chạy trong C#, data đã có sẵn trong memory
var pending = allOrders.Where(o => o.Status == "pending");
// Vấn đề: 100k orders → lấy 100k lên memory → filter ra 100 cái → lãng phí

// ──────────────────────────────────────────

// IQueryable — xây dựng câu SQL, CHƯA CHẠY GÌ CẢ
IQueryable<Order> query = _db.Orders;           // chưa có SQL nào chạy
query = query.Where(o => o.Status == "pending");// thêm điều kiện vào SQL
query = query.OrderBy(o => o.CreatedAt);        // thêm ORDER BY
query = query.Take(10);                         // thêm TOP 10

// CHỈ ĐẾN ĐÂY mới thực sự chạy SQL:
var result = await query.ToListAsync();
// SQL: SELECT TOP 10 * FROM orders WHERE status='pending' ORDER BY created_at
// Chỉ 10 rows được lấy lên → tiết kiệm memory và network
```

#### Bug thực tế — hay gặp trong dự án

```csharp
// ❌ Repository trả về IEnumerable — lấy hết data lên memory
public IEnumerable<Order> GetByCustomer(int customerId)
{
    return _db.Orders.Where(o => o.CustomerId == customerId);
    // Trông có vẻ OK nhưng thực ra: khi caller gọi hàm này
    // → SQL chạy: SELECT * FROM orders WHERE customer_id = ?
    // → Lấy HẾT order của customer lên memory
}

// Caller tưởng chỉ lấy 10:
var orders = _repo.GetByCustomer(customerId).Take(10).ToList();
// Take(10) chạy trên IEnumerable trong C# → đã lấy hết lên rồi mới lấy 10
// Customer có 10.000 orders → 10.000 objects trong memory → chỉ dùng 10

// ✅ Fix — trả về IQueryable để caller compose tiếp
public IQueryable<Order> GetByCustomer(int customerId)
{
    return _db.Orders.Where(o => o.CustomerId == customerId);
    // Chưa chạy SQL, chỉ là "ý định"
}
// Caller:
var orders = await _repo.GetByCustomer(customerId).Take(10).ToListAsync();
// SQL: SELECT TOP 10 * FROM orders WHERE customer_id = ? ← đúng rồi

// ✅ Hoặc — materialize luôn trong repository, trả List cụ thể
public async Task<List<Order>> GetByCustomerAsync(int customerId, int take = 20)
{
    return await _db.Orders
        .Where(o => o.CustomerId == customerId)
        .OrderByDescending(o => o.CreatedAt)
        .Take(take)
        .ToListAsync();   // materialize ngay → trả List, không phải IQueryable
}
```

#### Khi nào dùng cái nào?

```
IQueryable  → trong Repository, khi muốn cho caller thêm filter/sort/paging
              Cẩn thận: DbContext phải còn sống khi caller enumerate

List/Array  → khi repository đã biết đủ điều kiện, materialize luôn
              An toàn hơn, không lo DbContext disposed

IEnumerable → khi data đã có trong memory (không phải từ DB)
              Ví dụ: filter list từ config, in-memory collection
```

---

### Q: record vs class vs struct — dùng khi nào?

#### class — reference type (phổ biến nhất)

```csharp
class OrderDto
{
    public int Id { get; set; }
    public string Status { get; set; }
}

// Reference type: biến chứa địa chỉ bộ nhớ, không phải giá trị
var a = new OrderDto { Id = 1, Status = "pending" };
var b = a;           // b trỏ vào CÙNG object với a
b.Status = "done";
Console.WriteLine(a.Status);  // "done" — a cũng bị thay đổi!

// Equality: so sánh địa chỉ bộ nhớ
var x = new OrderDto { Id = 1 };
var y = new OrderDto { Id = 1 };
Console.WriteLine(x == y);    // FALSE — 2 object khác nhau dù cùng giá trị
```

#### record — reference type + value equality + immutable

```csharp
// record: tự generate Equals, GetHashCode, ToString, Deconstruct
record OrderDto(int Id, string Status);
// Tương đương class với Id, Status là init-only property

var a = new OrderDto(1, "pending");
var b = new OrderDto(1, "pending");
Console.WriteLine(a == b);     // TRUE — record so sánh theo GIÁ TRỊ

// Immutable — không thể thay đổi sau khi tạo
// a.Status = "done";  // ❌ Compile error

// "with" expression — tạo bản copy với field khác
var c = a with { Status = "done" };  // a vẫn là "pending", c là "done"

// Dùng record cho: DTO, Command, Query, Response — data truyền đi không cần thay đổi
record PlaceOrderCommand(int CustomerId, int ProductId, int Quantity);
record OrderResponse(int Id, string Status, decimal Total);
record Money(decimal Amount, string Currency);  // Value Object
```

#### struct — value type (dùng ít hơn)

```csharp
struct Point
{
    public int X { get; set; }
    public int Y { get; set; }
}

// Value type: biến chứa TRỰC TIẾP giá trị, không phải địa chỉ
var a = new Point { X = 1, Y = 2 };
var b = a;           // b là bản SAO của a
b.X = 99;
Console.WriteLine(a.X);  // 1 — a không bị ảnh hưởng

// Stack allocated — không tạo heap object → không tạo GC pressure
// Dùng cho: small, frequently created, value-semantics objects

// Dùng struct cho:
//   - Tọa độ: Point, Vector2, Rectangle
//   - Màu sắc: Color (R, G, B, A)
//   - DateTime, Guid (built-in struct)
//   - KHÔNG dùng cho object lớn → copy tốn kém
```

#### Bảng quyết định

```
Cần gì?                                    Dùng
─────────────────────────────────────────────────────
Entity (Order, Product, User)              class
Service, Repository, Controller            class
DTO, Response model, Command, Query        record
Value Object (Money, Address)              record
Small value (Point, Color, Size)           struct
Tập hợp data có thể thay đổi              class
Immutable data truyền qua layer            record
```



---

## PHẦN 2 — OOP + SOLID

---

### Q: Giải thích SOLID với ví dụ thực tế retail

**S — Single Responsibility**
```csharp
// ❌ Vi phạm — OrderService làm quá nhiều thứ
class OrderService
{
    public void PlaceOrder(Order order) { /* business logic */ }
    public void SendEmail(Order order)  { /* gửi email */ }         // không phải việc của nó
    public void SaveToDb(Order order)   { /* DB operation */ }      // không phải việc của nó
    public decimal CalculateTax(Order order) { /* tính thuế */ }    // không phải việc của nó
}

// ✅ Tách ra
class OrderService     { public void PlaceOrder(Order order) { } }
class EmailService     { public void SendOrderConfirmation(Order order) { } }
class OrderRepository  { public void Save(Order order) { } }
class TaxCalculator    { public decimal Calculate(Order order) { } }
```

**O — Open/Closed**
```csharp
// ❌ Mỗi loại discount phải sửa code cũ
class PriceCalculator
{
    public decimal Calculate(Order order, string discountType)
    {
        if (discountType == "vip") return order.Total * 0.9m;
        if (discountType == "sale") return order.Total * 0.8m;
        // Thêm flash-sale → phải sửa class này → vi phạm Open/Closed
    }
}

// ✅ Mở rộng bằng cách thêm class mới, không sửa class cũ
interface IDiscountStrategy
{
    decimal Apply(decimal total);
}
class VipDiscount   : IDiscountStrategy { public decimal Apply(decimal t) => t * 0.9m; }
class SaleDiscount  : IDiscountStrategy { public decimal Apply(decimal t) => t * 0.8m; }
class FlashDiscount : IDiscountStrategy { public decimal Apply(decimal t) => t * 0.5m; }

class PriceCalculator
{
    public decimal Calculate(Order order, IDiscountStrategy discount)
        => discount.Apply(order.Total);  // không cần sửa khi thêm loại discount mới
}
```

**L — Liskov Substitution**
```csharp
// ❌ Vi phạm — subclass thay đổi contract của base class
class Rectangle
{
    public virtual int Width  { get; set; }
    public virtual int Height { get; set; }
    public int Area() => Width * Height;
}
class Square : Rectangle
{
    public override int Width  { set { base.Width = base.Height = value; } get => base.Width; }
    public override int Height { set { base.Width = base.Height = value; } get => base.Height; }
}
// Rectangle r = new Square(); r.Width = 5; r.Height = 3;
// Expected: Area = 15, Actual: Area = 9 → LSP violated

// ✅ Fix: đừng inherit nếu subclass không thể thay thế base class
interface IShape { int Area(); }
class Rectangle : IShape { ... }
class Square    : IShape { ... }
```

**I — Interface Segregation**
```csharp
// ❌ Interface quá to — không phải ai cũng cần hết
interface IOrderService
{
    void PlaceOrder(Order o);
    void CancelOrder(int id);
    Report GenerateReport();      // chỉ admin cần
    void ExportToCsv();           // chỉ admin cần
}

// ✅ Tách thành interface nhỏ
interface IOrderService  { void PlaceOrder(Order o); void CancelOrder(int id); }
interface IOrderReporting { Report GenerateReport(); void ExportToCsv(); }
```

**D — Dependency Inversion**
```csharp
// ❌ Depend on concrete — khó test, khó đổi implementation
class OrderService
{
    private SqlOrderRepository _repo = new SqlOrderRepository();  // hardcode
}

// ✅ Depend on abstraction
class OrderService
{
    private readonly IOrderRepository _repo;
    public OrderService(IOrderRepository repo) { _repo = repo; }  // inject từ ngoài
    // Test: inject MockOrderRepository
    // Production: inject SqlOrderRepository
}
```

---

## PHẦN 3 — DESIGN PATTERNS

---

### Q: Repository Pattern là gì, tại sao cần?

#### Vấn đề không có Repository

```csharp
// ❌ Controller gọi DB trực tiếp — phổ biến ở junior
[ApiController]
public class OrderController : ControllerBase
{
    private readonly AppDbContext _db;

    [HttpGet("{id}")]
    public async Task<IActionResult> GetOrder(int id)
    {
        // SQL logic nằm thẳng trong controller
        var order = await _db.Orders
            .Include(o => o.Items)
            .FirstOrDefaultAsync(o => o.Id == id && o.DeletedAt == null);

        if (order == null) return NotFound();
        return Ok(order);
    }

    [HttpGet("customer/{customerId}")]
    public async Task<IActionResult> GetByCustomer(int customerId)
    {
        // Cùng logic filter, lặp lại ở đây
        var orders = await _db.Orders
            .Where(o => o.CustomerId == customerId && o.DeletedAt == null)
            .OrderByDescending(o => o.CreatedAt)
            .ToListAsync();
        return Ok(orders);
    }
}
// Vấn đề:
// 1. Logic filter (DeletedAt == null) lặp lại ở nhiều chỗ
// 2. Muốn đổi từ SQL Server sang MongoDB → sửa tất cả controller
// 3. Không thể unit test controller mà không cần DB thật
```

#### Repository — abstract hóa data access

```csharp
// Interface — định nghĩa "contract", không quan tâm implement thế nào
public interface IOrderRepository
{
    Task<Order?> GetByIdAsync(int id);
    Task<List<Order>> GetByCustomerAsync(int customerId, int page, int pageSize);
    Task<bool> ExistsAsync(int id);
    void Add(Order order);
    void Remove(Order order);
}

// Implementation SQL Server — chỉ file này biết đến EF Core / SQL
public class SqlOrderRepository : IOrderRepository
{
    private readonly AppDbContext _db;

    public SqlOrderRepository(AppDbContext db) { _db = db; }

    public async Task<Order?> GetByIdAsync(int id)
        => await _db.Orders
            .Include(o => o.Items)
            .FirstOrDefaultAsync(o => o.Id == id && o.DeletedAt == null);
    //                                               ↑
    //                               Logic soft-delete tập trung ở đây, không lặp lại

    public async Task<List<Order>> GetByCustomerAsync(int customerId, int page, int pageSize)
        => await _db.Orders
            .Where(o => o.CustomerId == customerId && o.DeletedAt == null)
            .OrderByDescending(o => o.CreatedAt)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();

    public void Add(Order order) => _db.Orders.Add(order);
    public void Remove(Order order) => order.DeletedAt = DateTime.UtcNow; // soft delete
    public async Task<bool> ExistsAsync(int id) => await _db.Orders.AnyAsync(o => o.Id == id);
}

// Controller giờ sạch — không biết SQL tồn tại
[ApiController]
public class OrderController : ControllerBase
{
    private readonly IOrderRepository _repo;

    [HttpGet("{id}")]
    public async Task<IActionResult> GetOrder(int id)
    {
        var order = await _repo.GetByIdAsync(id);
        if (order == null) return NotFound();
        return Ok(order);
    }
}

// Unit test — inject fake repository, không cần DB
public class OrderControllerTests
{
    [Fact]
    public async Task GetOrder_NotFound_Returns404()
    {
        var mockRepo = new Mock<IOrderRepository>();
        mockRepo.Setup(r => r.GetByIdAsync(999)).ReturnsAsync((Order?)null);

        var controller = new OrderController(mockRepo.Object);
        var result = await controller.GetOrder(999);

        Assert.IsType<NotFoundResult>(result);
        // Test chạy không cần DB thật, không cần Docker, không cần network
    }
}
```

---

### Q: Unit of Work — tại sao cần khi đã có Repository?

#### Vấn đề: nhiều repository, 1 transaction

```csharp
// ❌ Không có Unit of Work — 2 save riêng lẻ
public async Task PlaceOrderAsync(CreateOrderCommand cmd)
{
    var product = await _productRepo.GetByIdAsync(cmd.ProductId);
    product.Stock -= cmd.Quantity;
    await _productRepo.SaveAsync();    // commit 1: stock đã trừ

    var order = new Order(cmd);
    await _orderRepo.AddAsync(order);
    await _orderRepo.SaveAsync();      // commit 2: order được tạo

    // Nếu commit 2 fail → stock đã bị trừ nhưng order không có → inconsistent
    // 2 transaction riêng lẻ → không atomic
}

// ✅ Unit of Work — tất cả trong 1 transaction
public async Task PlaceOrderAsync(CreateOrderCommand cmd)
{
    var product = await _uow.Products.GetByIdAsync(cmd.ProductId);
    if (product.Stock < cmd.Quantity)
        throw new InsufficientStockException();

    product.Stock -= cmd.Quantity;      // chỉ modify trong memory, chưa save
    _uow.Products.Update(product);

    var order = new Order(cmd.CustomerId, cmd.ProductId, cmd.Quantity);
    _uow.Orders.Add(order);             // chỉ queue thêm vào context, chưa save

    await _uow.SaveChangesAsync();
    // MỘT lần commit duy nhất:
    // BEGIN TRAN
    //   UPDATE products SET stock = stock - 1 WHERE id = ?
    //   INSERT INTO orders (...)
    // COMMIT
    // Nếu bất kỳ bước nào fail → ROLLBACK toàn bộ → consistent
}
```

```csharp
// Interface Unit of Work
public interface IUnitOfWork : IDisposable
{
    IOrderRepository   Orders   { get; }
    IProductRepository Products { get; }
    Task<int> SaveChangesAsync(CancellationToken ct = default);
}

// Implementation — thực ra DbContext của EF Core đã là Unit of Work
public class AppUnitOfWork : IUnitOfWork
{
    private readonly AppDbContext _db;

    public AppUnitOfWork(AppDbContext db)
    {
        _db = db;
        Orders   = new SqlOrderRepository(db);
        Products = new SqlProductRepository(db);
    }

    public IOrderRepository   Orders   { get; }
    public IProductRepository Products { get; }

    // Tất cả thay đổi từ cả 2 repository đều flush cùng lúc ở đây
    public Task<int> SaveChangesAsync(CancellationToken ct = default)
        => _db.SaveChangesAsync(ct);

    public void Dispose() => _db.Dispose();
}
```

---

### Q: CQRS — tách Command và Query, tại sao?

#### Vấn đề khi dùng chung 1 model

```
Bài toán thực tế:
  - Trang "Danh sách đơn hàng" cần: order + customer name + product name + tổng tiền
    → JOIN 3 bảng, cần DTO flat cho UI

  - Đặt hàng mới cần: validate stock, validate payment, tạo order, publish event
    → Cần domain model đầy đủ với business rules

  Dùng chung 1 model Order:
    → Read phải load toàn bộ entity kể cả field không cần
    → Write phải map qua lại giữa DTO và domain model
    → Model trở nên phức tạp, thỏa hiệp cả 2 hướng
```

#### CQRS — tách biệt hoàn toàn

```
Command (Write side)         Query (Read side)
──────────────────           ──────────────────
Validate business rules      Tối ưu cho đọc nhanh
Domain model phức tạp        DTO flat, denormalized
1 transaction                Có thể dùng Read Replica
Publish domain events        Không có side effect
Chậm hơn (validation)        Nhanh hơn (no overhead)
```

```csharp
// ══ COMMAND SIDE — đặt hàng ══════════════════════════════════════

// Command = ý định thay đổi hệ thống
// Immutable, không trả về data (hoặc chỉ trả ID)
public record PlaceOrderCommand(
    int    CustomerId,
    int    ProductId,
    int    Quantity,
    string ShippingAddress
);

public class PlaceOrderHandler : IRequestHandler<PlaceOrderCommand, int>
{
    private readonly IUnitOfWork    _uow;
    private readonly IEventBus      _events;

    public async Task<int> Handle(PlaceOrderCommand cmd, CancellationToken ct)
    {
        // 1. Load domain objects
        var product  = await _uow.Products.GetByIdAsync(cmd.ProductId);
        var customer = await _uow.Customers.GetByIdAsync(cmd.CustomerId);

        // 2. Business rules — nằm trong Domain, không phải Handler
        if (product.Stock < cmd.Quantity)
            throw new InsufficientStockException(product.Name, product.Stock);

        if (!customer.CanPlaceOrder())
            throw new CustomerBlockedException(customer.Id);

        // 3. Thay đổi state
        product.ReserveStock(cmd.Quantity);      // domain method
        var order = Order.Create(cmd);           // factory method trong domain

        _uow.Products.Update(product);
        _uow.Orders.Add(order);

        // 4. Atomic commit
        await _uow.SaveChangesAsync(ct);

        // 5. Publish event — sau khi commit thành công
        await _events.PublishAsync(new OrderPlacedEvent(order.Id, cmd.ProductId));

        return order.Id;
    }
}

// ══ QUERY SIDE — lấy danh sách đơn hàng ════════════════════════

// Query = câu hỏi, không thay đổi gì
public record GetOrdersQuery(int CustomerId, int Page = 1, int PageSize = 20);

// DTO — flat, tối ưu cho UI, không cần domain model
public record OrderSummaryDto(
    int     Id,
    string  ProductName,
    int     Quantity,
    decimal Total,
    string  Status,
    DateTime CreatedAt
);

public class GetOrdersHandler : IRequestHandler<GetOrdersQuery, PagedResult<OrderSummaryDto>>
{
    private readonly AppDbContext _db;  // Query side có thể truy cập DB trực tiếp

    public async Task<PagedResult<OrderSummaryDto>> Handle(GetOrdersQuery q, CancellationToken ct)
    {
        // Project thẳng sang DTO — EF Core tự JOIN, không load toàn bộ entity
        var query = _db.Orders
            .Where(o => o.CustomerId == q.CustomerId && o.DeletedAt == null)
            .Select(o => new OrderSummaryDto(
                o.Id,
                o.Product.Name,        // EF Core tự JOIN vào products table
                o.Quantity,
                o.Quantity * o.Product.Price,
                o.Status.ToString(),
                o.CreatedAt
            ));

        var total = await query.CountAsync(ct);
        var items = await query
            .OrderByDescending(o => o.CreatedAt)
            .Skip((q.Page - 1) * q.PageSize)
            .Take(q.PageSize)
            .ToListAsync(ct);

        return new PagedResult<OrderSummaryDto>(items, total, q.Page, q.PageSize);
    }
}

// ══ MediatR — dispatch tới đúng handler ══════════════════════

// Controller không biết handler nào xử lý — chỉ gửi qua mediator
[ApiController]
[Route("api/v1/orders")]
public class OrderController : ControllerBase
{
    private readonly IMediator _mediator;

    // Đặt hàng — gửi Command
    [HttpPost]
    public async Task<IActionResult> PlaceOrder(PlaceOrderRequest req)
    {
        var orderId = await _mediator.Send(new PlaceOrderCommand(
            req.CustomerId, req.ProductId, req.Quantity, req.ShippingAddress));
        return CreatedAtAction(nameof(GetOrders), new { id = orderId }, new { id = orderId });
    }

    // Xem đơn hàng — gửi Query
    [HttpGet("my-orders")]
    public async Task<IActionResult> GetOrders([FromQuery] int page = 1)
    {
        var result = await _mediator.Send(new GetOrdersQuery(CurrentUserId, page));
        return Ok(result);
    }
}
```

---

### Q: Strategy Pattern — bài toán pricing trong retail

#### Vấn đề không có Strategy

```csharp
// ❌ Mỗi loại giá là 1 if/else — mỗi lần thêm loại mới phải sửa hàm này
public decimal CalculatePrice(Product product, Customer customer, string voucherCode)
{
    decimal price = product.BasePrice;

    if (customer.IsVip)
        price *= 0.85m;                          // VIP giảm 15%
    else if (IsFlashSaleActive(product.Id))
        price *= 0.5m;                           // Flash sale giảm 50%
    else if (voucherCode == "SUMMER2025")
        price *= 0.9m;                           // Voucher giảm 10%
    else if (customer.OrderCount > 10)
        price *= 0.95m;                          // Loyal customer giảm 5%
    // Thêm "birthday discount" → phải sửa hàm này → vi phạm Open/Closed
    // 10 loại discount → hàm này 50 dòng → nightmare

    return price;
}
```

#### Strategy Pattern — mỗi loại giá là 1 class riêng

```csharp
// Bước 1: Định nghĩa interface chung cho tất cả pricing strategy
public interface IPricingStrategy
{
    // Tên strategy để log/debug
    string Name { get; }

    // Priority: strategy nào có priority cao hơn sẽ được chọn trước
    int Priority { get; }

    // Strategy này có áp dụng không?
    bool IsApplicable(PricingContext context);

    // Tính giá
    decimal Apply(decimal originalPrice, PricingContext context);
}

// Context chứa mọi thông tin cần thiết để quyết định
public record PricingContext(
    Customer  Customer,
    Product   Product,
    int       Quantity,
    string?   VoucherCode,
    DateTime  Now
);

// ──────────────────────────────────────────────────────
// Bước 2: Implement từng strategy — mỗi class 1 trách nhiệm

public class VipPricingStrategy : IPricingStrategy
{
    public string Name     => "VIP Customer";
    public int    Priority => 100;   // ưu tiên cao

    public bool IsApplicable(PricingContext ctx)
        => ctx.Customer.IsVip;

    public decimal Apply(decimal price, PricingContext ctx)
        => price * 0.85m;   // 15% off cho VIP
}

public class FlashSalePricingStrategy : IPricingStrategy
{
    private readonly IFlashSaleRepository _flashRepo;

    public string Name     => "Flash Sale";
    public int    Priority => 90;

    public bool IsApplicable(PricingContext ctx)
    {
        var sale = _flashRepo.GetActiveFlashSale(ctx.Product.Id, ctx.Now);
        return sale != null;
    }

    public decimal Apply(decimal price, PricingContext ctx)
    {
        var sale = _flashRepo.GetActiveFlashSale(ctx.Product.Id, ctx.Now);
        return price * (1 - sale!.DiscountPercent / 100m);
    }
}

public class VoucherPricingStrategy : IPricingStrategy
{
    private readonly IVoucherRepository _voucherRepo;

    public string Name     => "Voucher";
    public int    Priority => 80;

    public bool IsApplicable(PricingContext ctx)
        => ctx.VoucherCode != null &&
           _voucherRepo.IsValid(ctx.VoucherCode, ctx.Customer.Id, ctx.Now);

    public decimal Apply(decimal price, PricingContext ctx)
    {
        var voucher = _voucherRepo.Get(ctx.VoucherCode!);
        return price - voucher.DiscountAmount;   // fixed amount discount
    }
}

public class LoyalCustomerStrategy : IPricingStrategy
{
    public string Name     => "Loyal Customer";
    public int    Priority => 50;

    public bool IsApplicable(PricingContext ctx)
        => ctx.Customer.TotalOrders > 10;

    public decimal Apply(decimal price, PricingContext ctx)
        => price * 0.95m;   // 5% off
}

// ──────────────────────────────────────────────────────
// Bước 3: Pricing Engine — chọn strategy phù hợp

public class PricingEngine
{
    private readonly IEnumerable<IPricingStrategy> _strategies;

    // DI inject tất cả strategies đã đăng ký
    public PricingEngine(IEnumerable<IPricingStrategy> strategies)
    {
        _strategies = strategies;
    }

    public PricingResult Calculate(PricingContext ctx)
    {
        // Tìm strategy có priority cao nhất và applicable
        var strategy = _strategies
            .Where(s => s.IsApplicable(ctx))
            .OrderByDescending(s => s.Priority)
            .FirstOrDefault();

        decimal finalPrice;
        string  appliedStrategy;

        if (strategy != null)
        {
            finalPrice      = strategy.Apply(ctx.Product.BasePrice * ctx.Quantity, ctx);
            appliedStrategy = strategy.Name;
        }
        else
        {
            finalPrice      = ctx.Product.BasePrice * ctx.Quantity;  // giá gốc
            appliedStrategy = "Normal";
        }

        return new PricingResult(finalPrice, appliedStrategy,
            OriginalPrice: ctx.Product.BasePrice * ctx.Quantity,
            Saving: ctx.Product.BasePrice * ctx.Quantity - finalPrice);
    }
}

// ──────────────────────────────────────────────────────
// Bước 4: Đăng ký trong DI container

services.AddScoped<IPricingStrategy, VipPricingStrategy>();
services.AddScoped<IPricingStrategy, FlashSalePricingStrategy>();
services.AddScoped<IPricingStrategy, VoucherPricingStrategy>();
services.AddScoped<IPricingStrategy, LoyalCustomerStrategy>();
services.AddScoped<PricingEngine>();

// Thêm "Birthday Discount" sau này:
services.AddScoped<IPricingStrategy, BirthdayDiscountStrategy>();
// ← Chỉ thêm 1 dòng này, không sửa bất kỳ code nào khác ✅
```

---

### Q: Outbox Pattern — đảm bảo không mất event khi crash

#### Vấn đề: 2 operation riêng lẻ không atomic

```csharp
// ❌ Tình huống crash giữa chừng
public async Task PlaceOrderAsync(CreateOrderCommand cmd)
{
    // Bước 1: lưu order vào DB
    var order = new Order(cmd);
    await _db.Orders.AddAsync(order);
    await _db.SaveChangesAsync();
    // ✅ Order đã commit vào DB

    // ← SERVER CRASH Ở ĐÂY (điện cúp, OOM, network timeout...)

    // Bước 2: publish event lên Kafka — KHÔNG BAO GIỜ CHẠY
    await _kafka.PublishAsync("order-events", new OrderPlacedEvent(order.Id));
    // Inventory service không biết có order mới → không reserve stock
    // → Order tồn tại trong DB nhưng system không xử lý → inconsistent
}
```

#### Outbox Pattern — ghi event cùng transaction với data

```
Ý tưởng:
  Thay vì publish trực tiếp lên Kafka (network call, có thể fail)
  → Ghi event vào bảng outbox TRONG CÙNG transaction với data
  → Atomic: hoặc cả 2 thành công, hoặc cả 2 rollback
  → Một process riêng (relay) đọc outbox và publish lên Kafka
  → Nếu publish fail → retry, event vẫn còn trong outbox
```

```csharp
// Bảng outbox
public class OutboxMessage
{
    public Guid     Id        { get; set; } = Guid.NewGuid();
    public string   EventType { get; set; } = "";    // "order.placed"
    public string   Topic     { get; set; } = "";    // "order-events"
    public string   Payload   { get; set; } = "";    // JSON
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? ProcessedAt { get; set; }       // null = chưa publish
    public int      RetryCount   { get; set; }
}

// ✅ Outbox Pattern
public async Task PlaceOrderAsync(CreateOrderCommand cmd)
{
    // Mở transaction bao gồm CẢ HAI operation
    await using var tx = await _db.Database.BeginTransactionAsync();
    try
    {
        // 1. Lưu order
        var order = new Order(cmd.CustomerId, cmd.ProductId, cmd.Quantity);
        _db.Orders.Add(order);

        // 2. Ghi event vào outbox — cùng transaction
        _db.OutboxMessages.Add(new OutboxMessage
        {
            EventType = "order.placed",
            Topic     = "order-events",
            Payload   = JsonSerializer.Serialize(new
            {
                orderId   = order.Id,
                productId = cmd.ProductId,
                quantity  = cmd.Quantity,
                timestamp = DateTime.UtcNow
            })
        });

        // Commit CẢ HAI trong 1 transaction
        await _db.SaveChangesAsync();
        await tx.CommitAsync();
        // Nếu đến đây thành công → order có trong DB, event có trong outbox
        // Nếu fail trước đây → rollback cả 2, không inconsistent
    }
    catch
    {
        await tx.RollbackAsync();
        throw;
    }
}

// ══ Relay — process riêng đọc outbox và publish ══════════════

// Chạy như background service, mỗi 2 giây scan outbox
public class OutboxRelayService : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await ProcessPendingMessages();
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }

    private async Task ProcessPendingMessages()
    {
        // Lấy 100 message chưa publish, theo thứ tự
        var messages = await _db.OutboxMessages
            .Where(m => m.ProcessedAt == null && m.RetryCount < 5)
            .OrderBy(m => m.CreatedAt)
            .Take(100)
            .ToListAsync();

        foreach (var msg in messages)
        {
            try
            {
                // Publish lên Kafka
                await _kafka.PublishAsync(msg.Topic, msg.Payload);

                // Đánh dấu đã xử lý
                msg.ProcessedAt = DateTime.UtcNow;
                await _db.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                // Publish fail → tăng retry count, thử lại lần sau
                msg.RetryCount++;
                _logger.LogWarning(ex, "Failed to publish {EventType}, retry {Count}", 
                    msg.EventType, msg.RetryCount);
                await _db.SaveChangesAsync();
            }
        }
    }
}
```

**Debezium thay Relay Cronjob:**
```
Thay vì cronjob polling mỗi 2 giây
→ Debezium đọc WAL/binlog của DB
→ Khi có INSERT vào outbox → Debezium tự publish lên Kafka ngay lập tức (<100ms)
→ Không tốn query DB, không delay
→ Cần setup Kafka Connect + Debezium connector
```



---

## PHẦN 4 — CLEAN ARCHITECTURE

---

### Q: Giải thích Clean Architecture, tại sao dùng?

```
Dependency rule: mũi tên chỉ vào trong — layer ngoài depend vào layer trong
Layer trong KHÔNG biết layer ngoài tồn tại

┌──────────────────────────────────────┐
│  Infrastructure (DB, Email, HTTP)     │  depend → Application
│  ┌──────────────────────────────┐    │
│  │  API / Presentation (MVC)    │    │  depend → Application
│  │  ┌────────────────────┐      │    │
│  │  │  Application       │      │    │  depend → Domain
│  │  │  ┌──────────┐      │      │    │
│  │  │  │  Domain  │      │      │    │  không depend vào ai
│  │  │  └──────────┘      │      │    │
│  │  └────────────────────┘      │    │
│  └──────────────────────────────┘    │
└──────────────────────────────────────┘
```

**Domain Layer** — trái tim của hệ thống:
```csharp
// Entity — business logic nằm trong Entity, không phải service
public class Order
{
    public int Id { get; private set; }
    public OrderStatus Status { get; private set; }
    public decimal Total { get; private set; }

    public void Confirm()
    {
        if (Status != OrderStatus.Pending)
            throw new InvalidOperationException("Chỉ confirm được order đang Pending");
        Status = OrderStatus.Confirmed;
        AddDomainEvent(new OrderConfirmedEvent(Id));
    }
    // Domain KHÔNG reference DbContext, không reference EmailService
}

// Value Object — immutable, equality by value
public record Money(decimal Amount, string Currency)
{
    public Money Add(Money other)
    {
        if (Currency != other.Currency) throw new InvalidOperationException();
        return new Money(Amount + other.Amount, Currency);
    }
}
```

**Application Layer** — orchestrate use cases:
```csharp
// Command Handler — gọi domain, không chứa business logic
public class ConfirmOrderHandler : ICommandHandler<ConfirmOrderCommand>
{
    private readonly IOrderRepository _repo;  // interface — không biết SQL hay MongoDB
    private readonly IEventBus _eventBus;     // interface — không biết Kafka hay RabbitMQ

    public async Task Handle(ConfirmOrderCommand cmd)
    {
        var order = await _repo.GetByIdAsync(cmd.OrderId);
        order.Confirm();  // business logic trong domain
        await _repo.SaveAsync(order);
        await _eventBus.PublishAsync(order.DomainEvents);
    }
}
```

**Infrastructure Layer** — implement interface:
```csharp
// Concrete implementation — chỉ layer này biết SQL Server cụ thể
public class SqlOrderRepository : IOrderRepository
{
    private readonly AppDbContext _db;
    public async Task<Order?> GetByIdAsync(int id) => await _db.Orders.FindAsync(id);
    public async Task SaveAsync(Order order)
    {
        _db.Orders.Update(order);
        await _db.SaveChangesAsync();
    }
}
```

---

## PHẦN 5 — RESTFUL API

---

### Q: Thiết kế API cho retail system — các nguyên tắc

```
GET    /api/v1/products          → danh sách (idempotent)
GET    /api/v1/products/{id}     → chi tiết
POST   /api/v1/products          → tạo mới (201 Created + Location header)
PUT    /api/v1/products/{id}     → update toàn bộ (idempotent)
PATCH  /api/v1/products/{id}     → update một phần
DELETE /api/v1/products/{id}     → xóa (idempotent)

POST   /api/v1/orders            → đặt hàng
POST   /api/v1/orders/{id}/cancel → cancel (action → dùng POST)
POST   /api/v1/orders/{id}/confirm
```

**Status codes đúng:**
```
200 OK           → GET thành công, PUT/PATCH thành công
201 Created      → POST tạo mới thành công + Location: /api/orders/123
204 No Content   → DELETE thành công, PUT không trả body
400 Bad Request  → validation fail (sai format, thiếu field)
401 Unauthorized → chưa login
403 Forbidden    → đã login nhưng không có quyền
404 Not Found    → resource không tồn tại
409 Conflict     → duplicate (tạo order đã tồn tại), optimistic lock fail
422 Unprocessable→ đúng format nhưng logic fail (đặt hàng vượt stock)
500 Server Error → bug, không expose detail ra ngoài
```

**Versioning:**
```csharp
// URL versioning — phổ biến nhất, dễ nhất
GET /api/v1/products
GET /api/v2/products  // v2 có thêm field mới

// Header versioning — URL sạch hơn nhưng khó test hơn
GET /api/products
Headers: Api-Version: 2.0

// Query string — không khuyến khích cho API production
GET /api/products?version=2
```

---

### Q: Pagination — offset vs cursor

```csharp
// Offset pagination — đơn giản nhưng vấn đề với large data
GET /api/orders?page=5&pageSize=20
// SQL: SELECT * FROM orders ORDER BY id OFFSET 80 ROWS FETCH NEXT 20 ROWS ONLY
// Vấn đề: OFFSET 80 → DB vẫn đọc 80 rows đầu rồi bỏ → chậm khi page lớn
// Vấn đề: item mới insert trang 1 → mọi thứ dịch xuống → trang 2 bị lặp item

// Cursor pagination — tốt hơn cho large data + real-time feed
GET /api/orders?cursor=eyJpZCI6MTAwfQ&pageSize=20
// cursor = base64({"id": 100})
// SQL: SELECT * FROM orders WHERE id > 100 ORDER BY id LIMIT 20
// Không bị vấn đề OFFSET, consistent khi có insert mới

// Response trả về next cursor
{
  "data": [...],
  "nextCursor": "eyJpZCI6MTIwfQ",  // base64({"id": 120})
  "hasMore": true
}
```

---

## PHẦN 6 — N+1 PROBLEM VÀ EF CORE

---

### Q: N+1 là gì, cách detect và fix?

```csharp
// N+1 Problem — 1 query lấy list, N query lấy từng related entity
var orders = await _db.Orders.ToListAsync();           // 1 query: SELECT * FROM orders
foreach (var order in orders)
{
    Console.WriteLine(order.Customer.Name);            // N queries: SELECT * FROM customers WHERE id = ?
    // 100 orders → 101 queries tổng cộng
}

// ✅ Fix 1: Eager Loading
var orders = await _db.Orders
    .Include(o => o.Customer)                          // 1 query với JOIN
    .ToListAsync();

// ✅ Fix 2: Select projection — chỉ lấy columns cần, không load cả entity
var orders = await _db.Orders
    .Select(o => new OrderDto
    {
        Id           = o.Id,
        CustomerName = o.Customer.Name,    // EF Core tự JOIN
        Total        = o.Total
    })
    .ToListAsync();

// ✅ Fix 3: Split Query — tránh cartesian explosion khi Include nhiều collections
var orders = await _db.Orders
    .Include(o => o.Customer)
    .Include(o => o.Items)                // Items là collection → cartesian explosion
    .AsSplitQuery()                       // chạy 3 query riêng, không JOIN → tránh duplicate rows
    .ToListAsync();
```

**Detect N+1:** Dùng MiniProfiler hoặc bật EF Core logging:
```csharp
// Program.cs
builder.Services.AddDbContext<AppDbContext>(opt =>
    opt.UseSqlServer(connectionString)
       .LogTo(Console.WriteLine, LogLevel.Information)  // log tất cả SQL
       .EnableSensitiveDataLogging());
```

---

## PHẦN 7 — CONCURRENCY (hay hỏi với retail)

---

### Q: 2 user cùng đặt hàng sản phẩm cuối cùng — xử lý thế nào?

**Optimistic Locking với EF Core:**
```csharp
// Model
public class Product
{
    public int Id { get; set; }
    public int Stock { get; set; }
    [Timestamp] public byte[] RowVersion { get; set; }  // EF Core tự thêm WHERE RowVersion = ?
}

// Handler
public async Task<bool> ReserveStockAsync(int productId, int quantity)
{
    var product = await _db.Products.FindAsync(productId);
    if (product.Stock < quantity) return false;

    product.Stock -= quantity;

    try
    {
        await _db.SaveChangesAsync();
        // EF Core tự generate:
        // UPDATE products SET stock=? WHERE id=? AND row_version=?
        // Nếu row_version thay đổi → 0 rows → throw exception
        return true;
    }
    catch (DbUpdateConcurrencyException)
    {
        // User khác đã mua → retry hoặc báo hết hàng
        return false;
    }
}
```

**Pessimistic Locking — khi conflict xảy ra thường xuyên:**
```csharp
// SQL Server: SELECT ... WITH (UPDLOCK, ROWLOCK)
var product = await _db.Products
    .FromSqlRaw("SELECT * FROM products WITH (UPDLOCK, ROWLOCK) WHERE id = {0}", productId)
    .FirstOrDefaultAsync();

product.Stock -= quantity;
await _db.SaveChangesAsync();
// Lock release khi transaction commit
```

---

## PHẦN 8 — MICROSERVICES CHO RETAIL

---

### Q: Circuit Breaker là gì, dùng Polly thế nào?

```
Vấn đề: inventory-service chậm/crash → order-service chờ timeout → thread pool cạn → cascade failure

Circuit Breaker có 3 trạng thái:
  Closed   → hoạt động bình thường, đếm failure
  Open     → sau N failure liên tiếp, chặn hết request trong T giây (fail fast)
  Half-Open→ sau T giây, cho thử 1 request → thành công thì Closed, fail thì Open tiếp
```

```csharp
// Polly — .NET resilience library
var policy = Policy
    .Handle<HttpRequestException>()
    .Or<TimeoutException>()
    .CircuitBreakerAsync(
        exceptionsAllowedBeforeBreaking: 5,    // 5 lỗi liên tiếp → Open
        durationOfBreak: TimeSpan.FromSeconds(30),  // Open 30 giây
        onBreak:    (ex, duration) => logger.LogWarning("Circuit OPEN"),
        onReset:    () => logger.LogInformation("Circuit CLOSED"),
        onHalfOpen: () => logger.LogInformation("Circuit HALF-OPEN")
    );

// Kết hợp Retry + CircuitBreaker + Timeout
var policy = Policy.WrapAsync(circuitBreaker, retryPolicy, timeoutPolicy);

var result = await policy.ExecuteAsync(() => _inventoryService.CheckStockAsync(productId));
```

---

### Q: Saga Pattern — xử lý distributed transaction

```
Vấn đề: PlaceOrder cần: Reserve Stock + Process Payment + Create Shipment
         3 service riêng → không có distributed transaction
         1 service fail → cần rollback các service đã thành công

Choreography Saga (event-driven):
  OrderService  → publish "OrderCreated"
  StockService  → nhận → reserve → publish "StockReserved"
  PaymentService→ nhận → charge  → publish "PaymentCompleted"
  ShipService   → nhận → create shipment
  Nếu Payment fail → publish "PaymentFailed"
  StockService  → nhận "PaymentFailed" → release stock (compensating transaction)

Orchestration Saga (có coordinator):
  OrderSaga (orchestrator) gọi lần lượt:
    1. StockService.Reserve()
    2. PaymentService.Charge()    ← nếu fail → gọi StockService.Release()
    3. ShipService.Create()
  Orchestrator biết toàn bộ flow → dễ debug hơn
```

---

## PHẦN 9 — CÂU TỰ GIỚI THIỆU

### Template trả lời "kể về dự án khó nhất"

```
Cấu trúc: Situation → Task → Action → Result (STAR)

"Tôi từng làm một hệ thống [tên hệ thống] xử lý [vấn đề].
 Thách thức lớn nhất là [vấn đề cụ thể — concurrency/performance/consistency].
 Tôi tiếp cận bằng cách [giải pháp kỹ thuật cụ thể].
 Kết quả là [số liệu cụ thể — giảm latency X%, tăng throughput Y%]."

Ví dụ:
"Hệ thống flash sale xử lý 10k đơn/phút. Vấn đề là race condition khi nhiều user
 cùng mua sản phẩm cuối cùng. Tôi dùng Redis SET NX để làm distributed lock
 + Kafka để queue request, tránh hit DB trực tiếp.
 Kết quả giảm từ 5% oversell xuống 0%, response time từ 2s xuống 300ms."
```

---

## PHẦN 10 — QUICK REFERENCE

```
Câu hỏi                               Keyword trả lời
──────────────────────────────────────────────────────────────────
Singleton inject Scoped?              Captive dependency → IServiceScopeFactory
IQueryable vs IEnumerable?            IQueryable = SQL, IEnumerable = in-memory
N+1 problem?                          Include() / Select() / AsSplitQuery()
2 user mua hàng cuối?                 Optimistic (RowVersion) / Pessimistic (UPDLOCK)
Service khác crash?                   Circuit Breaker (Polly)
Distributed transaction?              Saga Pattern (Choreography / Orchestration)
Không mất event khi crash?            Outbox Pattern
async deadlock?                       Không dùng .Result / .Wait() trong ASP.NET
Tách read/write?                      CQRS + MediatR
Pricing nhiều loại?                   Strategy Pattern
Layer nào chứa business logic?        Domain Layer (Entity)
Layer nào depend vào DB cụ thể?       Infrastructure Layer
```
