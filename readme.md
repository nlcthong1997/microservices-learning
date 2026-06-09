# 📝 Ghi Chú Hệ Thống Giám Sát & Quản Trị (Dashboards Note)

Tài liệu này ghi lại thông tin chi tiết về 3 giao diện quản trị (Dashboards) được tích hợp trong dự án `microservice-learning` qua `docker-compose.yml`.

Việc nắm vững các công cụ này là cốt lõi để hiểu cách Microservices giao tiếp, dữ liệu chảy qua hạ tầng như thế nào, và cách truy vết lỗi trong môi trường phân tán.

---

## 🟢 1. Giao diện quản trị RabbitMQ (RabbitMQ Management Plugin)

RabbitMQ đóng vai trò là **Message Broker** cho giao tiếp **bất đồng bộ (Asynchronous)** giữa `Order Service` và `Inventory Service` theo mô hình Point-to-Point (Queue).

### Info kết nối
* **Địa chỉ:** [http://localhost:15672](http://localhost:15672)
* **Tài khoản:** `guest`
* **Mật khẩu:** `guest`

### Chi tiết kỹ thuật

| Mục | Nội dung |
| :--- | :--- |
| **Công dụng** | Giám sát sức khỏe của Broker, quản lý Connections, Channels, Exchanges, Queues và theo dõi tốc độ đẩy/nhận tin nhắn (Message rates) theo thời gian thực. |
| **Nguyên lý & Lý thuyết** | Dựa trên giao thức **AMQP 0-9-1**. Quy trình: **Producer** (Order) bắn tin nhắn vào **Exchange** $\rightarrow$ Exchange dựa trên Routing Key để đẩy tin nhắn vào **Queue** phù hợp $\rightarrow$ **Consumer** (Inventory) lắng nghe Queue và lấy tin nhắn về xử lý. Tin nhắn thường bị xóa khỏi Queue sau khi Consumer xác nhận (ACK) đã xử lý xong. |
| **Cách hoạt động** | `Order Service` dùng thư viện Node.js kết nối tới cổng TCP `5672` để publish message JSON. `Inventory Service` cũng kết nối tới cổng này để subscribe message từ Queue. Giao diện quản trị chạy trên một cổng HTTP riêng biệt (`15672`). |
| **Ứng dụng trong project** | Khi User đặt hàng, `Order Service` push một event `order.created` vào RabbitMQ. `Inventory Service` lắng nghe event này ngầm để thực hiện **trừ kho** mà không làm Client phải chờ đợi lâu. |

### 📸 Hướng dẫn chụp ảnh làm Báo cáo
1.  **Chụp Exchange:** Vào tab **Exchanges**, tìm Exchange tên là `order_events` (hoặc tên bạn đặt trong code). Chụp màn hình để chứng minh Exchange này đã được tạo và đang hoạt động.
2.  **Chụp Queue & Message Rate:** Vào tab **Queues**, chọn queue `inventory_order_created`. Thực hiện bắn request đặt hàng liên tục (dùng Postman Runner hoặc `ab` tool). Chụp màn hình **biểu đồ hình sin (Queued messages)** đang nhảy lên nhảy xuống. Điều này chứng minh dữ liệu đang được đẩy vào và lấy ra khỏi hàng đợi thành công.

---

## 🔵 2. Giao diện quản trị Kafka UI (Provectus Kafka UI)

Kafka đóng vai trò là **Event Streaming Platform** cho luồng dữ liệu lớn (Big Data) từ `Order Service` đến `Analytics Service` theo mô hình Publish-Subscribe.

### Info kết nối
* **Địa chỉ:** [http://localhost:8080](http://localhost:8080)

### Chi tiết kỹ thuật

| Mục | Nội dung |
| :--- | :--- |
| **Công dụng** | Giao diện Web trực quan để quản lý Cluster Kafka, xem danh sách Topics, Partitions, Messages bên trong Topic, và giám sát các Consumer Groups. |
| **Nguyên lý & Lý thuyết** | Dựa trên kiến trúc **Distributed Commit Log**. Dữ liệu được push vào Kafka **Topic** và lưu trữ **cố định trên đĩa cứng** dưới dạng các bản ghi append-only (ghi nối đuôi). Khác với RabbitMQ, dữ liệu trong Kafka **không bị xóa** sau khi đọc (chỉ xóa khi hết hạn - Retention policy). **Consumer** đọc dữ liệu dựa trên **Offset** (vị trí đánh dấu đã đọc) mà nó tự quản lý. |
| **Cách hoạt động** | `Order Service` dùng thư viện Node.js kết nối tới cổng Kafka `9092` để produce message stream. `Analytics Service` kết nối tới cổng này để consume stream từ Topic. Kafka-UI là một service Docker riêng biệt, kết nối vào mạng nội bộ của Kafka (`kafka:29092`) để lấy thông tin hiển thị lên web. |
| **Ứng dụng trong project** | Dùng cho luồng **User Tracking/Analytics**. Mọi hành vi mua hàng được `Order Service` produce (đẩy) vào Kafka topic dưới dạng stream. `Analytics Service` consume (đọc) stream này để tính toán báo cáo doanh thu mà không ảnh hưởng đến tốc độ của luồng đặt hàng chính. |

### 📸 Thao tác
1.  **Chụp Topic & Storage:** Vào mục **Topics** $\rightarrow$ Chọn topic `user-behavior-logs`. Chụp màn hình phần tổng quan topic, chỉ rõ số lượng tin nhắn (Messages) và dung lượng trên đĩa. Điều này chứng minh Kafka đang lưu trữ dữ liệu bền vững.
2.  **Chụp Consumer Group & Offset:** Vào tab **Consumers**, tìm group `analytics-group`. Chụp màn hình hiển thị các Partitions, vị trí **Current Offset** (vị trí đã đọc) và **Log End Offset** (vị trí cuối cùng của dữ liệu). Sự chênh lệch (Lag) thấp chứng minh Consumer đang đọc stream thời gian thực tốt.

---

## 🟡 3. Giao diện xem Log tổng hợp Grafana (với Loki data source)

Đây là trung tâm của hệ thống giám sát, thực hiện vai trò **Centralized Logging (Quản lý log tập trung)** và hỗ trợ **Distributed Tracing (Truy vết phân tán)** cơ bản thông qua Trace ID.

### Info kết nối
* **Địa chỉ:** [http://localhost:3000](http://localhost:3000)
* **Tài khoản:** `admin`
* **Mật khẩu:** `admin`

### Chi tiết kỹ thuật

| Mục | Nội dung |
| :--- | :--- |
| **Công dụng** | Hiển thị, truy vấn và trực quan hóa logs được gom về từ tất cả các Microservices. Dùng để debug lỗi xuyên suốt nhiều service. |
| **Nguyên lý & Lý thuyết** | Sử dụng stack **Grafana - Loki**. Loki là hệ thống gom log được thiết kế để scale, nó chỉ đánh index trên labels (nhãn) của logs (ví dụ: service_name, host) chứ không đánh index toàn bộ nội dung logs. Grafana kết nối với Loki như một Data Source để thực hiện truy vấn bằng ngôn ngữ **LogQL**. |
| **Cách hoạt động** | `winston-loki` transport trong code Node.js gom log JSON và đẩy qua giao thức HTTP đến cổng 3100 của Loki. Loki đánh index dựa trên labels và lưu trữ dữ liệu. |
| **Ứng dụng trong project** | Cả 3 services (`Order`, `Inventory`, `Analytics`) đều cấu hình `logger.js` để đẩy log JSON về Loki. Khi `Order Service` tạo đơn, nó sinh ra một chuỗi UUID độc nhất (gọi là `trace_id` hoặc `Correlation ID`). `trace_id` này được truyền qua HTTP Header (sang Inventory Sync), truyền qua Message Property (sang RabbitMQ, Kafka). Nhờ đó, log của cả 3 service cho cùng 1 đơn hàng đều chứa chung một `trace_id`. |

### 📸 Thao tác
1.  **Thao tác:** Đặt một đơn hàng, lấy chuỗi `trace_id` (UUID) từ log console hoặc response trả về.
2.  **Truy vấn trên Grafana:** Vào mục **Explore**, chọn Data Source là **Loki**. Tại ô truy vấn LogQL, gõ: `{app="order-service"} |= "chuỗi-trace-id-của-bạn"` (hoặc chỉ cần gõ chuỗi trace_id vào ô search nếu đã cấu hình parse JSON).
3.  **Chụp ảnh:** Màn hình Grafana hiển thị các dòng log đầy đủ màu sắc, xếp theo thứ tự thời gian. Bạn phải chỉ rõ trong ảnh các dòng log đến từ các `app` khác nhau (Order, Inventory, Analytics) nhưng **tất cả đều chứa chung chuỗi trace_id**.
4.  **Lời bình báo cáo:** "Đây là bằng chứng kỹ thuật chứng minh khả năng **Truy vết phân tán (Distributed Tracing)**. Dù lỗi xảy ra ngầm ở Inventory Service khi xử lý RabbitMQ, ta vẫn dễ dàng tìm ra dựa trên trace_id được sinh ra từ Order Service ban đầu, giải quyết triệt để bài toán debug trong kiến trúc Microservices."