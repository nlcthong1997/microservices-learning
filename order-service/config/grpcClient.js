// order-service/config/grpcClient.js
//
// GRPC CLIENT - kết nối tới inventory-service gRPC server (port 50051)
//
// =====================================================================
// SO SÁNH: REST vs gRPC client
// =====================================================================
//
//  REST (axios):
//    const res = await axios.get('http://inventory-service:3002/inventory/IPHONE-15')
//    → Gửi HTTP/1.1 text request
//    → Tự parse JSON string → object
//    → Không biết trước response có field gì (phải đọc docs hoặc thử)
//
//  gRPC (client này):
//    const res = await checkStock({ product_id: 'IPHONE-15', quantity: 1 })
//    → Gửi HTTP/2 binary request
//    → Proto-loader tự serialize/deserialize
//    → IDE biết chính xác response có field: available, stock, message
//
// =====================================================================
// SINGLETON PATTERN — tại sao?
// =====================================================================
//
//  Kết nối gRPC (giống DB connection) tốn kém để tạo:
//    - Phải handshake HTTP/2
//    - Phải xác thực TLS (nếu có)
//    - Phải resolve DNS
//
//  Nếu tạo connection mới mỗi request → bottleneck, memory leak
//
//  → Export 1 instance dùng chung cho toàn bộ app (giống cách
//    httpClient.js dùng axios.create() một lần)

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// Load cùng file .proto mà inventory-service dùng
// → Đây là "hợp đồng" — cả hai bên đều phải dùng cùng 1 file
const PROTO_PATH = path.join(__dirname, '../../proto/inventory.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const inventoryProto = grpc.loadPackageDefinition(packageDefinition).inventory;

// =====================================================================
// Tạo gRPC Channel và Stub
// =====================================================================
//
//  "Stub" = đại diện của remote service trên local
//  Gọi stub.checkStock() giống như gọi function local,
//  nhưng thực ra đang gọi qua mạng tới inventory-service
//
//  INVENTORY_GRPC_URL: dùng Docker service name "inventory-service"
//  khi chạy trong Docker Compose (các container cùng network)
//  Khi chạy local: dùng localhost:50051

const INVENTORY_GRPC_URL = process.env.INVENTORY_GRPC_URL || 'localhost:50051';

// `createInsecure()` = không TLS (dùng trong dev/internal service)
// Production: dùng `grpc.credentials.createSsl()`
const inventoryClient = new inventoryProto.InventoryService(
  INVENTORY_GRPC_URL,
  grpc.credentials.createInsecure()
);

// =====================================================================
// Wrap callback-based gRPC vào Promise
// =====================================================================
//
//  gRPC client mặc định dùng callback style:
//    inventoryClient.checkStock(request, (err, response) => { ... })
//
//  Nhưng codebase đang dùng async/await → cần wrap thành Promise
//  để dùng thống nhất với httpClient.js

function checkStock({ product_id, quantity }) {
  return new Promise((resolve, reject) => {
    inventoryClient.checkStock({ product_id, quantity }, (err, response) => {
      if (err) {
        // gRPC error có thêm `err.code` (gRPC status code)
        // VD: grpc.status.NOT_FOUND (5), grpc.status.UNAVAILABLE (14)
        // Gắn thêm để caller có thể phân biệt loại lỗi
        err.grpcCode = err.code;
        return reject(err);
      }
      resolve(response);
    });
  });
}

module.exports = { checkStock, INVENTORY_GRPC_URL };
