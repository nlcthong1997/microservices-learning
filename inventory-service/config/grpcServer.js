// inventory-service/config/grpcServer.js
//
// GRPC SERVER - expose gRPC API trên port 50051
//
// Tại sao gRPC Server chạy riêng, không dùng Express (port 3002)?
//
//   Express dùng HTTP/1.1 (text-based)
//   gRPC dùng HTTP/2 (binary, multiplexing)
//
//   Hai protocol khác nhau hoàn toàn → cần hai server riêng:
//     - Port 3002: REST API (Express, dùng bởi browser, Postman, curl)
//     - Port 50051: gRPC API (dùng bởi service khác trong hệ thống)
//
//   Đây là kiến trúc phổ biến trong microservices: mỗi service có thể
//   phục vụ cả REST lẫn gRPC cùng lúc tùy từng use case.

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { checkStock: checkStockService } = require('../services/inventoryService');
const logger = require('./logger');

// =====================================================================
// BƯỚC 1: Load file .proto và tạo package definition
// =====================================================================
//
// protoLoader đọc file .proto (text) → tạo ra JavaScript object
// mô tả đúng cấu trúc service, message types...
//
// keepCase: true        → giữ nguyên tên field (product_id thay vì productId)
// longs: String         → số int64 convert thành String (JS không xử lý int64 tốt)
// enums: String         → enum convert thành String
// defaults: true        → field không set → trả về default value (vd: false, 0, "")
// oneofs: true          → hỗ trợ oneof fields

const PROTO_PATH = path.join(__dirname, '../../proto/inventory.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

// Tạo gRPC package từ definition
// `grpc.loadPackageDefinition` trả về object chứa tất cả service/message đã define trong .proto
const inventoryProto = grpc.loadPackageDefinition(packageDefinition).inventory;

// =====================================================================
// BƯỚC 2: Implement hàm CheckStock
// =====================================================================
//
// Đây là implementation thực tế của RPC "CheckStock" đã khai báo trong .proto
//
// `call.request` = object tương ứng CheckStockRequest message
//   → call.request.product_id (string)
//   → call.request.quantity (number)
//
// `callback(error, response)`:
//   - Nếu thành công: callback(null, responseObject)
//   - Nếu lỗi:        callback(new Error("message"))

function checkStock(call, callback) {
  const { product_id, quantity } = call.request;

  logger.info('gRPC: CheckStock request received', {
    product_id,
    quantity,
    protocol: 'gRPC',  // để phân biệt với REST logs
  });

  // Gọi service layer — cùng logic với REST, không viết lại
  const result = checkStockService(product_id, quantity);

  if (!result.found) {
    logger.warn('gRPC: Product not found', { product_id });
    return callback({
      code: grpc.status.NOT_FOUND,
      message: result.message,
    });
  }

  logger.info('gRPC: CheckStock result', {
    product_id,
    stock: result.stock,
    requested: quantity,
    available: result.available,
  });

  callback(null, {
    available: result.available,
    stock: result.stock,
    message: result.message,
  });
}

// =====================================================================
// BƯỚC 3: Tạo và export gRPC server
// =====================================================================

function startGrpcServer() {
  const server = new grpc.Server();

  // Gắn implementation vào service đã khai báo trong .proto
  // `inventoryProto.InventoryService.service` = service definition từ .proto
  // `{ checkStock }` = map tên RPC → implementation function
  server.addService(inventoryProto.InventoryService.service, {
    checkStock,
  });

  const GRPC_PORT = process.env.GRPC_PORT || '50051';
  const address = `0.0.0.0:${GRPC_PORT}`;

  // `grpc.ServerCredentials.createInsecure()` = không dùng TLS
  // Trong production phải dùng TLS: grpc.ServerCredentials.createSsl(...)
  server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      logger.error('gRPC: Failed to start server', { error: err.message });
      throw err;
    }
    logger.info(`gRPC Server listening`, { port, protocol: 'gRPC/HTTP2' });
  });

  return server;
}

module.exports = { startGrpcServer };
