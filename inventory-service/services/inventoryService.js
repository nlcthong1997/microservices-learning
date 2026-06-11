// inventory-service/services/inventoryService.js
//
// BUSINESS LOGIC LAYER — nguồn sự thật duy nhất cho nghiệp vụ kho
//
// Tại sao tách ra file này?
//
//   Trước khi có file này, logic check kho bị viết ở 2 chỗ:
//     - inventoryRoutes.js  (REST)   : product.stock > 0
//     - grpcServer.js       (gRPC)   : item.stock - item.reserved >= quantity
//
//   Hai cách tính KHÁC NHAU → REST và gRPC trả kết quả khác nhau cho cùng
//   một câu hỏi. Đây là bug kinh điển khi không có service layer.
//
//   Với file này: cả REST lẫn gRPC đều gọi checkStock() từ đây.
//   Muốn thay đổi logic → sửa một chỗ duy nhất.
//
// Đây là pattern "Service Layer" (hoặc "Domain Logic Layer"):
//   Route/Controller  → chỉ xử lý HTTP/gRPC request/response
//   Service Layer     → chứa business logic
//   Model/Repository  → chứa data

const mockInventory = require('../models/inventory');

/**
 * Check xem sản phẩm có đủ hàng không.
 *
 * @param {string} productId - Mã sản phẩm
 * @param {number} quantity  - Số lượng cần
 * @returns {{ found: boolean, available: boolean, stock: number, message: string }}
 */
function checkStock(productId, quantity = 1) {
  const item = mockInventory[productId];

  if (!item) {
    return {
      found: false,
      available: false,
      stock: 0,
      message: `Product not found: ${productId}`,
    };
  }

  // Hàng thực tế có thể dùng = tổng kho - đã đặt chờ xử lý
  const availableStock = item.stock - item.reserved;
  const available = availableStock >= quantity;

  return {
    found: true,
    available,
    stock: availableStock,
    message: available
      ? `In stock (${availableStock} units available)`
      : `Out of stock (only ${availableStock} left, requested ${quantity})`,
  };
}

module.exports = { checkStock };
