// inventory-service/src/models/inventory.js

// --- DATA KHO GIẢ LẬP ---
// Trong thực tế, đây sẽ là dữ liệu trong Database (ví dụ Postgres/MongoDB)
// Ta tách ra đây để dùng chung (Shared State)
const mockInventory = {
    'IPHONE-15': { stock: 10, reserved: 0 },
    'MACBOOK-M3': { stock: 5, reserved: 0 },
    'LAPTOP-MODULAR-TEST': { stock: 100, reserved: 0 }
};

module.exports = mockInventory;