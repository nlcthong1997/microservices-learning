-- inventory-service/db/init.sql
--
-- Script này chạy TỰ ĐỘNG khi PostgreSQL container khởi động lần đầu
-- (mount vào /docker-entrypoint-initdb.d/)
--
-- Chạy lại: docker compose down -v && docker compose up -d postgres
--   -v = xóa volume → DB reset về trạng thái ban đầu

-- ── Bảng inventory ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory (
    product_id   VARCHAR(100) PRIMARY KEY,
    stock        INTEGER      NOT NULL DEFAULT 0,
    reserved     INTEGER      NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Đảm bảo không bao giờ âm kho
    CONSTRAINT stock_non_negative    CHECK (stock    >= 0),
    CONSTRAINT reserved_non_negative CHECK (reserved >= 0),
    CONSTRAINT reserved_le_stock     CHECK (reserved <= stock)
);

-- ── Data mẫu ─────────────────────────────────────────────────────────────────
-- FAIL-PAYMENT: có hàng để inventory reserve thành công,
-- nhưng payment-service sẽ reject → trigger SAGA rollback (compensating transaction)
INSERT INTO inventory (product_id, stock, reserved) VALUES
    ('IPHONE-15',           10,  0),
    ('MACBOOK-M3',           5,  0),
    ('LAPTOP-MODULAR-TEST', 100, 0),
    ('FAIL-PAYMENT',        99,  0)
ON CONFLICT (product_id) DO NOTHING;

-- ── Index (tăng tốc lookup theo product_id) ───────────────────────────────────
-- PRIMARY KEY đã tạo index tự động → không cần thêm ở đây

-- ── Function tự cập nhật updated_at ─────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER inventory_updated_at
    BEFORE UPDATE ON inventory
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
