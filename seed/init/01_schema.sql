-- ============================================
-- Schema for the restaurant source database
-- ============================================

CREATE TABLE menu_items (
    id        SERIAL PRIMARY KEY,
    name      TEXT    NOT NULL,
    category  TEXT    NOT NULL,
    price     DECIMAL(10,2) NOT NULL
);

CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    name        TEXT    NOT NULL,
    email       TEXT    NOT NULL UNIQUE,
    phone       TEXT,
    joined_at   DATE    NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE orders (
    id            SERIAL PRIMARY KEY,
    order_date    DATE    NOT NULL,
    customer_id   INTEGER NOT NULL REFERENCES customers(id)
);

CREATE TABLE order_items (
    id           SERIAL PRIMARY KEY,
    order_id     INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    menu_item_id INTEGER NOT NULL REFERENCES menu_items(id),
    quantity     INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0)
);

-- Indexes for incremental queries by date
CREATE INDEX idx_orders_date       ON orders  (order_date);
CREATE INDEX idx_order_items_order  ON order_items (order_id);