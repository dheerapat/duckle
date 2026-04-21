-- ============================================
-- Seed data for the restaurant source database
-- 180 days of orders (2024-01-01 → 2024-06-30)
-- ============================================

-- ---------- menu_items ----------
INSERT INTO menu_items (name, category, price) VALUES
  -- Appetizers
  ('Bruschetta',          'Appetizers', 8.50),
  ('Caesar Salad',        'Appetizers', 9.00),
  ('Soup of the Day',     'Appetizers', 7.50),
  ('Garlic Bread',        'Appetizers', 5.00),
  ('Spring Rolls',        'Appetizers', 8.00),
  -- Mains
  ('Grilled Salmon',      'Mains',      24.00),
  ('Ribeye Steak',        'Mains',      28.00),
  ('Chicken Parmesan',   'Mains',      18.50),
  ('Margherita Pizza',    'Mains',      16.00),
  ('Mushroom Risotto',    'Mains',      19.00),
  ('Fish and Chips',      'Mains',      15.50),
  ('Pad Thai',            'Mains',      14.00),
  -- Sides
  ('French Fries',        'Sides',      4.50),
  ('Mashed Potatoes',     'Sides',      5.00),
  ('Steamed Vegetables',  'Sides',      6.00),
  -- Desserts
  ('Tiramisu',            'Desserts',   9.50),
  ('Chocolate Lava Cake', 'Desserts',  10.00),
  ('Cheesecake',          'Desserts',   8.50),
  -- Drinks
  ('Craft Beer',          'Drinks',     7.00),
  ('House Wine',           'Drinks',     9.00);

-- ---------- customers ----------
INSERT INTO customers (name, email, phone, joined_at) VALUES
  ('Alice Johnson',    'alice@example.com',    '555-0101', '2023-06-15'),
  ('Bob Martinez',     'bob@example.com',      '555-0102', '2023-07-20'),
  ('Carol Chen',       'carol@example.com',    '555-0103', '2023-08-03'),
  ('David Kim',        'david@example.com',    '555-0104', '2023-09-12'),
  ('Eva Rossi',        'eva@example.com',      '555-0105', '2023-10-01'),
  ('Frank Müller',     'frank@example.com',    '555-0106', '2023-10-25'),
  ('Grace Okafor',     'grace@example.com',    '555-0107', '2023-11-05'),
  ('Henry Patel',      'henry@example.com',    '555-0108', '2023-11-18'),
  ('Iris Nakamura',    'iris@example.com',     '555-0109', '2023-12-01'),
  ('James Smith',      'james@example.com',    '555-0110', '2023-12-15'),
  ('Karen Lopez',      'karen@example.com',    '555-0111', '2024-01-02'),
  ('Leo Dubois',       'leo@example.com',      '555-0112', '2024-01-10'),
  ('Maya Andersen',    'maya@example.com',     '555-0113', '2024-01-18'),
  ('Nick Petrov',      'nick@example.com',      '555-0114', '2024-02-01'),
  ('Olivia Tanaka',    'olivia@example.com',   '555-0115', '2024-02-14'),
  ('Paul Schmidt',     'paul@example.com',     '555-0116', '2024-03-01'),
  ('Quinn Rivera',     'quinn@example.com',    '555-0117', '2024-03-10'),
  ('Rosa Fernandez',   'rosa@example.com',     '555-0118', '2024-04-01'),
  ('Sam Wilson',       'sam@example.com',      '555-0119', '2024-04-15'),
  ('Tina Gupta',       'tina@example.com',     '555-0120', '2024-05-01'),
  ('Uma Johansson',   'uma@example.com',       '555-0121', '2024-05-10'),
  ('Victor Larsen',    'victor@example.com',   '555-0122', '2024-05-20'),
  ('Wendy Zhao',       'wendy@example.com',    '555-0123', '2024-06-01'),
  ('Xavier Costa',     'xavier@example.com',   '555-0124', '2023-07-01'),
  ('Yuki Tan',         'yuki@example.com',     '555-0125', '2023-08-15'),
  ('Zara Ali',         'zara@example.com',     '555-0126', '2023-09-20'),
  ('Aaron Blake',      'aaron@example.com',    '555-0127', '2023-10-10'),
  ('Bella Moretti',    'bella@example.com',    '555-0128', '2023-11-05'),
  ('Carlos Ruiz',      'carlos@example.com',   '555-0129', '2023-12-08'),
  ('Diana Park',       'diana@example.com',    '555-0130', '2024-01-15'),
  ('Eliot Nash',       'eliot@example.com',    '555-0131', '2024-02-20'),
  ('Fiona Walsh',      'fiona@example.com',    '555-0132', '2024-03-05'),
  ('George Huang',     'george@example.com',   '555-0133', '2024-04-10'),
  ('Hannah Meyer',     'hannah@example.com',   '555-0134', '2024-05-15'),
  ('Ivan Popov',       'ivan@example.com',     '555-0135', '2023-06-20'),
  ('Julia Santos',     'julia@example.com',    '555-0136', '2023-07-25'),
  ('Kevin O''Brien',    'kevin@example.com',    '555-0137', '2023-08-30'),
  ('Lena Ilic',        'lena@example.com',     '555-0138', '2023-09-15'),
  ('Marco Bianchi',    'marco@example.com',    '555-0139', '2023-10-20');

-- ---------- orders & order_items ----------
-- Generate 8–20 orders per day across 180 days (≈ 2500–3500 orders)
-- Each order has 1–4 line items.

DO $$
DECLARE
    ord_date   DATE;
    num_orders INTEGER;
    cust_id    INTEGER;
    ord_id     INTEGER;
    num_items  INTEGER;
BEGIN
    FOR ord_date IN SELECT generate_series('2024-01-01'::date, '2024-06-30'::date, '1 day'::interval)::date LOOP
        -- More orders on weekends (Sat=0, Sun=6)
        IF EXTRACT(DOW FROM ord_date) IN (0, 6) THEN
            num_orders := floor(random() * 10) + 13;  -- 13–22 on weekends
        ELSE
            num_orders := floor(random() * 8) + 8;    -- 8–15 on weekdays
        END IF;

        FOR i IN 1..num_orders LOOP
            cust_id := floor(random() * 39) + 1;      -- customers 1–39

            INSERT INTO orders (order_date, customer_id)
            VALUES (ord_date, cust_id)
            RETURNING id INTO ord_id;

            num_items := floor(random() * 4) + 1;      -- 1–4 items per order

            INSERT INTO order_items (order_id, menu_item_id, quantity)
            SELECT ord_id, (floor(random() * 20) + 1), (floor(random() * 3) + 1)
            FROM generate_series(1, num_items);
        END LOOP;
    END LOOP;
END $$;