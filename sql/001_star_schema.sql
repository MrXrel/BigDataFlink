CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    sale_year INTEGER NOT NULL,
    sale_month INTEGER NOT NULL,
    sale_day INTEGER NOT NULL,
    sale_quarter INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_stores (
    store_name TEXT PRIMARY KEY,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_name TEXT PRIMARY KEY,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    price NUMERIC(10, 2),
    stock_quantity INTEGER,
    pet_category TEXT,
    weight NUMERIC(8, 2),
    color TEXT,
    size TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(4, 2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    supplier_name TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    source_file TEXT,
    source_record_no INTEGER,
    source_row_id BIGINT,
    date_id INTEGER,
    customer_id INTEGER,
    seller_id INTEGER,
    product_id INTEGER,
    store_name TEXT,
    supplier_name TEXT,
    quantity INTEGER,
    total_price NUMERIC(12, 2),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales (product_id);
