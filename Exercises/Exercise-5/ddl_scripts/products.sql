DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE products(
    product_id INTEGER PRIMARY KEY,
    product_code INTEGER NOT NULL unique,
    product_description VARCHAR(255) unique
);