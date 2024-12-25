DROP TABLE IF EXISTS products;

CREATE TABLE products(
    product_id INTEGER unique,
    product_code INTEGER NOT NULL unique,
    product_description TEXT unique
    PRIMARY KEY(product_id, product_code, product_description)
);