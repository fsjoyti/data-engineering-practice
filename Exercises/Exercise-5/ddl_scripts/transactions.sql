DROP TABLE IF EXISTS transactions CASCADE;

CREATE TABLE transactions(
    transaction_id VARCHAR(255) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    product_id INTEGER references products(product_id),
    product_code INTEGER NOT NULL,
    product_description VARCHAR(255),
    quantity INTEGER NOT NULL,
    account_id INTEGER references accounts(customer_id)

);