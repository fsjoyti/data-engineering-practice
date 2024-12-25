DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions(
    transaction_id VARCHAR(255) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    product_id INTEGER NOT NULL ,
    product_code INTEGER NOT NULL,
    product_description TEXT,
    quantity INTEGER NOT NULL,
    account_id NOT NULL,
    FOREIGN KEY account_id references accounts(customer_id),
    FOREIGN KEY (product_id, product_code, product_code) references products(product_id, product_code, product_description)

);