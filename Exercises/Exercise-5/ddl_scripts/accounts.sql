DROP TABLE IF EXISTS accounts CASCADE;
CREATE TABLE accounts(
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR (255) NOT NULL, 
    last_name VARCHAR (255) NOT NULL,
    address_1 VARCHAR (255) NOT NULL,
    address_2 VARCHAR (255),
    city VARCHAR (255) NOT NULL,
    state VARCHAR (255) NOT NULL,
    zip_code INTEGER NOT NULL,
    join_date VARCHAR (255) NOT NULL
);
