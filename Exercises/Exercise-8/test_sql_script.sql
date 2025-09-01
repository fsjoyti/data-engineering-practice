drop table IF EXISTS test_table;
create table IF NOT EXISTS test_table (id INTEGER, name VARCHAR);
COPY test_table FROM 'test_data.csv' (AUTO_DETECT TRUE, HEADER TRUE);