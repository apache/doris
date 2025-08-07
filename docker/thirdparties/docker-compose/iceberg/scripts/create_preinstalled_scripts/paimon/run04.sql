use paimon;

create database if not exists test_paimon_incr_read_db;

use test_paimon_incr_read_db;

CREATE TABLE paimon_incr (
    id INT,
    name STRING,
    age INT
) USING paimon;

INSERT INTO paimon_incr (id, name, age) VALUES (1, 'Alice', 30);
INSERT INTO paimon_incr (id, name, age) VALUES (2, 'Bob', 25);
INSERT INTO paimon_incr (id, name, age) VALUES (3, 'Charlie', 28);

