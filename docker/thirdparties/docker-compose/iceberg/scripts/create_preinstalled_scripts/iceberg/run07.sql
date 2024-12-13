
use demo.test_db;
CREATE TABLE iceberg_add_partition (
    id INT,
    name STRING,
    age INT
) USING iceberg;
INSERT INTO iceberg_add_partition VALUES(1, 'Alice', 30),(2, 'Bob', 25);
ALTER TABLE iceberg_add_partition ADD PARTITION FIELD age;
ALTER TABLE iceberg_add_partition ADD COLUMNS address STRING;
INSERT INTO iceberg_add_partition VALUES (4, 'Charlie', 45, '123 Street Name');
ALTER TABLE iceberg_add_partition ADD PARTITION FIELD bucket(10, id);
INSERT INTO iceberg_add_partition VALUES (5, 'Eve', 29, '789 Third St');
ALTER TABLE iceberg_add_partition ADD PARTITION FIELD truncate(5, address);
INSERT INTO iceberg_add_partition VALUES (6, 'Frank', 33,"xx"),(7, 'Grace', 28,"yyyyyyyyy");



CREATE TABLE iceberg_drop_partition (
    id INT,
    name STRING,
    amount DOUBLE,
    created_date DATE
) 
USING iceberg
PARTITIONED BY (year(created_date),bucket(10,created_date));
INSERT INTO iceberg_drop_partition VALUES
    (1, 'Alice', 100.0, DATE '2023-12-01'),
    (2, 'Bob', 200.0, DATE '2023-12-02'),
    (3, 'Charlie', 300.0, DATE '2024-12-03');
ALTER TABLE iceberg_drop_partition  DROP PARTITION FIELD year(created_date);
INSERT INTO iceberg_drop_partition VALUES
    (4, 'David', 400.0, DATE '2023-12-02'),
    (5, 'Eve', 500.0, DATE '2024-12-03');
ALTER TABLE iceberg_drop_partition  DROP PARTITION FIELD bucket(10,created_date);
INSERT INTO iceberg_drop_partition VALUES
    (6, 'David', 400.0, DATE '2025-12-12'),
    (7, 'Eve', 500.0, DATE '2025-12-23');


CREATE TABLE iceberg_replace_partition (
    id INT,
    name STRING,
    amount DOUBLE,
    created_date DATE
)
USING iceberg
PARTITIONED BY (year(created_date),bucket(10,created_date));
INSERT INTO iceberg_replace_partition VALUES
    (1, 'Alice', 100.0, DATE '2023-01-01'),
    (2, 'Bob', 200.0, DATE '2023-12-02'),
    (3, 'Charlie', 300.0, DATE '2024-12-03');
ALTER TABLE iceberg_replace_partition  REPLACE PARTITION FIELD year(created_date) WITH month(created_date);
INSERT INTO iceberg_replace_partition VALUES
    (4, 'David', 400.0, DATE '2023-12-02'),
    (5, 'Eve', 500.0, DATE '2024-07-03');
ALTER TABLE iceberg_replace_partition  REPLACE PARTITION FIELD bucket(10,created_date) WITH bucket(10,id);
INSERT INTO iceberg_replace_partition VALUES
    (6, 'David', 400.0, DATE '2025-10-12'),
    (7, 'Eve', 500.0, DATE '2025-09-23');




CREATE TABLE iceberg_evolution_partition (
    id INT,
    name STRING,
    age INT
) USING iceberg;
INSERT INTO iceberg_evolution_partition VALUES(1, 'Alice', 30),(2, 'Bob', 25);
ALTER TABLE iceberg_evolution_partition ADD PARTITION FIELD age;
ALTER TABLE iceberg_evolution_partition ADD COLUMNS address STRING;
INSERT INTO iceberg_evolution_partition VALUES (4, 'Charlie', 45, '123 Street Name');
ALTER TABLE iceberg_evolution_partition ADD PARTITION FIELD bucket(10, id);
INSERT INTO iceberg_evolution_partition VALUES (5, 'Eve', 29, '789 Third St');
ALTER TABLE iceberg_evolution_partition REPLACE PARTITION FIELD bucket(10, id) WITH truncate(5, address);
INSERT INTO iceberg_evolution_partition VALUES (6, 'Frank', 33,"xx"),(7, 'Grace', 28,"yyyyyyyyy");
ALTER TABLE iceberg_evolution_partition DROP PARTITION FIELD truncate(5, address);
INSERT INTO iceberg_evolution_partition VALUES (8, 'Hank', 40, "zz"), (9, 'Ivy', 22, "aaaaaa");
ALTER TABLE iceberg_evolution_partition DROP COLUMNS address;
-- INSERT INTO iceberg_evolution_partition VALUES (10, 'Jack', 35), (11, 'Kara', 30);
-- spark error.

