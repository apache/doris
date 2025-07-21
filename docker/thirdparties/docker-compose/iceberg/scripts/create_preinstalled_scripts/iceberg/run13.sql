use demo.test_db;

CREATE TABLE test_iceberg_systable_unpartitioned (
  id INT,
  name STRING
)
USING ICEBERG
TBLPROPERTIES (
  'primary-key' = 'id',
  'write.upsert.enabled' = 'true'
);

CREATE TABLE test_iceberg_systable_partitioned (
  id INT,
  name STRING,
  age INT
)
USING ICEBERG
PARTITIONED BY (age)
TBLPROPERTIES (
  'primary-key' = 'id',
  'write.upsert.enabled' = 'true'
);

INSERT INTO test_iceberg_systable_unpartitioned VALUES
(1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave'), (5, 'Eve'),
(6, 'Frank'), (7, 'Grace'), (8, 'Heidi'), (9, 'Ivan'), (10, 'Judy');

INSERT INTO test_iceberg_systable_unpartitioned VALUES (2, 'Bob Updated');
DELETE FROM test_iceberg_systable_unpartitioned WHERE id = 3;

INSERT INTO test_iceberg_systable_partitioned VALUES
(1, 'Alice', 30), (2, 'Bob', 30), (3, 'Carol', 25), (4, 'Dave', 25),
(5, 'Eve', 35), (6, 'Frank', 35), (7, 'Grace', 40), (8, 'Heidi', 40),
(9, 'Ivan', 45), (10, 'Judy', 45);

INSERT INTO test_iceberg_systable_partitioned VALUES (2, 'Bob Updated', 31);
DELETE FROM test_iceberg_systable_partitioned WHERE id = 3;