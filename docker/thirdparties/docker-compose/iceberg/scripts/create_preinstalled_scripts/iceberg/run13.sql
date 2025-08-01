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
  name STRING
)
USING ICEBERG
PARTITIONED BY (id)
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
(1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave'),
(5, 'Eve'), (6, 'Frank'), (7, 'Grace'), (8, 'Heidi'),
(9, 'Ivan'), (10, 'Judy');

INSERT INTO test_iceberg_systable_partitioned VALUES (2, 'Bob Updated');
DELETE FROM test_iceberg_systable_partitioned WHERE id = 3;