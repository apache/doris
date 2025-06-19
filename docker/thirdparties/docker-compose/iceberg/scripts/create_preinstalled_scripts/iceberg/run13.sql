use demo.test_db;

SET spark.sql.catalog.spark_catalog.write.delete.mode = merge-on-read;
SET spark.sql.catalog.spark_catalog.write.update.mode = merge-on-read;

CREATE TABLE test_iceberg_systable_unpartitioned (
  id INT,
  name STRING
)
USING ICEBERG;

CREATE TABLE test_iceberg_systable_partitioned (
  id INT,
  name STRING
)
USING ICEBERG
PARTITIONED BY (id);

INSERT INTO test_iceberg_systable_unpartitioned VALUES
(1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave'), (5, 'Eve'),
(6, 'Frank'), (7, 'Grace'), (8, 'Heidi'), (9, 'Ivan'), (10, 'Judy');

INSERT INTO test_iceberg_systable_partitioned VALUES
(1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave'), (5, 'Eve'),
(6, 'Frank'), (7, 'Grace'), (8, 'Heidi'), (9, 'Ivan'), (10, 'Judy');

DELETE FROM test_iceberg_systable_unpartitioned WHERE id % 2 = 1;
DELETE FROM test_iceberg_systable_partitioned WHERE id % 2 = 1;