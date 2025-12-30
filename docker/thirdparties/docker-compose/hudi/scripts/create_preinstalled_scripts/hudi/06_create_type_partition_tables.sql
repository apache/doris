-- Create partition tables with different partition column types
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS boolean_partition_tb;
DROP TABLE IF EXISTS tinyint_partition_tb;
DROP TABLE IF EXISTS smallint_partition_tb;
DROP TABLE IF EXISTS int_partition_tb;
DROP TABLE IF EXISTS bigint_partition_tb;
DROP TABLE IF EXISTS string_partition_tb;
DROP TABLE IF EXISTS date_partition_tb;
DROP TABLE IF EXISTS timestamp_partition_tb;

CREATE TABLE IF NOT EXISTS boolean_partition_tb (
  id BIGINT,
  name STRING,
  part1 BOOLEAN
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/boolean_partition_tb';

CREATE TABLE IF NOT EXISTS tinyint_partition_tb (
  id BIGINT,
  name STRING,
  part1 TINYINT
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/tinyint_partition_tb';

CREATE TABLE IF NOT EXISTS smallint_partition_tb (
  id BIGINT,
  name STRING,
  part1 SMALLINT
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/smallint_partition_tb';

CREATE TABLE IF NOT EXISTS int_partition_tb (
  id BIGINT,
  name STRING,
  part1 INT
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/int_partition_tb';

CREATE TABLE IF NOT EXISTS bigint_partition_tb (
  id BIGINT,
  name STRING,
  part1 BIGINT
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/bigint_partition_tb';

CREATE TABLE IF NOT EXISTS string_partition_tb (
  id BIGINT,
  name STRING,
  part1 STRING
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/string_partition_tb';

CREATE TABLE IF NOT EXISTS date_partition_tb (
  id BIGINT,
  name STRING,
  part1 DATE
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/date_partition_tb';

CREATE TABLE IF NOT EXISTS timestamp_partition_tb (
  id BIGINT,
  name STRING,
  part1 TIMESTAMP
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/timestamp_partition_tb';

-- Insert data
INSERT INTO boolean_partition_tb VALUES
  (1, 'name1', true),
  (2, 'name2', true),
  (3, 'name3', false);

INSERT INTO tinyint_partition_tb VALUES
  (1, 'name1', 1),
  (2, 'name2', 1),
  (3, 'name3', 2);

INSERT INTO smallint_partition_tb VALUES
  (1, 'name1', 10),
  (2, 'name2', 10),
  (3, 'name3', 20);

INSERT INTO int_partition_tb VALUES
  (1, 'name1', 100),
  (2, 'name2', 100),
  (3, 'name3', 200);

INSERT INTO bigint_partition_tb VALUES
  (1, 'name1', 1234567890),
  (2, 'name2', 1234567890),
  (3, 'name3', 9876543210);

INSERT INTO string_partition_tb VALUES
  (1, 'name1', 'RegionA'),
  (2, 'name2', 'RegionA'),
  (3, 'name3', 'RegionB');

INSERT INTO date_partition_tb VALUES
  (1, 'name1', DATE '2023-12-01'),
  (2, 'name2', DATE '2023-12-01'),
  (3, 'name3', DATE '2023-12-02');

INSERT INTO timestamp_partition_tb VALUES
  (1, 'name1', TIMESTAMP '2023-12-01 08:00:00'),
  (2, 'name2', TIMESTAMP '2023-12-01 08:00:00'),
  (3, 'name3', TIMESTAMP '2023-12-01 09:00:00');

