-- Create partition tables for partition pruning tests
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS one_partition_tb;
DROP TABLE IF EXISTS two_partition_tb;
DROP TABLE IF EXISTS three_partition_tb;

-- One partition table
CREATE TABLE IF NOT EXISTS one_partition_tb (
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
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/one_partition_tb';

-- Two partition table
CREATE TABLE IF NOT EXISTS two_partition_tb (
  id BIGINT,
  name STRING,
  part1 STRING,
  part2 INT
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1, part2)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/two_partition_tb';

-- Three partition table
CREATE TABLE IF NOT EXISTS three_partition_tb (
  id BIGINT,
  name STRING,
  part1 STRING,
  part2 INT,
  part3 STRING
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
PARTITIONED BY (part1, part2, part3)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/three_partition_tb';

-- Insert data
INSERT INTO one_partition_tb VALUES
  (1, 'name1', 2024),
  (2, 'name2', 2024),
  (3, 'name3', 2025),
  (4, 'name4', 2025),
  (5, 'name5', 2024);

INSERT INTO two_partition_tb VALUES
  (1, 'name1', 'US', 1),
  (2, 'name2', 'US', 1),
  (3, 'name3', 'US', 2),
  (4, 'name4', 'US', 2),
  (5, 'name5', 'EU', 1),
  (6, 'name6', 'EU', 2),
  (7, 'name7', 'EU', 2),
  (8, 'name8', 'EU', 2);

INSERT INTO three_partition_tb VALUES
  (1, 'name1', 'US', 2024, 'Q1'),
  (2, 'name2', 'US', 2024, 'Q1'),
  (3, 'name3', 'US', 2024, 'Q2'),
  (4, 'name4', 'US', 2024, 'Q2'),
  (5, 'name5', 'US', 2025, 'Q1'),
  (6, 'name6', 'US', 2025, 'Q2'),
  (7, 'name7', 'EU', 2024, 'Q1'),
  (8, 'name8', 'EU', 2024, 'Q2'),
  (9, 'name9', 'EU', 2025, 'Q1'),
  (10, 'name10', 'EU', 2025, 'Q2'),
  (11, 'name11', 'AS', 2025, 'Q1'),
  (12, 'name12', 'AS', 2025, 'Q2');

