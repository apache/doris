-- Create user_activity_log tables (COW/MOR, partitioned/non-partitioned) and insert demo data
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS user_activity_log_cow_partition;
DROP TABLE IF EXISTS user_activity_log_cow_non_partition;
DROP TABLE IF EXISTS user_activity_log_mor_partition;
DROP TABLE IF EXISTS user_activity_log_mor_non_partition;

-- Create COW partitioned table
CREATE TABLE IF NOT EXISTS user_activity_log_cow_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING,
  dt STRING
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
PARTITIONED BY (dt)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_cow_partition';

-- Create COW non-partitioned table
CREATE TABLE IF NOT EXISTS user_activity_log_cow_non_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_cow_non_partition';

-- Create MOR partitioned table
CREATE TABLE IF NOT EXISTS user_activity_log_mor_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING,
  dt STRING
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.compact.inline = 'true',
  hoodie.compact.inline.max.delta.commits = '1',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
PARTITIONED BY (dt)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_mor_partition';

-- Create MOR non-partitioned table
CREATE TABLE IF NOT EXISTS user_activity_log_mor_non_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.compact.inline = 'true',
  hoodie.compact.inline.max.delta.commits = '1',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_mor_non_partition';

-- Insert demo data into tables with multiple small inserts for timetravel testing
-- Each INSERT creates a new commit, allowing us to test timetravel queries

-- Insert data into user_activity_log_cow_partition (5 commits, 2 rows each)
INSERT INTO user_activity_log_cow_partition VALUES
  (1, 1710000000000, 'login', '2024-03-01'),
  (2, 1710000001000, 'click', '2024-03-01');
INSERT INTO user_activity_log_cow_partition VALUES
  (3, 1710000002000, 'logout', '2024-03-02'),
  (4, 1710000003000, 'view', '2024-03-01');
INSERT INTO user_activity_log_cow_partition VALUES
  (5, 1710000004000, 'purchase', '2024-03-02'),
  (6, 1710000005000, 'search', '2024-03-01');
INSERT INTO user_activity_log_cow_partition VALUES
  (7, 1710000006000, 'add_to_cart', '2024-03-02'),
  (8, 1710000007000, 'remove_from_cart', '2024-03-01');
INSERT INTO user_activity_log_cow_partition VALUES
  (9, 1710000008000, 'share', '2024-03-02'),
  (10, 1710000009000, 'comment', '2024-03-01');

-- Insert data into user_activity_log_cow_non_partition (5 commits, 2 rows each)
INSERT INTO user_activity_log_cow_non_partition VALUES
  (1, 1710000000000, 'login'),
  (2, 1710000001000, 'click');
INSERT INTO user_activity_log_cow_non_partition VALUES
  (3, 1710000002000, 'logout'),
  (4, 1710000003000, 'view');
INSERT INTO user_activity_log_cow_non_partition VALUES
  (5, 1710000004000, 'purchase'),
  (6, 1710000005000, 'search');
INSERT INTO user_activity_log_cow_non_partition VALUES
  (7, 1710000006000, 'add_to_cart'),
  (8, 1710000007000, 'remove_from_cart');
INSERT INTO user_activity_log_cow_non_partition VALUES
  (9, 1710000008000, 'share'),
  (10, 1710000009000, 'comment');

-- Insert data into user_activity_log_mor_partition (5 commits, 2 rows each)
INSERT INTO user_activity_log_mor_partition VALUES
  (1, 1710000000000, 'login', '2024-03-01'),
  (2, 1710000001000, 'click', '2024-03-01');
INSERT INTO user_activity_log_mor_partition VALUES
  (3, 1710000002000, 'logout', '2024-03-02'),
  (4, 1710000003000, 'view', '2024-03-01');
INSERT INTO user_activity_log_mor_partition VALUES
  (5, 1710000004000, 'purchase', '2024-03-02'),
  (6, 1710000005000, 'search', '2024-03-01');
INSERT INTO user_activity_log_mor_partition VALUES
  (7, 1710000006000, 'add_to_cart', '2024-03-02'),
  (8, 1710000007000, 'remove_from_cart', '2024-03-01');
INSERT INTO user_activity_log_mor_partition VALUES
  (9, 1710000008000, 'share', '2024-03-02'),
  (10, 1710000009000, 'comment', '2024-03-01');

-- Insert data into user_activity_log_mor_non_partition (5 commits, 2 rows each)
INSERT INTO user_activity_log_mor_non_partition VALUES
  (1, 1710000000000, 'login'),
  (2, 1710000001000, 'click');
INSERT INTO user_activity_log_mor_non_partition VALUES
  (3, 1710000002000, 'logout'),
  (4, 1710000003000, 'view');
INSERT INTO user_activity_log_mor_non_partition VALUES
  (5, 1710000004000, 'purchase'),
  (6, 1710000005000, 'search');
INSERT INTO user_activity_log_mor_non_partition VALUES
  (7, 1710000006000, 'add_to_cart'),
  (8, 1710000007000, 'remove_from_cart');
INSERT INTO user_activity_log_mor_non_partition VALUES
  (9, 1710000008000, 'share'),
  (10, 1710000009000, 'comment');

