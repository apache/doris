-- Create COW partitioned table
USE regression_hudi;

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
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
PARTITIONED BY (dt)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_cow_partition';

