-- Create MOR non-partitioned table
USE regression_hudi;

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
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_mor_non_partition';

