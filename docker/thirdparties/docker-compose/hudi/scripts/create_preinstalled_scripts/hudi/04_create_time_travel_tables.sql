-- Create time travel test tables
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS timetravel_cow;
DROP TABLE IF EXISTS timetravel_mor;

CREATE TABLE IF NOT EXISTS timetravel_cow (
  id BIGINT,
  name STRING,
  value DOUBLE
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/timetravel_cow';

CREATE TABLE IF NOT EXISTS timetravel_mor (
  id BIGINT,
  name STRING,
  value DOUBLE
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'id',
  hoodie.compact.inline = 'true',
  hoodie.compact.inline.max.delta.commits = '1',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/timetravel_mor';

-- Insert initial data for time travel testing
INSERT INTO timetravel_cow VALUES
  (1, 'initial', 100.0),
  (2, 'initial', 200.0);

INSERT INTO timetravel_mor VALUES
  (1, 'initial', 100.0),
  (2, 'initial', 200.0);

