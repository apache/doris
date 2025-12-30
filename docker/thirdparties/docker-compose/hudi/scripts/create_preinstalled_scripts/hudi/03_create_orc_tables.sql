-- Create ORC format Hudi tables
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS orc_hudi_table_cow;
DROP TABLE IF EXISTS orc_hudi_table_mor;

CREATE TABLE IF NOT EXISTS orc_hudi_table_cow (
  id BIGINT,
  name STRING,
  value DOUBLE
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'id',
  hoodie.base.file.format = 'orc',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/orc_hudi_table_cow';

CREATE TABLE IF NOT EXISTS orc_hudi_table_mor (
  id BIGINT,
  name STRING,
  value DOUBLE
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'id',
  hoodie.base.file.format = 'orc',
  hoodie.compact.inline = 'true',
  hoodie.compact.inline.max.delta.commits = '1',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/orc_hudi_table_mor';

INSERT INTO orc_hudi_table_cow VALUES
  (1, 'test1', 10.5),
  (2, 'test2', 20.5);

INSERT INTO orc_hudi_table_mor VALUES
  (1, 'test1', 10.5),
  (2, 'test2', 20.5);

