-- Create timestamp test table
-- This table is used to test timestamp handling with different timezones
USE regression_hudi;

-- Drop existing table if it exists
DROP TABLE IF EXISTS hudi_table_with_timestamp;

-- Set time zone for timestamp insertion
-- Timestamps will be inserted in America/Los_Angeles timezone
SET TIME ZONE 'America/Los_Angeles';

-- Create hudi_table_with_timestamp table
CREATE TABLE IF NOT EXISTS hudi_table_with_timestamp (
    id STRING,
    name STRING,
    event_time TIMESTAMP
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  preCombineField = 'event_time',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/hudi_table_with_timestamp';

-- Insert data with timestamps
-- Note: Timestamps are inserted in America/Los_Angeles timezone
-- The test will query them in different timezones to verify timezone handling
INSERT INTO hudi_table_with_timestamp VALUES
  ('1', 'Alice', TIMESTAMP '2024-10-25 08:00:00'),
  ('2', 'Bob', TIMESTAMP '2024-10-25 09:30:00'),
  ('3', 'Charlie', TIMESTAMP '2024-10-25 11:00:00');

-- Reset time zone to default (UTC) to avoid affecting other scripts
SET TIME ZONE 'UTC';

