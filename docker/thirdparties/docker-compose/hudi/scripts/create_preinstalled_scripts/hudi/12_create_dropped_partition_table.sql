-- A table with a partition that exists at one instant and is gone at the next.
--
-- This is the one shape that tells a snapshot-exact partition listing from a listing that only
-- looks like one. Every other partitioned table here only ever gains partitions, so listing "now"
-- and listing "at a past instant" return the same set and a wrong implementation reads as correct.
-- Here a FOR TIME AS OF read before the DROP must still see the dropped partition's rows: hive sync
-- removes the partition from HMS, so a listing that starts from HMS and can only subtract would
-- prune it away and silently return fewer rows.
USE regression_hudi;

DROP TABLE IF EXISTS dropped_partition_tb;

CREATE TABLE IF NOT EXISTS dropped_partition_tb (
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
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/dropped_partition_tb';

-- One commit, two partitions. Every live row afterwards comes from this commit, so the test can
-- find the instant to travel to with a DISTINCT over _hoodie_commit_time.
INSERT INTO dropped_partition_tb VALUES
  (1, 'keep1', 'KEEP'),
  (2, 'gone1', 'GONE');

-- ... and now one of them disappears, from the table and from HMS.
ALTER TABLE dropped_partition_tb DROP PARTITION (part1 = 'GONE');
