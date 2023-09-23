CREATE TABLE IF NOT EXISTS  dbgen_version (
  dv_version varchar(16) NULL,
  dv_create_date date NULL,
  dv_create_time char(10) NULL,
  dv_cmdline_args varchar(200) NULL
)
DISTRIBUTED BY HASH(dv_version) BUCKETS 1 
PROPERTIES (
  "replication_num" = "1",
  "enable_duplicate_without_keys_by_default" = "true"
);