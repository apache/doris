CREATE TABLE IF NOT EXISTS gavin_test (
    id bigint,
    name char(16),
    score bigint
)
DUPLICATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
  "enable_mow_light_delete" = "true",
  "replication_num" = "1"
)
