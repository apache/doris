CREATE TABLE IF NOT EXISTS reason (
    r_reason_sk bigint not null,
    r_reason_id char(16) not null,
    r_reason_desc char(100)
 )
DISTRIBUTED BY HASH(r_reason_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_duplicate_without_keys_by_default" = "true"
);