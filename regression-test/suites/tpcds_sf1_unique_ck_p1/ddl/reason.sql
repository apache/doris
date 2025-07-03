CREATE TABLE IF NOT EXISTS reason (
    r_reason_sk bigint,
    r_reason_id char(16),
    r_reason_desc char(100)
 )
UNIQUE KEY(r_reason_sk)
CLUSTER BY(r_reason_id, r_reason_sk)
DISTRIBUTED BY HASH(r_reason_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

