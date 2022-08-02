CREATE TABLE IF NOT EXISTS reason (
    r_reason_sk bigint,
    r_reason_id char(16),
    r_reason_desc char(100)
 )
DUPLICATE KEY(r_reason_sk, r_reason_id)
DISTRIBUTED BY HASH(r_reason_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

