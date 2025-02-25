CREATE TABLE IF NOT EXISTS ship_mode (
    sm_ship_mode_sk bigint,
    sm_ship_mode_id char(16),
    sm_type char(30),
    sm_code char(10),
    sm_carrier char(20),
    sm_contract char(20)
)
UNIQUE KEY(sm_ship_mode_sk)
CLUSTER BY(sm_ship_mode_id)
DISTRIBUTED BY HASH(sm_ship_mode_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

