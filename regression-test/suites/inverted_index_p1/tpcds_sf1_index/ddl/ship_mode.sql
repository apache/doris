CREATE TABLE IF NOT EXISTS ship_mode (
    sm_ship_mode_sk bigint,
    sm_ship_mode_id char(16),
    sm_type char(30),
    sm_code char(10),
    sm_carrier char(20),
    sm_contract char(20),
    INDEX sm_ship_mode_sk_idx(sm_ship_mode_sk) USING INVERTED COMMENT "sm_ship_mode_sk index",
    INDEX sm_type_type(sm_type) USING INVERTED COMMENT "sm_type index"
)
DUPLICATE KEY(sm_ship_mode_sk, sm_ship_mode_id)
DISTRIBUTED BY HASH(sm_ship_mode_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

