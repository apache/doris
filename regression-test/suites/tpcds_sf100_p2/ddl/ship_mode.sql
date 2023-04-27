CREATE TABLE IF NOT EXISTS ship_mode (
    sm_ship_mode_sk bigint not null,
    sm_ship_mode_id char(16) not null,
    sm_type char(30),
    sm_code char(10),
    sm_carrier char(20),
    sm_contract char(20)
)
DUPLICATE KEY(sm_ship_mode_sk)
DISTRIBUTED BY HASH(sm_ship_mode_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);