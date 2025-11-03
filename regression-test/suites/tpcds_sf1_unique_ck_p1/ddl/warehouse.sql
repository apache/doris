CREATE TABLE IF NOT EXISTS warehouse (
    w_warehouse_sk bigint,
    w_warehouse_id char(16),
    w_warehouse_name varchar(20),
    w_warehouse_sq_ft integer,
    w_street_number char(10),
    w_street_name varchar(60),
    w_street_type char(15),
    w_suite_number char(10),
    w_city varchar(60),
    w_county varchar(30),
    w_state char(2),
    w_zip char(10),
    w_country varchar(20),
    w_gmt_offset decimal(5,2)
)
UNIQUE KEY(w_warehouse_sk)
CLUSTER BY(w_warehouse_id)
DISTRIBUTED BY HASH(w_warehouse_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

