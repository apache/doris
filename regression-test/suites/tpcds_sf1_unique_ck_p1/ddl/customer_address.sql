CREATE TABLE IF NOT EXISTS customer_address (
    ca_address_sk bigint,
    ca_address_id char(16),
    ca_street_number char(10),
    ca_street_name varchar(60),
    ca_street_type char(15),
    ca_suite_number char(10),
    ca_city varchar(60),
    ca_county varchar(30),
    ca_state char(2),
    ca_zip char(10),
    ca_country varchar(20),
    ca_gmt_offset decimal(5,2),
    ca_location_type char(20)
)
UNIQUE KEY(ca_address_sk)
CLUSTER BY(ca_address_id, ca_address_sk, ca_street_number)
DISTRIBUTED BY HASH(ca_address_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

