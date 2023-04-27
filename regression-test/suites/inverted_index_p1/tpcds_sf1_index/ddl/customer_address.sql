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
    ca_location_type char(20),
    INDEX ca_street_number_idx(ca_street_number) USING INVERTED PROPERTIES("parser"="none") COMMENT "ca_street_number index",
    INDEX ca_street_name_idx(ca_street_name) USING INVERTED COMMENT "ca_street_name index",
    INDEX ca_city_idx(ca_city) USING INVERTED PROPERTIES("parser"="standard") COMMENT "ca_city index",
    INDEX ca_county_idx(ca_county) USING INVERTED COMMENT "ca_county index"
)
DUPLICATE KEY(ca_address_sk, ca_address_id)
DISTRIBUTED BY HASH(ca_address_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

