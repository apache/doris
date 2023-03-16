CREATE TABLE IF NOT EXISTS `datatype` (
    c_bigint bigint,
    c_double double,
    c_string string,
    c_date date,
    c_timestamp datetime,
    c_boolean boolean,
    c_short_decimal decimal(5,2),
    c_long_decimal decimal(27,9)
)
DUPLICATE KEY(c_bigint)
DISTRIBUTED BY HASH(c_bigint) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)
