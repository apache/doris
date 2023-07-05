CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk bigint not null,
    ib_lower_bound integer,
    ib_upper_bound integer
)
COMMENT 'duplicate_no_keys'
DISTRIBUTED BY HASH(ib_income_band_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);