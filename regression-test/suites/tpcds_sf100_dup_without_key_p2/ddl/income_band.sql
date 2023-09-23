CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk bigint not null,
    ib_lower_bound integer,
    ib_upper_bound integer
)
DISTRIBUTED BY HASH(ib_income_band_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_duplicate_without_keys_by_default" = "true"
);