CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk bigint,
    ib_lower_bound integer,
    ib_upper_bound integer
)
UNIQUE KEY(ib_income_band_sk)
DISTRIBUTED BY HASH(ib_income_band_sk) BUCKETS 3
PROPERTIES (
  "compression"="zstd",
  "replication_num" = "1"
)

