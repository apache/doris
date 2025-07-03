CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk bigint,
    ib_lower_bound integer,
    ib_upper_bound integer
)
UNIQUE KEY(ib_income_band_sk)
CLUSTER BY(ib_lower_bound, ib_upper_bound)
DISTRIBUTED BY HASH(ib_income_band_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

