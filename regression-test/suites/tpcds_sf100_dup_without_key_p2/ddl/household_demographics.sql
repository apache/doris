CREATE TABLE IF NOT EXISTS household_demographics (
    hd_demo_sk bigint not null,
    hd_income_band_sk bigint,
    hd_buy_potential char(15),
    hd_dep_count integer,
    hd_vehicle_count integer
)
DISTRIBUTED BY HASH(hd_demo_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1",
  "enable_duplicate_without_keys_by_default" = "true"
);