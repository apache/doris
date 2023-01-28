CREATE TABLE IF NOT EXISTS household_demographics (
    hd_demo_sk bigint,
    hd_income_band_sk bigint,
    hd_buy_potential char(15),
    hd_dep_count integer,
    hd_vehicle_count integer,
    INDEX hd_demo_sk_idx(hd_demo_sk) USING INVERTED COMMENT "hd_demo_sk index",
    INDEX hd_income_band_sk_idx(hd_income_band_sk) USING INVERTED COMMENT "hd_income_band_sk index",
    INDEX hd_buy_potential_idx(hd_buy_potential) USING INVERTED COMMENT "hd_buy_potential index"
)
DUPLICATE KEY(hd_demo_sk, hd_income_band_sk)
DISTRIBUTED BY HASH(hd_demo_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

