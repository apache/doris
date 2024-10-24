CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk bigint,
    inv_item_sk bigint,
    inv_warehouse_sk bigint,
    inv_quantity_on_hand integer
)
UNIQUE KEY(inv_date_sk, inv_item_sk, inv_warehouse_sk)
CLUSTER BY(inv_item_sk, inv_date_sk)
DISTRIBUTED BY HASH(inv_date_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

