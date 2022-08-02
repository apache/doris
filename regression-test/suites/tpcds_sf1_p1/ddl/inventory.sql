CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk bigint,
    inv_item_sk bigint,
    inv_warehouse_sk bigint,
    inv_quantity_on_hand integer
)
DUPLICATE KEY(inv_date_sk, inv_item_sk)
DISTRIBUTED BY HASH(inv_warehouse_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

