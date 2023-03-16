CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk bigint not null,
    inv_item_sk bigint not null,
    inv_warehouse_sk bigint,
    inv_quantity_on_hand integer
)
DUPLICATE KEY(inv_date_sk, inv_item_sk, inv_warehouse_sk)
DISTRIBUTED BY HASH(inv_date_sk, inv_item_sk, inv_warehouse_sk) BUCKETS 32
PROPERTIES (
  "replication_num" = "1"
);