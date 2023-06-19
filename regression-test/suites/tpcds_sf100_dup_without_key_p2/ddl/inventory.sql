CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk bigint not null,
    inv_item_sk bigint not null,
    inv_warehouse_sk bigint,
    inv_quantity_on_hand integer
)
DISTRIBUTED BY HASH(inv_date_sk, inv_item_sk, inv_warehouse_sk) BUCKETS 32
PROPERTIES (
  "replication_num" = "1",
  "enable_duplicate_without_keys_by_default" = "true"
);