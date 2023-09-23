CREATE TABLE IF NOT EXISTS store_sales (
    ss_sold_date_sk bigint,
    ss_sold_time_sk bigint,
    ss_item_sk bigint,
    ss_customer_sk bigint,
    ss_cdemo_sk bigint,
    ss_hdemo_sk bigint,
    ss_addr_sk bigint,
    ss_store_sk bigint,
    ss_promo_sk bigint,
    ss_ticket_number bigint,
    ss_quantity integer,
    ss_wholesale_cost decimal(7,2),
    ss_list_price decimal(7,2),
    ss_sales_price decimal(7,2),
    ss_ext_discount_amt decimal(7,2),
    ss_ext_sales_price decimal(7,2),
    ss_ext_wholesale_cost decimal(7,2),
    ss_ext_list_price decimal(7,2),
    ss_ext_tax decimal(7,2),
    ss_coupon_amt decimal(7,2),
    ss_net_paid decimal(7,2),
    ss_net_paid_inc_tax decimal(7,2),
    ss_net_profit decimal(7,2),
    INDEX ss_sold_date_sk_idx(ss_sold_date_sk) USING BITMAP COMMENT "ss_sold_date_sk index",
    INDEX ss_sold_time_sk_idx(ss_sold_time_sk) USING INVERTED COMMENT "ss_sold_time_sk index",
    INDEX ss_item_sk_idx(ss_item_sk) USING INVERTED COMMENT "ss_item_sk index",
    INDEX ss_customer_sk_idx(ss_customer_sk) USING INVERTED COMMENT "ss_customer_sk index",
    INDEX ss_quantity_idx(ss_quantity) USING INVERTED COMMENT "ss_quantity index"
)
DUPLICATE KEY(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk)
DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)


