CREATE TABLE IF NOT EXISTS catalog_sales (
    cs_item_sk bigint,
    cs_order_number bigint,
    cs_sold_date_sk bigint,
    cs_sold_time_sk bigint,
    cs_ship_date_sk bigint,
    cs_bill_customer_sk bigint,
    cs_bill_cdemo_sk bigint,
    cs_bill_hdemo_sk bigint,
    cs_bill_addr_sk bigint,
    cs_ship_customer_sk bigint,
    cs_ship_cdemo_sk bigint,
    cs_ship_hdemo_sk bigint,
    cs_ship_addr_sk bigint,
    cs_call_center_sk bigint,
    cs_catalog_page_sk bigint,
    cs_ship_mode_sk bigint,
    cs_warehouse_sk bigint,
    cs_promo_sk bigint,
    cs_quantity integer,
    cs_wholesale_cost decimal(7,2),
    cs_list_price decimal(7,2),
    cs_sales_price decimal(7,2),
    cs_ext_discount_amt decimal(7,2),
    cs_ext_sales_price decimal(7,2),
    cs_ext_wholesale_cost decimal(7,2),
    cs_ext_list_price decimal(7,2),
    cs_ext_tax decimal(7,2),
    cs_coupon_amt decimal(7,2),
    cs_ext_ship_cost decimal(7,2),
    cs_net_paid decimal(7,2),
    cs_net_paid_inc_tax decimal(7,2),
    cs_net_paid_inc_ship decimal(7,2),
    cs_net_paid_inc_ship_tax decimal(7,2),
    cs_net_profit decimal(7,2)
)
UNIQUE KEY(cs_item_sk, cs_order_number)
CLUSTER BY(cs_list_price, cs_call_center_sk, cs_item_sk)
DISTRIBUTED BY HASH(cs_item_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

