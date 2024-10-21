CREATE TABLE IF NOT EXISTS web_sales (
    ws_item_sk bigint,
    ws_order_number bigint,
    ws_sold_date_sk bigint,
    ws_sold_time_sk bigint,
    ws_ship_date_sk bigint,
    ws_bill_customer_sk bigint,
    ws_bill_cdemo_sk bigint,
    ws_bill_hdemo_sk bigint,
    ws_bill_addr_sk bigint,
    ws_ship_customer_sk bigint,
    ws_ship_cdemo_sk bigint,
    ws_ship_hdemo_sk bigint,
    ws_ship_addr_sk bigint,
    ws_web_page_sk bigint,
    ws_web_site_sk bigint,
    ws_ship_mode_sk bigint,
    ws_warehouse_sk bigint,
    ws_promo_sk bigint,
    ws_quantity integer,
    ws_wholesale_cost decimal(7,2),
    ws_list_price decimal(7,2),
    ws_sales_price decimal(7,2),
    ws_ext_discount_amt decimal(7,2),
    ws_ext_sales_price decimal(7,2),
    ws_ext_wholesale_cost decimal(7,2),
    ws_ext_list_price decimal(7,2),
    ws_ext_tax decimal(7,2),
    ws_coupon_amt decimal(7,2),
    ws_ext_ship_cost decimal(7,2),
    ws_net_paid decimal(7,2),
    ws_net_paid_inc_tax decimal(7,2),
    ws_net_paid_inc_ship decimal(7,2),
    ws_net_paid_inc_ship_tax decimal(7,2),
    ws_net_profit decimal(7,2)
)
UNIQUE KEY(ws_item_sk, ws_order_number)
CLUSTER BY(ws_net_profit, ws_quantity)
DISTRIBUTED BY HASH(ws_item_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)

