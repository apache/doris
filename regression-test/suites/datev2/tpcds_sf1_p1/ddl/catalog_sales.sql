CREATE TABLE IF NOT EXISTS catalog_sales (
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
    cs_item_sk bigint,
    cs_promo_sk bigint,
    cs_order_number bigint,
    cs_quantity integer,
    cs_wholesale_cost decimalv3(7,2),
    cs_list_price decimalv3(7,2),
    cs_sales_price decimalv3(7,2),
    cs_ext_discount_amt decimalv3(7,2),
    cs_ext_sales_price decimalv3(7,2),
    cs_ext_wholesale_cost decimalv3(7,2),
    cs_ext_list_price decimalv3(7,2),
    cs_ext_tax decimalv3(7,2),
    cs_coupon_amt decimalv3(7,2),
    cs_ext_ship_cost decimalv3(7,2),
    cs_net_paid decimalv3(7,2),
    cs_net_paid_inc_tax decimalv3(7,2),
    cs_net_paid_inc_ship decimalv3(7,2),
    cs_net_paid_inc_ship_tax decimalv3(7,2),
    cs_net_profit decimalv3(7,2)
)
DUPLICATE KEY(cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk)
DISTRIBUTED BY HASH(cs_bill_customer_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

