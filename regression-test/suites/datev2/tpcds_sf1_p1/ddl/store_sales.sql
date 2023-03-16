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
    ss_wholesale_cost decimalv3(7,2),
    ss_list_price decimalv3(7,2),
    ss_sales_price decimalv3(7,2),
    ss_ext_discount_amt decimalv3(7,2),
    ss_ext_sales_price decimalv3(7,2),
    ss_ext_wholesale_cost decimalv3(7,2),
    ss_ext_list_price decimalv3(7,2),
    ss_ext_tax decimalv3(7,2),
    ss_coupon_amt decimalv3(7,2),
    ss_net_paid decimalv3(7,2),
    ss_net_paid_inc_tax decimalv3(7,2),
    ss_net_profit decimalv3(7,2)
)
DUPLICATE KEY(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk)
DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)


