CREATE TABLE IF NOT EXISTS store_sales_agg (
    ss_sold_date_sk bigint,
    ss_sold_time_sk bigint,
    ss_item_sk bigint MAX,
    ss_customer_sk bigint MAX,
    ss_cdemo_sk bigint MAX,
    ss_hdemo_sk bigint MAX,
    ss_addr_sk bigint MAX,
    ss_store_sk bigint MIN,
    ss_promo_sk bigint MIN,
    ss_ticket_number bigint MIN,
    ss_quantity integer MIN,
    ss_wholesale_cost decimal(7,2) SUM,
    ss_list_price decimal(7,2) SUM,
    ss_sales_price decimal(7,2) SUM,
    ss_ext_discount_amt decimal(7,2) SUM,
    ss_ext_sales_price decimal(7,2) SUM,
    ss_ext_wholesale_cost decimal(7,2) SUM,
    ss_ext_list_price decimal(7,2) MAX,
    ss_ext_tax decimal(7,2) MAX,
    ss_coupon_amt decimal(7,2) MAX,
    ss_net_paid decimal(7,2) MIN,
    ss_net_paid_inc_tax decimal(7,2) MIN,
    ss_net_profit decimal(7,2) MIN
)
AGGREGATE KEY(ss_sold_date_sk, ss_sold_time_sk)
DISTRIBUTED BY HASH(ss_sold_date_sk) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
)