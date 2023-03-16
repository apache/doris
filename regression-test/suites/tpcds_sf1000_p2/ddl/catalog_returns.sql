CREATE TABLE IF NOT EXISTS catalog_returns (
    cr_returned_date_sk bigint,
    cr_returned_time_sk bigint,
    cr_item_sk bigint,
    cr_refunded_customer_sk bigint,
    cr_refunded_cdemo_sk bigint,
    cr_refunded_hdemo_sk bigint,
    cr_refunded_addr_sk bigint,
    cr_returning_customer_sk bigint,
    cr_returning_cdemo_sk bigint,
    cr_returning_hdemo_sk bigint,
    cr_returning_addr_sk bigint,
    cr_call_center_sk bigint,
    cr_catalog_page_sk bigint,
    cr_ship_mode_sk bigint,
    cr_warehouse_sk bigint,
    cr_reason_sk bigint,
    cr_order_number bigint,
    cr_return_quantity integer,
    cr_return_amount decimal(7,2),
    cr_return_tax decimal(7,2),
    cr_return_amt_inc_tax decimal(7,2),
    cr_fee decimal(7,2),
    cr_return_ship_cost decimal(7,2),
    cr_refunded_cash decimal(7,2),
    cr_reversed_charge decimal(7,2),
    cr_store_credit decimal(7,2),
    cr_net_loss decimal(7,2)
)
DUPLICATE KEY(cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk)
DISTRIBUTED BY HASH(cr_refunded_customer_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)
