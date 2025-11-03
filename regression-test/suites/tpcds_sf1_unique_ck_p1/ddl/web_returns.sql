CREATE TABLE IF NOT EXISTS web_returns (
    wr_item_sk bigint,
    wr_order_number bigint,
    wr_returned_date_sk bigint,
    wr_returned_time_sk bigint,
    wr_refunded_customer_sk bigint,
    wr_refunded_cdemo_sk bigint,
    wr_refunded_hdemo_sk bigint,
    wr_refunded_addr_sk bigint,
    wr_returning_customer_sk bigint,
    wr_returning_cdemo_sk bigint,
    wr_returning_hdemo_sk bigint,
    wr_returning_addr_sk bigint,
    wr_web_page_sk bigint,
    wr_reason_sk bigint,
    wr_return_quantity integer,
    wr_return_amt decimal(7,2),
    wr_return_tax decimal(7,2),
    wr_return_amt_inc_tax decimal(7,2),
    wr_fee decimal(7,2),
    wr_return_ship_cost decimal(7,2),
    wr_refunded_cash decimal(7,2),
    wr_reversed_charge decimal(7,2),
    wr_account_credit decimal(7,2),
    wr_net_loss decimal(7,2)
)
UNIQUE KEY(wr_item_sk, wr_order_number)
CLUSTER BY(wr_net_loss)
DISTRIBUTED BY HASH(wr_item_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)
