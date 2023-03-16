CREATE TABLE IF NOT EXISTS store_returns (
    sr_returned_date_sk bigint,
    sr_return_time_sk bigint,
    sr_item_sk bigint,
    sr_customer_sk bigint,
    sr_cdemo_sk bigint,
    sr_hdemo_sk bigint,
    sr_addr_sk bigint,
    sr_store_sk bigint,
    sr_reason_sk bigint,
    sr_ticket_number bigint,
    sr_return_quantity integer,
    sr_return_amt decimalv3(7,2),
    sr_return_tax decimalv3(7,2),
    sr_return_amt_inc_tax decimalv3(7,2),
    sr_fee decimalv3(7,2),
    sr_return_ship_cost decimalv3(7,2),
    sr_refunded_cash decimalv3(7,2),
    sr_reversed_charge decimalv3(7,2),
    sr_store_credit decimalv3(7,2),
    sr_net_loss decimalv3(7,2)
)
DUPLICATE KEY(sr_returned_date_sk, sr_return_time_sk, sr_item_sk)
DISTRIBUTED BY HASH(sr_return_time_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

