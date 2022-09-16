CREATE TABLE IF NOT EXISTS store_returns (
    sr_item_sk bigint not null,
    sr_ticket_number bigint not null,
    sr_returned_date_sk bigint,
    sr_return_time_sk bigint,
    sr_customer_sk bigint,
    sr_cdemo_sk bigint,
    sr_hdemo_sk bigint,
    sr_addr_sk bigint,
    sr_store_sk bigint,
    sr_reason_sk bigint,
    sr_return_quantity integer,
    sr_return_amt decimal(7,2),
    sr_return_tax decimal(7,2),
    sr_return_amt_inc_tax decimal(7,2),
    sr_fee decimal(7,2),
    sr_return_ship_cost decimal(7,2),
    sr_refunded_cash decimal(7,2),
    sr_reversed_charge decimal(7,2),
    sr_store_credit decimal(7,2),
    sr_net_loss decimal(7,2)
)
duplicate key(sr_item_sk, sr_ticket_number)
distributed by hash (sr_item_sk, sr_ticket_number) buckets 32
properties (
  "replication_num" = "1",
  "colocate_with" = "store"
);