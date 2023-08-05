LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/store_returns.dat.gz")
    INTO TABLE store_returns
    COLUMNS TERMINATED BY "|"
    (sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss)
)