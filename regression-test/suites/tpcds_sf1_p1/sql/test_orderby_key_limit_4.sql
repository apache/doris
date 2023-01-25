SELECT ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk FROM store_sales
ORDER BY ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk
LIMIT 100
