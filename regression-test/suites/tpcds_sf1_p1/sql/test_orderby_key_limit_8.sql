SELECT * FROM store_sales
ORDER BY ss_sold_date_sk DESC, ss_sold_time_sk DESC, ss_item_sk DESC, ss_customer_sk DESC
LIMIT 100
