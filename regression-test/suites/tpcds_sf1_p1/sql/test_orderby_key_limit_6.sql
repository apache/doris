SELECT ss_sold_date_sk, ss_sold_time_sk FROM store_sales
ORDER BY ss_sold_date_sk DESC, ss_sold_time_sk DESC
LIMIT 100
