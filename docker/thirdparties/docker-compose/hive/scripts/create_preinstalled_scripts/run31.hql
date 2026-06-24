drop table if exists `test_different_column_orders_parquet`;
create table `test_different_column_orders_parquet`(
  `name` string,
  `id` int,
  `city` string,
  `age` int,
  `sex` string)
STORED AS PARQUET
LOCATION
  '/user/doris/preinstalled_data/test_different_column_orders/parquet';

