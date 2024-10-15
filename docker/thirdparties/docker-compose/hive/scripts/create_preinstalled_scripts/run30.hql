CREATE TABLE `test_different_column_orders_orc`(
  `name` string,
  `id` int,
  `city` string,
  `age` int,
  `sex` string)
STORED AS ORC
LOCATION
  '/user/doris/preinstalled_data/test_different_column_orders/orc';

