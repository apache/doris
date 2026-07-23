use `default`;

drop table if exists test_hive_struct_add_column_orc;

create table test_hive_struct_add_column_orc (
  `id` int,                                         
  `name` string,                                      
  `details` struct<age:int,city:string,email:string,phone:int>,                          
  `sex` int,                                         
  `complex` array<struct<a:int,b:struct<aa:string,bb:int>>>
)
STORED AS ORC
LOCATION '/user/doris/preinstalled_data/orc_table/test_hive_struct_add_column_orc';

drop table if exists test_hive_struct_add_column_parquet;

create table test_hive_struct_add_column_parquet (
  `id` int,                                         
  `name` string,                                      
  `details` struct<age:int,city:string,email:string,phone:int>,                          
  `sex` int,                                         
  `complex` array<struct<a:int,b:struct<aa:string,bb:int>>>
)
STORED AS parquet
LOCATION '/user/doris/preinstalled_data/parquet_table/test_hive_struct_add_column_parquet';

