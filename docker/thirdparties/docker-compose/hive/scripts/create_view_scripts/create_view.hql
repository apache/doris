use default;
create view test_view1 as select * from sale_table;
create view test_view2 as select * from default.sale_table;
create view test_view3 as select * from sale_table where bill_code="bill_code1";
create view test_view4 as select parquet_zstd_all_types.t_int, parquet_zstd_all_types.t_varchar from parquet_zstd_all_types join multi_catalog.parquet_all_types on parquet_zstd_all_types.t_varchar = parquet_all_types.t_varchar order by t_int limit 10;
create view unsupported_view as select bill_code from sale_table union all select t_varchar from multi_catalog.parquet_all_types order by bill_code limit 10;
