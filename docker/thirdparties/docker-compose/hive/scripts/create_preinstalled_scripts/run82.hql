use `default`;

create table decimals_1_10 
(
    d_1 DECIMAL(1, 0), 
    d_10 DECIMAL(10, 0)
) stored as PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/decimals_1_10';
