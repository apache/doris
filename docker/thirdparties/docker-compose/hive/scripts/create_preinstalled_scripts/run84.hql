use `default`;

drop table if exists fact_big;

create table fact_big (
  k        INT,
  c1       INT,
  c2       BIGINT,
  c3       DOUBLE,
  c4       STRING
)stored as parquet
LOCATION '/user/doris/preinstalled_data/parquet_table/runtime_filter_fact_big';

drop table if exists dim_small;

create table dim_small (
  k        INT,
  c1       INT,
  c2       BIGINT
)stored as parquet
LOCATION '/user/doris/preinstalled_data/parquet_table/runtime_filter_dim_small';
