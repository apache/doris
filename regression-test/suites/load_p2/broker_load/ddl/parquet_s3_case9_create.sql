create table if not exists parquet_s3_case9 (
tinyint_col           tinyint,
smallint_col          smallint,
int_col               int,
bigint_col            bigint,
boolean_col           boolean,
float_col             float,
double_col            double,
string_col            string,
decimal_col           decimal(12,4),
char_col              char(50),
varchar_col           varchar(50),
date_col              date,
list_double_col       array<double>,
list_string_col       array<string>
) distributed by hash(smallint_col) buckets 3
properties("replication_num" = "1");
