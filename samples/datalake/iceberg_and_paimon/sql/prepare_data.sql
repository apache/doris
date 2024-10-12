
CREATE TEMPORARY VIEW customer_files
USING org.apache.spark.sql.parquet
OPTIONS (
  path "file:///opt/data/customer/"
);

insert overwrite paimon.db_paimon.customer select c_custkey,c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment from customer_files;