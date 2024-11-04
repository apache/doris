CREATE NAMESPACE IF NOT EXISTS `lakesoul`.`demo`;
CREATE TABLE IF NOT EXISTS `lakesoul`.`demo`.customer(
  c_custkey INT, 
  c_name STRING, 
  c_address STRING,
  c_nationkey STRING,
  c_phone STRING,
  c_acctbal STRING,
  c_mktsegment STRING,
  c_comment STRING
  ) 
  USING lakesoul 
  -- PARTITIONED BY (c_nationkey) 
  LOCATION 's3://lakesoul-test-bucket/demo/customer';
  -- TBLPROPERTIES('hashPartitions'='c_custkey', 'hashBucketNum'='2');

CREATE TEMPORARY VIEW customer_files
USING org.apache.spark.sql.parquet
OPTIONS (
  path "file:///opt/data/customer/"
);


insert overwrite `lakesoul`.`demo`.customer select c_custkey,c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment from customer_files;