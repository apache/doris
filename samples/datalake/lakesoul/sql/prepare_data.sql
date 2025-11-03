CREATE TABLE spark_catalog.default.customer (
    c_custkey INT NOT NULL,
    c_name STRING,
    c_address STRING,
    c_nationkey INT,
    c_phone STRING,
    c_acctbal DECIMAL(15, 2),
    c_mktsegment STRING,
    c_comment STRING
)
USING csv
OPTIONS (path '/data/tpch/customer.tbl', delimiter '|');

USE lakesoul;
CREATE NAMESPACE IF NOT EXISTS tpch;

CREATE TABLE IF NOT EXISTS tpch.customer_from_spark (
  c_custkey INT NOT NULL,
  c_name STRING,
  c_address STRING,
  c_nationkey INT,
  c_phone STRING,
  c_acctbal DECIMAL(15, 2),
  c_mktsegment STRING,
  c_comment STRING)
USING lakesoul
PARTITIONED BY (c_nationkey)
LOCATION 's3://lakesoul-test-bucket/data/tpch/customer_from_spark'
TBLPROPERTIES (
  'hashBucketNum' = '4',
  'hashPartitions' = 'c_custkey');
  

INSERT OVERWRITE lakesoul.tpch.customer_from_spark
SELECT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_comment, c_nationkey
FROM (
    SELECT COALESCE(c_custkey, -1) AS c_custkey,
           c_name, c_address, c_phone, c_mktsegment, c_comment, c_acctbal,
           COALESCE(CAST(c_nationkey AS int), -1) AS c_nationkey
    FROM spark_catalog.default.customer
);
