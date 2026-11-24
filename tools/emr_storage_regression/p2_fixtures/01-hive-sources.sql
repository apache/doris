-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Fresh QA fixture initialization. Existing target tables cause an error; no DROP is issued.
CREATE DATABASE IF NOT EXISTS regression;
CREATE DATABASE IF NOT EXISTS regression_iceberg;
CREATE DATABASE IF NOT EXISTS regression_paimon;
CREATE TABLE regression.hive_migrate_parquet (
    id INT,
    name STRING,
    arr_col ARRAY<INT>,
    map_col MAP<STRING, INT>,
    struct_col STRUCT<name:STRING, age:INT>,
    arr_struct_col ARRAY<STRUCT<city:STRING, zip:INT>>,
    map_array_col MAP<STRING, ARRAY<INT>>
) STORED AS PARQUET;
INSERT INTO TABLE regression.hive_migrate_parquet VALUES (
  1, 'a', array(1,2,3), map('a',10,'b',20),
  named_struct('name','Alice','age',25),
  array(named_struct('city','Beijing','zip',100000),named_struct('city','Shanghai','zip',200000)),
  map('x',array(1,2),'y',array(3,4))
),(
  2, 'b', array(4,5), map('c',30,'d',40),
  named_struct('name','Bob','age',30),
  array(named_struct('city','Shenzhen','zip',518000)),
  map('k1',array(5,6))
);
CREATE TABLE regression_iceberg.hive_migrate_partition_parquet (id INT, name STRING) PARTITIONED BY (dt STRING) STORED AS PARQUET;
INSERT INTO regression_iceberg.hive_migrate_partition_parquet PARTITION(dt='2024-01-01') VALUES (1,'a'),(2,'b');
INSERT INTO regression_iceberg.hive_migrate_partition_parquet PARTITION(dt='2024-01-02') VALUES (3,'c');
CREATE TABLE regression_paimon.hive_origin_parquet (id BIGINT, user_name STRING, amount DECIMAL(10,2)) PARTITIONED BY(dt STRING) STORED AS PARQUET;
INSERT INTO regression_paimon.hive_origin_parquet PARTITION(dt='2026-04-01') VALUES (1,'alice',CAST(10.50 AS DECIMAL(10,2)));
INSERT INTO regression_paimon.hive_origin_parquet PARTITION(dt='2026-04-01') VALUES (2,'bob',CAST(20.00 AS DECIMAL(10,2)));
INSERT INTO regression_paimon.hive_origin_parquet PARTITION(dt='2026-04-02') VALUES (3,'carol',CAST(30.25 AS DECIMAL(10,2)));
CREATE TABLE regression.hive_migrate_orc (
    id INT,
    name STRING,
    arr_col ARRAY<INT>,
    map_col MAP<STRING, INT>,
    struct_col STRUCT<name:STRING, age:INT>,
    arr_struct_col ARRAY<STRUCT<city:STRING, zip:INT>>,
    map_array_col MAP<STRING, ARRAY<INT>>
) STORED AS ORC;
INSERT INTO TABLE regression.hive_migrate_orc VALUES (
  1, 'a', array(1,2,3), map('a',10,'b',20),
  named_struct('name','Alice','age',25),
  array(named_struct('city','Beijing','zip',100000),named_struct('city','Shanghai','zip',200000)),
  map('x',array(1,2),'y',array(3,4))
),(
  2, 'b', array(4,5), map('c',30,'d',40),
  named_struct('name','Bob','age',30),
  array(named_struct('city','Shenzhen','zip',518000)),
  map('k1',array(5,6))
);
CREATE TABLE regression_iceberg.hive_migrate_partition_orc (id INT, name STRING) PARTITIONED BY (dt STRING) STORED AS ORC;
INSERT INTO regression_iceberg.hive_migrate_partition_orc PARTITION(dt='2024-01-01') VALUES (1,'a'),(2,'b');
INSERT INTO regression_iceberg.hive_migrate_partition_orc PARTITION(dt='2024-01-02') VALUES (3,'c');
CREATE TABLE regression_paimon.hive_origin_orc (id BIGINT, user_name STRING, amount DECIMAL(10,2)) PARTITIONED BY(dt STRING) STORED AS ORC;
INSERT INTO regression_paimon.hive_origin_orc PARTITION(dt='2026-04-01') VALUES (1,'alice',CAST(10.50 AS DECIMAL(10,2)));
INSERT INTO regression_paimon.hive_origin_orc PARTITION(dt='2026-04-01') VALUES (2,'bob',CAST(20.00 AS DECIMAL(10,2)));
INSERT INTO regression_paimon.hive_origin_orc PARTITION(dt='2026-04-02') VALUES (3,'carol',CAST(30.25 AS DECIMAL(10,2)));
