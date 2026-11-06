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

CREATE DATABASE IF NOT EXISTS delta_m1 LOCATION 's3a://delta/warehouse/delta_m1';
DROP TABLE IF EXISTS delta_m1.basic;
CREATE TABLE delta_m1.basic (id BIGINT, value STRING, part STRING)
USING delta PARTITIONED BY (part)
LOCATION 's3a://delta/warehouse/delta_m1/basic'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');
INSERT OVERWRITE delta_m1.basic VALUES (1, 'first', 'a'), (2, 'second', 'b'), (3, 'third', NULL);
SELECT * FROM delta_m1.basic ORDER BY id;
DESCRIBE EXTENDED delta_m1.basic;

DROP TABLE IF EXISTS delta_m1.types;
CREATE TABLE delta_m1.types (
  id BIGINT, b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT,
  f FLOAT, d DOUBLE, dec DECIMAL(20,4), str STRING, bin BINARY,
  dt DATE, ts TIMESTAMP, arr ARRAY<INT>, m MAP<STRING, BIGINT>, nested STRUCT<MiXeD:INT,Txt:STRING>
) USING delta LOCATION 's3a://delta/warehouse/delta_m1/types'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');
INSERT OVERWRITE delta_m1.types VALUES
  (1, true, -128, 32767, 2147483647, 9223372036854775807, 1.25, -2.5, 123456789012.3400,
   '中文 / + %', unhex('00ff1d'), DATE '2024-02-29', TIMESTAMP '2024-03-10 02:30:00.123456',
   array(1, NULL, 3), map('a', 9L, 'b', 10L), named_struct('MiXeD', 7, 'Txt', 'nested')),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT * FROM delta_m1.types ORDER BY id;

DROP TABLE IF EXISTS delta_m1.empty;
CREATE TABLE delta_m1.empty (id BIGINT) USING delta
LOCATION 's3a://delta/warehouse/delta_m1/empty'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');

DROP TABLE IF EXISTS delta_m1.partition_types;
CREATE TABLE delta_m1.partition_types (id BIGINT, p_bool BOOLEAN, p_int INT,
  p_dec DECIMAL(10,2), p_date DATE, p_ts TIMESTAMP, p_str STRING)
USING delta PARTITIONED BY (p_bool,p_int,p_dec,p_date,p_ts,p_str)
LOCATION 's3a://delta/warehouse/delta_m1/partition_types'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');
INSERT OVERWRITE delta_m1.partition_types VALUES
  (1, true, 42, 12.50, DATE '2024-02-29', TIMESTAMP '2024-03-10 02:30:00.123456', 'a/b=c%20+d 中文'),
  (2, false, -5, -9.00, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.123456', '__HIVE_DEFAULT_PARTITION__'),
  (3, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT * FROM delta_m1.partition_types ORDER BY id;
