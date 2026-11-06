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


CREATE OR REPLACE TABLE delta_m1.mapping_name (
  id BIGINT, old STRING, nested STRUCT<Before:INT,Keep:STRING>,
  arr ARRAY<STRUCT<Before:INT>>, m MAP<STRING,STRUCT<Before:INT>>, part STRING)
USING delta PARTITIONED BY (part)
LOCATION 's3a://delta/warehouse/delta_m1/mapping_name'
TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'='false');
INSERT INTO delta_m1.mapping_name VALUES
  (1, 'old-value', named_struct('Before', 10, 'Keep', 'old-nested'),
   array(named_struct('Before', 100)), map('k', named_struct('Before', 1000)), 'a/b');
ALTER TABLE delta_m1.mapping_name RENAME COLUMN id TO renamed;
ALTER TABLE delta_m1.mapping_name RENAME COLUMN nested.Before TO After;
ALTER TABLE delta_m1.mapping_name RENAME COLUMN arr.element.Before TO After;
ALTER TABLE delta_m1.mapping_name RENAME COLUMN m.value.Before TO After;
ALTER TABLE delta_m1.mapping_name RENAME COLUMN part TO partition_renamed;
ALTER TABLE delta_m1.mapping_name DROP COLUMN old;
ALTER TABLE delta_m1.mapping_name ADD COLUMNS (old STRING);
ALTER TABLE delta_m1.mapping_name DROP COLUMN nested.Keep;
ALTER TABLE delta_m1.mapping_name ADD COLUMNS (nested.Keep STRING);
INSERT INTO delta_m1.mapping_name (renamed, nested, arr, m, partition_renamed, old) VALUES
  (2, named_struct('After', 20, 'Keep', 'new-nested'), array(named_struct('After', 200)),
   map('k', named_struct('After', 2000)), NULL, 'new-value');
SELECT * FROM delta_m1.mapping_name ORDER BY renamed;

CREATE OR REPLACE TABLE delta_m1.mapping_id (
  id BIGINT, old STRING, nested STRUCT<Before:INT,Keep:STRING>,
  arr ARRAY<STRUCT<Before:INT>>, m MAP<STRING,STRUCT<Before:INT>>, part STRING)
USING delta PARTITIONED BY (part)
LOCATION 's3a://delta/warehouse/delta_m1/mapping_id'
TBLPROPERTIES ('delta.columnMapping.mode'='id', 'delta.enableDeletionVectors'='false');
INSERT INTO delta_m1.mapping_id VALUES
  (1, 'old-value', named_struct('Before', 10, 'Keep', 'old-nested'),
   array(named_struct('Before', 100)), map('k', named_struct('Before', 1000)), 'a/b');
ALTER TABLE delta_m1.mapping_id RENAME COLUMN id TO renamed;
ALTER TABLE delta_m1.mapping_id RENAME COLUMN nested.Before TO After;
ALTER TABLE delta_m1.mapping_id RENAME COLUMN arr.element.Before TO After;
ALTER TABLE delta_m1.mapping_id RENAME COLUMN m.value.Before TO After;
ALTER TABLE delta_m1.mapping_id RENAME COLUMN part TO partition_renamed;
ALTER TABLE delta_m1.mapping_id DROP COLUMN old;
ALTER TABLE delta_m1.mapping_id ADD COLUMNS (old STRING);
ALTER TABLE delta_m1.mapping_id DROP COLUMN nested.Keep;
ALTER TABLE delta_m1.mapping_id ADD COLUMNS (nested.Keep STRING);
INSERT INTO delta_m1.mapping_id (renamed, nested, arr, m, partition_renamed, old) VALUES
  (2, named_struct('After', 20, 'Keep', 'new-nested'), array(named_struct('After', 200)),
   map('k', named_struct('After', 2000)), NULL, 'new-value');
SELECT * FROM delta_m1.mapping_id ORDER BY renamed;

CREATE OR REPLACE TABLE delta_m1.ntz (id BIGINT, ts TIMESTAMP_NTZ, part TIMESTAMP_NTZ)
USING delta PARTITIONED BY (part) LOCATION 's3a://delta/warehouse/delta_m1/ntz'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');
INSERT INTO delta_m1.ntz VALUES
  (1, TIMESTAMP_NTZ '2024-03-10 02:30:00.123456', TIMESTAMP_NTZ '2024-03-10 02:30:00.123456'),
  (2, TIMESTAMP_NTZ '1969-12-31 23:59:59.123456', NULL);
SELECT * FROM delta_m1.ntz ORDER BY id;

CREATE OR REPLACE TABLE delta_m1.widening (
  id BIGINT, tiny TINYINT, small SMALLINT, int_long INT, int_double INT, int_decimal INT,
  f FLOAT, dec DECIMAL(10,2), dt DATE, nested STRUCT<s:SMALLINT>, arr ARRAY<SMALLINT>, m MAP<INT,SMALLINT>)
USING delta LOCATION 's3a://delta/warehouse/delta_m1/widening'
TBLPROPERTIES ('delta.enableTypeWidening'='true', 'delta.enableDeletionVectors'='false',
  'delta.feature.timestampNtz'='supported');
INSERT INTO delta_m1.widening VALUES
  (1, -128, 32767, 2147483647, -2147483648, 123456789, 1.25, 1234.56, DATE '2024-02-29',
   named_struct('s', 7S), array(8S), map(9, 10S));
ALTER TABLE delta_m1.widening ALTER COLUMN tiny TYPE SMALLINT;
ALTER TABLE delta_m1.widening ALTER COLUMN small TYPE BIGINT;
ALTER TABLE delta_m1.widening ALTER COLUMN int_long TYPE BIGINT;
ALTER TABLE delta_m1.widening ALTER COLUMN int_double TYPE DOUBLE;
ALTER TABLE delta_m1.widening ALTER COLUMN int_decimal TYPE DECIMAL(20,4);
ALTER TABLE delta_m1.widening ALTER COLUMN f TYPE DOUBLE;
ALTER TABLE delta_m1.widening ALTER COLUMN dec TYPE DECIMAL(20,4);
ALTER TABLE delta_m1.widening ALTER COLUMN dt TYPE TIMESTAMP_NTZ;
ALTER TABLE delta_m1.widening ALTER COLUMN nested.s TYPE BIGINT;
ALTER TABLE delta_m1.widening ALTER COLUMN arr.element TYPE BIGINT;
ALTER TABLE delta_m1.widening ALTER COLUMN m.key TYPE BIGINT;
ALTER TABLE delta_m1.widening ALTER COLUMN m.value TYPE BIGINT;
INSERT INTO delta_m1.widening VALUES
  (2, 128, 2147483648L, 2147483648L, 1.5, 123.4567, 2.5, 1.2345, TIMESTAMP_NTZ '2024-02-29 12:34:56.123456',
   named_struct('s', 2147483648L), array(2147483648L), map(2147483648L, 2147483648L));
SELECT * FROM delta_m1.widening ORDER BY id;

CREATE OR REPLACE TABLE delta_m1.dv (id BIGINT, value STRING, part INT)
USING delta PARTITIONED BY (part) LOCATION 's3a://delta/warehouse/delta_m1/dv'
TBLPROPERTIES ('delta.enableDeletionVectors'='true', 'delta.checkpointInterval'='2');
INSERT INTO delta_m1.dv SELECT id, concat('value-', id), cast(id % 2 AS INT) FROM range(0, 100);
DELETE FROM delta_m1.dv WHERE id % 7 = 0;
UPDATE delta_m1.dv SET value=concat('updated-', id) WHERE id % 11 = 0;
MERGE INTO delta_m1.dv t USING (SELECT 1L id, 'merged-1' value, 1 part) s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET t.value=s.value
WHEN NOT MATCHED THEN INSERT *;
SELECT * FROM delta_m1.dv ORDER BY id;
SELECT count(*) FROM delta_m1.dv;
