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

DROP TABLE IF EXISTS delta_m1.skipping;
CREATE TABLE delta_m1.skipping (id BIGINT, s STRING, dec DECIMAL(20,4), f FLOAT, d DOUBLE)
USING delta LOCATION 's3a://delta/warehouse/delta_m1/skipping'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');
INSERT OVERWRITE delta_m1.skipping SELECT /*+ COALESCE(1) */
  id, concat('low-', id), cast(id AS DECIMAL(20,4)), cast(id AS FLOAT), cast(id AS DOUBLE) FROM range(0, 10);
INSERT INTO delta_m1.skipping SELECT /*+ COALESCE(1) */
  id, concat('high-', id), cast(id AS DECIMAL(20,4)), cast(id AS FLOAT), cast(id AS DOUBLE) FROM range(100, 110);
INSERT INTO delta_m1.skipping VALUES (NULL, NULL, NULL, NULL, NULL);
SELECT * FROM delta_m1.skipping ORDER BY id;
SELECT count(*) FROM delta_m1.skipping;
SELECT * FROM delta_m1.skipping WHERE id < 5 ORDER BY id;
SELECT * FROM delta_m1.skipping WHERE NOT (id <=> 1) ORDER BY id;
