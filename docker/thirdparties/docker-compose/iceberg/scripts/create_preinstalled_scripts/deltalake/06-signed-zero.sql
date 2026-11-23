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

DROP TABLE IF EXISTS delta_m1.signed_zero;
CREATE TABLE delta_m1.signed_zero (id BIGINT, f FLOAT, d DOUBLE)
USING delta LOCATION 's3a://delta/warehouse/delta_m1/signed_zero'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');

-- Separate AddFiles give each signed zero its own min/max statistics.
INSERT OVERWRITE delta_m1.signed_zero VALUES (1, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE));
INSERT INTO delta_m1.signed_zero VALUES (2, CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE));
INSERT INTO delta_m1.signed_zero VALUES (3, -1, -1), (4, 1, 1), (5, NULL, NULL);

DROP TABLE IF EXISTS delta_m1.signed_zero_partition;
CREATE TABLE delta_m1.signed_zero_partition (id BIGINT, p FLOAT)
USING delta PARTITIONED BY (p) LOCATION 's3a://delta/warehouse/delta_m1/signed_zero_partition'
TBLPROPERTIES ('delta.enableDeletionVectors'='false');

-- Keep one file in each of the three physical partitions.
INSERT OVERWRITE delta_m1.signed_zero_partition VALUES (1, CAST('-0.0' AS FLOAT));
INSERT INTO delta_m1.signed_zero_partition VALUES (2, CAST('0.0' AS FLOAT));
INSERT INTO delta_m1.signed_zero_partition VALUES (3, CAST('1.0' AS FLOAT));
