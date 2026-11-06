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


CREATE OR REPLACE TABLE delta_m1.log_compaction (id BIGINT, value BIGINT)
USING delta LOCATION 's3a://delta/warehouse/delta_m1/log_compaction'
TBLPROPERTIES ('delta.enableDeletionVectors'='false', 'delta.checkpointInterval'='10000');
INSERT INTO delta_m1.log_compaction SELECT id, id FROM range(0, 10);
UPDATE delta_m1.log_compaction SET value=value+100 WHERE id<3;
DELETE FROM delta_m1.log_compaction WHERE id%2=0;
INSERT INTO delta_m1.log_compaction VALUES (20, 200), (21, 210), (22, 220);
SELECT * FROM delta_m1.log_compaction ORDER BY id;
