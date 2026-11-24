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
CALL paimon.sys.migrate_table(
 source_type => 'hive',
 table => 'regression_paimon.hive_origin_parquet',
 target_table => 'regression_paimon.hive_migrate_paimon_parquet',
 delete_origin => false,
 options => 'file.format=parquet'
);
INSERT INTO paimon.regression_paimon.hive_migrate_paimon_parquet VALUES (4,'dva',CAST(40.00 AS DECIMAL(10,2)),'2026-04-03');
SELECT * FROM paimon.regression_paimon.hive_migrate_paimon_parquet ORDER BY id;
CALL paimon.sys.migrate_table(
 source_type => 'hive',
 table => 'regression_paimon.hive_origin_orc',
 target_table => 'regression_paimon.hive_migrate_paimon_orc',
 delete_origin => false,
 options => 'file.format=orc'
);
INSERT INTO paimon.regression_paimon.hive_migrate_paimon_orc VALUES (4,'dva',CAST(40.00 AS DECIMAL(10,2)),'2026-04-03');
SELECT * FROM paimon.regression_paimon.hive_migrate_paimon_orc ORDER BY id;
