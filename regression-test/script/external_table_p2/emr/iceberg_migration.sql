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
CALL spark_catalog.system.migrate(table => 'regression.hive_migrate_parquet', properties => map('write.format.default','parquet'));
INSERT INTO regression.hive_migrate_parquet VALUES (
  3, 'c', array(6,7,8,9), map('e',50),
  named_struct('name','Cindy','age',28),
  array(named_struct('city','Guangzhou','zip',510000)),
  map('k2',array(7,8,9))
);
ALTER TABLE regression.hive_migrate_parquet RENAME COLUMN name TO new_name;
ALTER TABLE regression.hive_migrate_parquet RENAME COLUMN struct_col.name TO new_name;
ALTER TABLE regression.hive_migrate_parquet RENAME COLUMN struct_col TO new_struct_col;
INSERT INTO regression.hive_migrate_parquet VALUES (
  4, 'd', array(10), map('x',7,'z',10),
  named_struct('new_name','David','age',35),
  array(named_struct('city','Chengdu','zip',610000),named_struct('city','Chengdu2','zip',610002)),
  map('kkk',array(71,81,91),'dddd',array(70,80,90))
);
SELECT * FROM regression.hive_migrate_parquet ORDER BY id;
CALL spark_catalog.system.migrate(table => 'regression_iceberg.hive_migrate_partition_parquet', properties => map('write.format.default','parquet'));
INSERT INTO regression_iceberg.hive_migrate_partition_parquet VALUES (4,'d','2024-01-02');
ALTER TABLE regression_iceberg.hive_migrate_partition_parquet RENAME COLUMN name TO new_name;
INSERT INTO regression_iceberg.hive_migrate_partition_parquet VALUES (5,'e','2024-01-03');
SELECT * FROM regression_iceberg.hive_migrate_partition_parquet ORDER BY id;
CALL spark_catalog.system.migrate(table => 'regression.hive_migrate_orc', properties => map('write.format.default','orc'));
INSERT INTO regression.hive_migrate_orc VALUES (
  3, 'c', array(6,7,8,9), map('e',50),
  named_struct('name','Cindy','age',28),
  array(named_struct('city','Guangzhou','zip',510000)),
  map('k2',array(7,8,9))
);
ALTER TABLE regression.hive_migrate_orc RENAME COLUMN name TO new_name;
ALTER TABLE regression.hive_migrate_orc RENAME COLUMN struct_col.name TO new_name;
ALTER TABLE regression.hive_migrate_orc RENAME COLUMN struct_col TO new_struct_col;
INSERT INTO regression.hive_migrate_orc VALUES (
  4, 'd', array(10), map('x',7,'z',10),
  named_struct('new_name','David','age',35),
  array(named_struct('city','Chengdu','zip',610000),named_struct('city','Chengdu2','zip',610002)),
  map('kkk',array(71,81,91),'dddd',array(70,80,90))
);
SELECT * FROM regression.hive_migrate_orc ORDER BY id;
CALL spark_catalog.system.migrate(table => 'regression_iceberg.hive_migrate_partition_orc', properties => map('write.format.default','orc'));
INSERT INTO regression_iceberg.hive_migrate_partition_orc VALUES (4,'d','2024-01-02');
ALTER TABLE regression_iceberg.hive_migrate_partition_orc RENAME COLUMN name TO new_name;
INSERT INTO regression_iceberg.hive_migrate_partition_orc VALUES (5,'e','2024-01-03');
SELECT * FROM regression_iceberg.hive_migrate_partition_orc ORDER BY id;
