// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_hudi_schema_change", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_schema_change"
    String props = context.config.otherConfigs.get("hudiEmrCatalog")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            ${props}
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ use regression_hudi;""" 
    sql """ set enable_fallback_to_original_planner=false """
    sql """set force_jni_scanner = false;"""
    
    def hudi_sc_tbs = ["hudi_sc_orc_cow","hudi_sc_parquet_cow"]

    for (String hudi_sc_tb : hudi_sc_tbs) {
        qt_hudi_0  """ SELECT * FROM ${hudi_sc_tb} ORDER BY id; """
        qt_hudi_1 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score > 90 ORDER BY id; """
        qt_hudi_2 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score < 90 ORDER BY id; """    
        qt_hudi_3 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score = 90 ORDER BY id; """    
        qt_hudi_4 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score IS NULL ORDER BY id; """    
        qt_hudi_5 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE location = 'New York' ORDER BY id; """
        qt_hudi_6 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE location IS NULL ORDER BY id; """
        qt_hudi_7 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score > 85 AND location = 'San Francisco' ORDER BY id; """
        qt_hudi_8 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score < 100 OR location = 'Austin' ORDER BY id; """
        qt_hudi_9 """ SELECT id, full_name FROM ${hudi_sc_tb} WHERE full_name LIKE 'A%' ORDER BY id; """
        qt_hudi_10 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE id BETWEEN 3 AND 7 ORDER BY id; """
        qt_hudi_11 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE age > 20 ORDER BY id; """
        qt_hudi_12 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE age IS NULL ORDER BY id; """
        qt_hudi_13 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE score > 100 AND age IS NOT NULL ORDER BY id; """
        qt_hudi_14 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE location = 'cn' ORDER BY id; """
        qt_hudi_15 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE full_name = 'QQ' AND age > 20 ORDER BY id; """
        qt_hudi_16 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE score < 100 OR age < 25 ORDER BY id; """
        qt_hudi_17 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE age BETWEEN 20 AND 30 ORDER BY id; """
        qt_hudi_18 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE location IS NULL AND age IS NULL ORDER BY id; """
        qt_hudi_19 """ SELECT id, full_name, age FROM ${hudi_sc_tb} WHERE full_name LIKE 'Q%' AND age IS NOT NULL ORDER BY id; """    
        qt_hudi_20 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE id > 5 AND age IS NULL ORDER BY id; """

	
	}
	qt_orc_time_travel """ select * from hudi_sc_orc_cow FOR TIME AS OF "20250314162817433" order by id; """ //1-8
	qt_parquet_time_travel """ select * from hudi_sc_parquet_cow   FOR TIME AS OF "20250314163425482"  order by id; """//1-6

	qt_parquet_inc_1 """ SELECT * from hudi_sc_parquet_cow@incr('beginTime'='20250314163421827') order by id; """
	qt_parquet_inc_2 """ SELECT * from hudi_sc_parquet_cow@incr('beginTime'='20250314163421827','endTime'="20250314163434457") order by id; """

	qt_orc_inc_1 """ SELECT * from hudi_sc_orc_cow@incr('beginTime'='20250314162813019')  order by id; """
	qt_orc_inc_2 """ SELECT * from hudi_sc_orc_cow@incr('beginTime'='20250314162813019','endTime'='20250314162822624')  order by id; """


    sql """drop catalog if exists ${catalog_name};"""
}
/*

spark-sql \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes=false

set hoodie.schema.on.read.enable=true;
set hoodie.metadata.enable=false;
set hoodie.parquet.small.file.limit = 100;


CREATE TABLE hudi_sc_orc_cow (
  id int,
  name string,
  age int
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.base.file.format= 'orc'
);

desc hudi_sc_orc_cow;
select * from hudi_sc_orc_cow;

INSERT INTO hudi_sc_orc_cow VALUES (1, 'Alice', 25);
INSERT INTO hudi_sc_orc_cow VALUES (2, 'Bob', 30);

-- id name age city
ALTER TABLE hudi_sc_orc_cow ADD COLUMNS (city string);
INSERT INTO hudi_sc_orc_cow VALUES (3, 'Charlie', 28, 'New York');

-- id name city
ALTER TABLE hudi_sc_orc_cow DROP COLUMN age;
INSERT INTO hudi_sc_orc_cow VALUES (4, 'David', 'Los Angeles');

-- id full_name city
ALTER TABLE hudi_sc_orc_cow RENAME COLUMN name TO full_name;
INSERT INTO hudi_sc_orc_cow VALUES (5, 'Eve', 'Chicago');

-- id score full_name city
ALTER TABLE hudi_sc_orc_cow ADD COLUMNS (score float AFTER id);
INSERT INTO hudi_sc_orc_cow VALUES (6,85.5, 'Frank', 'San Francisco');

-- id city score full_name 
ALTER TABLE hudi_sc_orc_cow CHANGE COLUMN city city string AFTER id;
INSERT INTO hudi_sc_orc_cow  VALUES (7, 'Seattle', 90.0, 'Grace');

ALTER TABLE hudi_sc_orc_cow CHANGE COLUMN score score double;
INSERT INTO hudi_sc_orc_cow VALUES (8,  'Portland', 95.5 , 'Heidi');

-- id location score full_name
ALTER TABLE hudi_sc_orc_cow RENAME COLUMN city TO location;
INSERT INTO hudi_sc_orc_cow VALUES (9, 'Denver', 88.0, 'Ivan');

-- id  score full_name location
ALTER TABLE hudi_sc_orc_cow ALTER COLUMN location AFTER full_name;
INSERT INTO hudi_sc_orc_cow VALUES (10, 101.1,'Judy', 'Austin');


select id,score,full_name,location from hudi_sc_orc_cow order by id;
1       NULL    Alice   NULL
2       NULL    Bob     NULL
3       NULL    Charlie New York
4       NULL    David   Los Angeles
5       NULL    Eve     Chicago
6       85.5    Frank   San Francisco
7       90.0    Grace   Seattle
8       95.5    Heidi   Portland
9       88.0    Ivan    Denver
10      101.1   Judy    Austin

-- id  score full_name location age 
ALTER TABLE hudi_sc_orc_cow ADD COLUMN age int;
INSERT INTO hudi_sc_orc_cow VALUES (11, 222.2,'QQ', 'cn', 24);
*/

