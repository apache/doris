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

suite("test_hive_migrate_iceberg", "p2,external,iceberg,external_remote,external_remote_iceberg") {

    String enabled = context.config.otherConfigs.get("enableExternalIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test enableExternalIcebergTest = false")
        return
    }

    enabled = context.config.otherConfigs.get("enableExternalEmrTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test enableExternalEmrTest = false")
        return
    }
    String props = context.config.otherConfigs.get("emrCatalogCommonProp")
    String catalog_name = "test_hive_migrate_iceberg"
    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hms",
        'hive.version' = '3.1.3',
        ${props}
    );"""
    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """ use regression;"""

    for (String table : ["hive_migrate_orc", "hive_migrate_parquet"]) {
        // Basic query
        qt_all """ select * FROM ${table} ORDER BY id"""

        qt_some_1 """ select id, new_name, map_array_col FROM ${table} ORDER BY id"""
        qt_some_2 """ select id, arr_col, map_col, new_struct_col, arr_struct_col FROM ${table} ORDER BY id"""

        // Test array column (arr_col)
        qt_arr_col_size """select id, ARRAY_SIZE(arr_col) AS arr_size FROM ${table} ORDER BY id"""
        qt_arr_col_contains_1 """select * FROM ${table} WHERE ARRAY_CONTAINS(arr_col, 1) ORDER BY id"""
        qt_arr_col_size_3 """select * FROM ${table} WHERE ARRAY_SIZE(arr_col) = 3 ORDER BY id"""

        // Test map column (map_col)
        qt_map_col_a """select * FROM ${table} WHERE map_col['a'] = 10 ORDER BY id"""
        qt_map_col_keys """select id, MAP_KEYS(map_col) AS map_keys FROM ${table} ORDER BY id"""
        qt_map_col_contains_x """select * FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(map_col), 'x') ORDER BY id"""

        // Test struct column (new_struct_col)
        qt_struct_name_alice """select * FROM ${table} WHERE STRUCT_ELEMENT(new_struct_col, 'new_name') = 'Alice' ORDER BY id"""
        qt_struct_age_over_28 """select * FROM ${table} WHERE STRUCT_ELEMENT(new_struct_col, 'age') > 28 ORDER BY id"""
        qt_struct_name_like_c """select * FROM ${table} WHERE STRUCT_ELEMENT(new_struct_col, 'new_name') LIKE 'C%' ORDER BY id"""

        // Test array of struct column (arr_struct_col)
        qt_arr_struct_city_beijing """select * FROM ${table} WHERE STRUCT_ELEMENT(arr_struct_col[1], 'city') = 'Beijing' ORDER BY id"""
        qt_arr_struct_size_2 """select * FROM ${table} WHERE ARRAY_SIZE(arr_struct_col) = 2 ORDER BY id"""

        // Test map of array column (map_array_col)
        qt_map_array_k1 """select * FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(map_array_col), 'k1') ORDER BY id"""
        qt_map_array_contains_1 """select * FROM ${table} WHERE ARRAY_CONTAINS(MAP_VALUES(map_array_col)[1], 1) ORDER BY id"""

        // Complex nested query
        qt_complex_1 """select id, new_name, STRUCT_ELEMENT(new_struct_col, 'age') AS age, ARRAY_SIZE(arr_col) AS arr_size FROM ${table} WHERE STRUCT_ELEMENT(new_struct_col, 'age') > 28 ORDER BY id"""

    }
    sql """drop catalog if exists ${catalog_name}"""

}
/*

use regression;
CREATE TABLE hive_migrate_parquet (
    id INT,
    name string,
    arr_col ARRAY<INT>,
    map_col MAP<STRING, INT>,
    struct_col STRUCT<name:STRING, age:INT>,
    arr_struct_col ARRAY<STRUCT<city:STRING, zip:INT>>,
    map_array_col MAP<STRING, ARRAY<INT>>
) STORED AS PARQUET;

INSERT INTO TABLE hive_migrate_parquet VALUES (
  1,
  'a',
  array(1,2,3),
  map('a',10,'b',20),
  named_struct('name','Alice','age',25),
  array(named_struct('city','Beijing','zip',100000),
        named_struct('city','Shanghai','zip',200000)),
  map('x', array(1,2), 'y', array(3,4))
),(
  2,
  'b',
  array(4,5),
  map('c',30,'d',40),
  named_struct('name','Bob','age',30),
  array(named_struct('city','Shenzhen','zip',518000)),
  map('k1', array(5,6))
);

CALL iceberg_emr.system.migrate(
    schema_name => 'regression',
    table_name => 'hive_migrate_parquet');


INSERT INTO iceberg_emr.regression.hive_migrate_parquet VALUES 
(
    3,
    'c',
    ARRAY[6,7,8,9],
    MAP(ARRAY['e'], ARRAY[50]),
    ROW('Cindy', 28),
    ARRAY[
        ROW('Guangzhou', 510000)
    ],
    MAP(
        ARRAY['k2'],
        ARRAY[ARRAY[7,8,9]]
    )
);

alter table iceberg_emr.regression.hive_migrate_parquet rename column name to new_name;
alter table iceberg_emr.regression.hive_migrate_parquet rename column struct_col.name to new_name;
alter table iceberg_emr.regression.hive_migrate_parquet rename column struct_col to new_struct_col;

INSERT INTO iceberg_emr.regression.hive_migrate_parquet VALUES (
    4,
    'd',
    ARRAY[10],
    MAP(ARRAY['x','z'], ARRAY[7,10]),
    ROW('David', 35),
    ARRAY[ROW('Chengdu', 610000),ROW('Chengdu2', 610002)],
    MAP(ARRAY['kkk','dddd'], ARRAY[ARRAY[71,81,91],ARRAY[70,80,90]])
);
*/


