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

suite("test_paimon_full_schema_change", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String catalog_name = "test_paimon_full_schema_change"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        
        String table_name = "ts_scale_orc"

        sql """drop catalog if exists ${catalog_name}"""

        sql """
            CREATE CATALOG ${catalog_name} PROPERTIES (
                    'type' = 'paimon',
                    'warehouse' = 's3://warehouse/wh',
                    's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                    's3.access_key' = 'admin',
                    's3.secret_key' = 'password',
                    's3.path.style.access' = 'true'
            );
        """
        sql """switch `${catalog_name}`"""
        sql """show databases; """
        sql """use `test_paimon_schema_change`;""" 


        def tables = ["paimon_full_schema_change_parquet","paimon_full_schema_change_orc"]

        for (String table: tables) {
            qt_all """ select * FROM ${table} ORDER BY id"""

            qt_location_seattle """select * FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'location') = 'Seattle' ORDER BY id"""
            qt_location_seattle_cols """select id, STRUCT_ELEMENT(struct_column, 'population') AS population, STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'full_name') AS full_name, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'location') = 'Seattle' ORDER BY id"""

            qt_location_s """select * FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'location') LIKE 'S%' ORDER BY id"""
            qt_location_s_cols """select id, STRUCT_ELEMENT(struct_column, 'population') AS population, STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'age') AS age, STRUCT_ELEMENT(array_column[1], 'new_product') AS first_product FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'location') LIKE 'S%' ORDER BY id"""

            qt_age_over_30 """select * FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'age') > 30 ORDER BY id"""
            qt_age_over_30_cols """select id, STRUCT_ELEMENT(struct_column, 'location') AS location, STRUCT_ELEMENT(array_column[2], 'price') AS second_price FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'age') > 30 ORDER BY id"""

            qt_name_alice """select * FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'full_name') = 'Alice' ORDER BY id"""
            qt_name_alice_cols """select id, STRUCT_ELEMENT(struct_column, 'location') AS location, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'full_name') = 'Alice' ORDER BY id"""

            qt_map_person5 """select * FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(map_column), 'person5') ORDER BY id"""
            qt_map_person5_cols """select id, STRUCT_ELEMENT(struct_column, 'population') AS population, STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'address') AS address FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(map_column), 'person5') ORDER BY id"""

            qt_array_size_2 """select * FROM ${table} WHERE ARRAY_SIZE(array_column) = 2 ORDER BY id"""
            qt_array_size_2_cols """select id, STRUCT_ELEMENT(struct_column, 'location') AS location, STRUCT_ELEMENT(array_column[1], 'new_product') AS first_product FROM ${table} WHERE ARRAY_SIZE(array_column) = 2 ORDER BY id"""

            qt_struct2_not_null """select * FROM ${table} WHERE struct_column2 IS NOT NULL ORDER BY id"""
            qt_struct2_not_null_cols """select id, STRUCT_ELEMENT(struct_column2, 'b') AS b_value, STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'c'), 'cc') AS c_cc FROM ${table} WHERE struct_column2 IS NOT NULL ORDER BY id"""

            qt_new_aa_1001 """select * FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') = 1001 ORDER BY id"""
            qt_new_aa_1001_cols """select id, STRUCT_ELEMENT(struct_column2, 'b') AS b_value, MAP_KEYS(map_column)[1] AS map_key FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') = 1001 ORDER BY id"""

            qt_population_over_800k """select * FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'population') > 800000 ORDER BY id"""
            qt_population_over_800k_cols """select id, STRUCT_ELEMENT(struct_column, 'location') AS location, STRUCT_ELEMENT(MAP_VALUES(map_column)[1], 'age') AS age FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'population') > 800000 ORDER BY id"""

        }
    }
}


