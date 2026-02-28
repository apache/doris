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

suite("test_hudi_full_schema_change", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_full_schema_change"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hudiHmsPort = context.config.otherConfigs.get("hudiHmsPort")
    String hudiMinioPort = context.config.otherConfigs.get("hudiMinioPort")
    String hudiMinioAccessKey = context.config.otherConfigs.get("hudiMinioAccessKey")
    String hudiMinioSecretKey = context.config.otherConfigs.get("hudiMinioSecretKey")
    
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hudiHmsPort}',
            's3.endpoint' = 'http://${externalEnvIp}:${hudiMinioPort}',
            's3.access_key' = '${hudiMinioAccessKey}',
            's3.secret_key' = '${hudiMinioSecretKey}',
            's3.region' = 'us-east-1',
            'use_path_style' = 'true'
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ use regression_hudi;""" 
    sql """ set enable_fallback_to_original_planner=false """
    sql """set force_jni_scanner = false;"""
    

    def tables  = ["hudi_full_schema_change_parquet","hudi_full_schema_change_orc"]


    for (String table: tables) {
        order_qt_all """ select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} ORDER BY id"""

        order_qt_country_usa """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'country') = 'USA' ORDER BY id"""
        order_qt_country_usa_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') AS full_name, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'country') = 'USA' ORDER BY id"""

        order_qt_city_new """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'city') LIKE 'New%' ORDER BY id"""
        order_qt_city_new_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') AS age, STRUCT_ELEMENT(array_column[1], 'item') AS first_item FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'city') LIKE 'New%' ORDER BY id"""

        order_qt_age_over_30 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') > 30 ORDER BY id"""
        order_qt_age_over_30_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(array_column[2], 'category') AS second_category FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') > 30 ORDER BY id"""

        order_qt_age_under_25 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') < 25 ORDER BY id"""
        order_qt_age_under_25_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, MAP_KEYS(new_map_column)[1] AS map_key FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') < 25 ORDER BY id"""

        order_qt_name_alice """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') = 'Alice' ORDER BY id"""
        order_qt_name_alice_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') = 'Alice' ORDER BY id"""

        order_qt_name_j """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') like 'J%' ORDER BY id"""
        order_qt_name_j_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'gender') AS gender FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') LIKE 'J%' ORDER BY id"""

        order_qt_map_person5 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(new_map_column), 'person5') ORDER BY id"""
        order_qt_map_person5_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') AS age FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(new_map_column), 'person5') ORDER BY id"""

        order_qt_array_size_2 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE ARRAY_SIZE(array_column) = 2 ORDER BY id"""
        order_qt_array_size_2_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') AS b_cc FROM ${table} WHERE ARRAY_SIZE(array_column) = 2 ORDER BY id"""

        order_qt_quantity_not_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NOT NULL ORDER BY id"""
        order_qt_quantity_not_null_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') AS full_name FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NOT NULL ORDER BY id"""

        order_qt_quantity_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NULL ORDER BY id"""
        order_qt_quantity_null_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NULL ORDER BY id"""

        order_qt_struct2_not_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE struct_column2 IS NOT NULL ORDER BY id"""
        order_qt_struct2_not_null_cols """select id, STRUCT_ELEMENT(struct_column2, 'c') AS c_value, STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') AS new_aa FROM ${table} WHERE struct_column2 IS NOT NULL ORDER BY id"""

        order_qt_struct2_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE struct_column2 IS NULL ORDER BY id"""
        order_qt_struct2_null_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city FROM ${table} WHERE struct_column2 IS NULL ORDER BY id"""

        order_qt_cc_nested """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') like 'NestedC%' ORDER BY id"""
        order_qt_cc_nested_cols """select id, STRUCT_ELEMENT(struct_column2, 'c') AS c_value FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') LIKE 'NestedC%' ORDER BY id"""

        order_qt_c_over_20 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(struct_column2, 'c') > 20 ORDER BY id"""
        order_qt_c_over_20_cols """select id, STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') AS b_cc FROM ${table} WHERE STRUCT_ELEMENT(struct_column2, 'c') > 20 ORDER BY id"""

        order_qt_new_aa_50 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') = 50 ORDER BY id"""
        order_qt_new_aa_50_cols """select id, STRUCT_ELEMENT(struct_column2, 'c') AS c_value FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') = 50 ORDER BY id"""

        order_qt_gender_female """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'gender') = 'Female' ORDER BY id"""
        order_qt_gender_female_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'gender') = 'Female' ORDER BY id"""

        order_qt_category_fruit """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Fruit' ORDER BY id"""
        order_qt_category_fruit_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') AS full_name FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Fruit' ORDER BY id"""

        order_qt_category_vegetable """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Vegetable' ORDER BY id"""
        order_qt_category_vegetable_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') AS age FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Vegetable' ORDER BY id"""
    }

    sql """drop catalog if exists ${catalog_name};"""
}

