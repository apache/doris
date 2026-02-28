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

suite("test_hudi_schema_evolution", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_schema_evolution"
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
    
    // Test with JNI scanner
    sql """set force_jni_scanner = true;"""
    
    // Test adding_simple_columns_table: schema evolution adding columns (id, name) -> (id, name, age, city)
    qt_jni_adding_simple_columns_table_all """ select id, name, age, city from adding_simple_columns_table order by id """
    qt_jni_adding_simple_columns_table_old_data """ select id, name, age, city from adding_simple_columns_table where id in ('1', '2', '3') order by id """
    qt_jni_adding_simple_columns_table_new_data """ select id, name, age, city from adding_simple_columns_table where id in ('4', '5', '6') order by id """
    
    // Test deleting_simple_columns_table: schema evolution dropping columns (id, name, age, city) -> (id, name)
    qt_jni_deleting_simple_columns_table_all """ select id, name from deleting_simple_columns_table order by id """
    qt_jni_deleting_simple_columns_table_old_data """ select id, name from deleting_simple_columns_table where id in ('1', '2', '3') order by id """
    qt_jni_deleting_simple_columns_table_new_data """ select id, name from deleting_simple_columns_table where id in ('4', '5', '6') order by id """
    
    // Test renaming_simple_columns_table: schema evolution renaming column name -> full_name
    // Note: Hudi doesn't support RENAME COLUMN, so this test is skipped
    // qt_jni_renaming_simple_columns_table_all """ select id, full_name from renaming_simple_columns_table order by id """
    // qt_jni_renaming_simple_columns_table_old_data """ select id, full_name from renaming_simple_columns_table where id in ('1', '2', '3') order by id """
    // qt_jni_renaming_simple_columns_table_new_data """ select id, full_name from renaming_simple_columns_table where id in ('4', '5', '6') order by id """
    
    // Test reordering_columns_table: schema evolution reordering columns
    qt_jni_reordering_columns_table_all """ select id, name, age from reordering_columns_table order by id """
    
    // Test adding_complex_columns_table: schema evolution adding email field to struct
    qt_jni_adding_complex_columns_table_all """ select id, name, info from adding_complex_columns_table order by id """
    qt_jni_adding_complex_columns_table_old_struct """ select id, name, info from adding_complex_columns_table where id in ('1', '2', '3') order by id """
    qt_jni_adding_complex_columns_table_new_struct """ select id, name, info from adding_complex_columns_table where id in ('4', '5', '6') order by id """
    
    // Test deleting_complex_columns_table: schema evolution dropping email field from struct
    qt_jni_deleting_complex_columns_table_all """ select id, name, info from deleting_complex_columns_table order by id """
    qt_jni_deleting_complex_columns_table_old_struct """ select id, name, info from deleting_complex_columns_table where id in ('1', '2', '3') order by id """
    qt_jni_deleting_complex_columns_table_new_struct """ select id, name, info from deleting_complex_columns_table where id in ('4', '5', '6') order by id """
    
    // Test renaming_complex_columns_table: schema evolution renaming location -> address in struct
    // Note: Hudi doesn't support renaming struct fields, so this test is skipped
    // qt_jni_renaming_complex_columns_table_all """ select * from renaming_complex_columns_table order by id """
    // qt_jni_renaming_complex_columns_table_old_struct """ select id, name, info from renaming_complex_columns_table where id in ('1', '2', '3') order by id """
    // qt_jni_renaming_complex_columns_table_new_struct """ select id, name, info from renaming_complex_columns_table where id in ('4', '5', '6') order by id """
    
    // Test with native scanner
    sql """set force_jni_scanner = false;"""
    
    // Test adding_simple_columns_table: schema evolution adding columns (id, name) -> (id, name, age, city)
    qt_native_adding_simple_columns_table_all """ select id, name, age, city from adding_simple_columns_table order by id """
    qt_native_adding_simple_columns_table_old_data """ select id, name, age, city from adding_simple_columns_table where id in ('1', '2', '3') order by id """
    qt_native_adding_simple_columns_table_new_data """ select id, name, age, city from adding_simple_columns_table where id in ('4', '5', '6') order by id """
    
    // Test deleting_simple_columns_table: schema evolution dropping columns (id, name, age, city) -> (id, name)
    qt_native_deleting_simple_columns_table_all """ select id, name from deleting_simple_columns_table order by id """
    qt_native_deleting_simple_columns_table_old_data """ select id, name from deleting_simple_columns_table where id in ('1', '2', '3') order by id """
    qt_native_deleting_simple_columns_table_new_data """ select id, name from deleting_simple_columns_table where id in ('4', '5', '6') order by id """
    
    // Test renaming_simple_columns_table: schema evolution renaming column name -> full_name
    // Note: Hudi doesn't support RENAME COLUMN, so this test is skipped
    // qt_native_renaming_simple_columns_table_all """ select id, full_name from renaming_simple_columns_table order by id """
    // qt_native_renaming_simple_columns_table_old_data """ select id, full_name from renaming_simple_columns_table where id in ('1', '2', '3') order by id """
    // qt_native_renaming_simple_columns_table_new_data """ select id, full_name from renaming_simple_columns_table where id in ('4', '5', '6') order by id """
    
    // Test reordering_columns_table: schema evolution reordering columns
    qt_native_reordering_columns_table_all """ select id, name, age from reordering_columns_table order by id """
    
    // Test adding_complex_columns_table: schema evolution adding email field to struct
    qt_native_adding_complex_columns_table_all """ select id, name, info from adding_complex_columns_table order by id """
    qt_native_adding_complex_columns_table_old_struct """ select id, name, info from adding_complex_columns_table where id in ('1', '2', '3') order by id """
    qt_native_adding_complex_columns_table_new_struct """ select id, name, info from adding_complex_columns_table where id in ('4', '5', '6') order by id """
    
    // Test deleting_complex_columns_table: schema evolution dropping email field from struct
    qt_native_deleting_complex_columns_table_all """ select id, name, info from deleting_complex_columns_table order by id """
    qt_native_deleting_complex_columns_table_old_struct """ select id, name, info from deleting_complex_columns_table where id in ('1', '2', '3') order by id """
    qt_native_deleting_complex_columns_table_new_struct """ select id, name, info from deleting_complex_columns_table where id in ('4', '5', '6') order by id """
    
    // Test renaming_complex_columns_table: schema evolution renaming location -> address in struct
    // Note: Hudi doesn't support renaming struct fields, so this test is skipped
    // qt_native_renaming_complex_columns_table_all """ select * from renaming_complex_columns_table order by id """
    // qt_native_renaming_complex_columns_table_old_struct """ select id, name, info from renaming_complex_columns_table where id in ('1', '2', '3') order by id """
    // qt_native_renaming_complex_columns_table_new_struct """ select id, name, info from renaming_complex_columns_table where id in ('4', '5', '6') order by id """

    sql """drop catalog if exists ${catalog_name};"""
}
