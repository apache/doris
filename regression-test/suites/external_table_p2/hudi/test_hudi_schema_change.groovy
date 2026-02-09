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

suite("test_hudi_schema_change", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_schema_change"
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
    
    def hudi_sc_tbs = ["hudi_sc_orc_cow","hudi_sc_parquet_cow"]

    for (String hudi_sc_tb : hudi_sc_tbs) {
        qt_hudi_0  """ SELECT id, name, age, city, score FROM ${hudi_sc_tb} ORDER BY id; """
        // TODO: Uncomment these test cases after RENAME COLUMN feature is implemented in Hudi
        // The following queries use 'full_name' and 'location' columns which require RENAME COLUMN operation
        // Currently RENAME COLUMN is disabled in 07_create_schema_change_tables.sql, so these tests are commented out
        // qt_hudi_1 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score > 90 ORDER BY id; """
        // qt_hudi_2 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score < 90 ORDER BY id; """    
        // qt_hudi_3 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score = 90 ORDER BY id; """    
        // qt_hudi_4 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score IS NULL ORDER BY id; """    
        // qt_hudi_5 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE location = 'New York' ORDER BY id; """
        // qt_hudi_6 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE location IS NULL ORDER BY id; """
        // qt_hudi_7 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score > 85 AND location = 'San Francisco' ORDER BY id; """
        // qt_hudi_8 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE score < 100 OR location = 'Austin' ORDER BY id; """
        // qt_hudi_9 """ SELECT id, full_name FROM ${hudi_sc_tb} WHERE full_name LIKE 'A%' ORDER BY id; """
        // qt_hudi_10 """ SELECT id, score, full_name, location FROM ${hudi_sc_tb} WHERE id BETWEEN 3 AND 7 ORDER BY id; """
        // qt_hudi_11 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE age > 20 ORDER BY id; """
        // qt_hudi_12 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE age IS NULL ORDER BY id; """
        // qt_hudi_13 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE score > 100 AND age IS NOT NULL ORDER BY id; """
        // qt_hudi_14 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE location = 'cn' ORDER BY id; """
        // qt_hudi_15 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE full_name = 'QQ' AND age > 20 ORDER BY id; """
        // qt_hudi_16 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE score < 100 OR age < 25 ORDER BY id; """
        // qt_hudi_17 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE age BETWEEN 20 AND 30 ORDER BY id; """
        // qt_hudi_18 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE location IS NULL AND age IS NULL ORDER BY id; """
        // qt_hudi_19 """ SELECT id, full_name, age FROM ${hudi_sc_tb} WHERE full_name LIKE 'Q%' AND age IS NOT NULL ORDER BY id; """    
        // qt_hudi_20 """ SELECT id, score, full_name, location, age FROM ${hudi_sc_tb} WHERE id > 5 AND age IS NULL ORDER BY id; """
    }

    sql """drop catalog if exists ${catalog_name};"""
}

