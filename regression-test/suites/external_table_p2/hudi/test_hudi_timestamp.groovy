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

suite("test_hudi_timestamp", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_timestamp"
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

    def test_timestamp_different_timezones = {
        sql """set time_zone = 'America/Los_Angeles';"""
        qt_timestamp1 """ select id, name, event_time from hudi_table_with_timestamp order by id; """
        sql """set time_zone = 'Asia/Shanghai';"""
        qt_timestamp2 """ select id, name, event_time from hudi_table_with_timestamp order by id; """
        sql """set time_zone = 'UTC';"""
        qt_timestamp3 """ select id, name, event_time from hudi_table_with_timestamp order by id; """
    }

    // test native reader
    test_timestamp_different_timezones()
    sql """ set force_jni_scanner = true; """
    // test jni reader
    test_timestamp_different_timezones()
    sql """ set force_jni_scanner = false; """


    sql """drop catalog if exists ${catalog_name};"""
}

// DROP TABLE IF EXISTS hudi_table_with_timestamp;

// -- create table
// CREATE TABLE hudi_table_with_timestamp (
//   id STRING,
//   name STRING,
//   event_time TIMESTAMP
// ) USING HUDI
// OPTIONS (
//   type = 'cow',
//   primaryKey = 'id',
//   preCombineField = 'event_time'
// );

// SET TIME ZONE 'America/Los_Angeles';

// INSERT OVERWRITE hudi_table_with_timestamp VALUES
// ('1', 'Alice', timestamp('2024-10-25 08:00:00')),
// ('2', 'Bob', timestamp('2024-10-25 09:30:00')),
// ('3', 'Charlie', timestamp('2024-10-25 11:00:00'));
