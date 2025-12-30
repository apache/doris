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

suite("test_hudi_meta", "p0,external,hudi,external_docker,external_docker_hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_meta"
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
    
    qt_hudi_meta1 """ select * from hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_cow_non_partition", "query_type" = "timeline"); """
    qt_hudi_meta2 """ select * from hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_mor_non_partition", "query_type" = "timeline"); """
    qt_hudi_meta3 """ select * from hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_cow_partition", "query_type" = "timeline"); """
    qt_hudi_meta4 """ select * from hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_cow_partition", "query_type" = "timeline"); """
    qt_hudi_meta5 """ select * from hudi_meta("table"="${catalog_name}.regression_hudi.timetravel_cow", "query_type" = "timeline"); """
    qt_hudi_meta6 """ select * from hudi_meta("table"="${catalog_name}.regression_hudi.timetravel_mor", "query_type" = "timeline"); """

    sql """drop catalog if exists ${catalog_name};"""
}

