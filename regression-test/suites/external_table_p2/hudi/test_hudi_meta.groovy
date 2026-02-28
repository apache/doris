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

suite("test_hudi_meta", "p2,external,hudi") {
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
    
    // Query timeline and verify structure (action, state) without relying on specific timestamps
    // For user_activity_log_cow_non_partition: expect 5 commits (we changed from 10 to 5 commits)
    qt_hudi_meta1 """ 
        SELECT action, state 
        FROM hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_cow_non_partition", "query_type" = "timeline")
        ORDER BY timestamp;
    """
    
    // For user_activity_log_mor_non_partition: expect 5 deltacommits
    qt_hudi_meta2 """ 
        SELECT action, state 
        FROM hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_mor_non_partition", "query_type" = "timeline")
        ORDER BY timestamp;
    """
    
    // For user_activity_log_cow_partition: expect 5 commits
    qt_hudi_meta3 """ 
        SELECT action, state 
        FROM hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_cow_partition", "query_type" = "timeline")
        ORDER BY timestamp;
    """
    
    // Same table as hudi_meta3, should have same result
    qt_hudi_meta4 """ 
        SELECT action, state 
        FROM hudi_meta("table"="${catalog_name}.regression_hudi.user_activity_log_cow_partition", "query_type" = "timeline")
        ORDER BY timestamp;
    """
    
    // For timetravel_cow: expect 1 commit
    qt_hudi_meta5 """ 
        SELECT action, state 
        FROM hudi_meta("table"="${catalog_name}.regression_hudi.timetravel_cow", "query_type" = "timeline")
        ORDER BY timestamp;
    """
    
    // For timetravel_mor: expect 1 deltacommit
    qt_hudi_meta6 """ 
        SELECT action, state 
        FROM hudi_meta("table"="${catalog_name}.regression_hudi.timetravel_mor", "query_type" = "timeline")
        ORDER BY timestamp;
    """

    sql """drop catalog if exists ${catalog_name};"""
}

