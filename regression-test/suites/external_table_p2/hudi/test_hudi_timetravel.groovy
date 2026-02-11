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

suite("test_hudi_timetravel", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_timetravel"
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

    // Function to get commit timestamps dynamically from hudi_meta table function
    def getCommitTimestamps = { table_name ->
        def result = sql """ 
            SELECT timestamp 
            FROM hudi_meta("table"="${catalog_name}.regression_hudi.${table_name}", "query_type" = "timeline")
            WHERE action = 'commit' OR action = 'deltacommit'
            ORDER BY timestamp
        """
        return result.collect { it[0] }
    }

    def test_hudi_timetravel_querys = { table_name, timestamps ->
        timestamps.eachWithIndex { timestamp, index ->
            def query_name = "qt_timetravel${index + 1}"
            "${query_name}" """ select count(user_id) from ${table_name} for time as of "${timestamp}"; """
        }
    }

    // Get commit timestamps dynamically for each table
    def timestamps_cow_non_partition = getCommitTimestamps("user_activity_log_cow_non_partition")
    def timestamps_cow_partition = getCommitTimestamps("user_activity_log_cow_partition")
    def timestamps_mor_non_partition = getCommitTimestamps("user_activity_log_mor_non_partition")
    def timestamps_mor_partition = getCommitTimestamps("user_activity_log_mor_partition")

    sql """set force_jni_scanner=true;"""
    test_hudi_timetravel_querys("user_activity_log_cow_non_partition", timestamps_cow_non_partition)
    test_hudi_timetravel_querys("user_activity_log_cow_partition", timestamps_cow_partition)
    test_hudi_timetravel_querys("user_activity_log_mor_non_partition", timestamps_mor_non_partition)
    test_hudi_timetravel_querys("user_activity_log_mor_partition", timestamps_mor_partition)

    sql """set force_jni_scanner=false;"""
    test_hudi_timetravel_querys("user_activity_log_cow_non_partition", timestamps_cow_non_partition)
    test_hudi_timetravel_querys("user_activity_log_cow_partition", timestamps_cow_partition)
    test_hudi_timetravel_querys("user_activity_log_mor_non_partition", timestamps_mor_non_partition)
    test_hudi_timetravel_querys("user_activity_log_mor_partition", timestamps_mor_partition)

    sql """drop catalog if exists ${catalog_name};"""
}
