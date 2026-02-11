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

suite("test_hudi_snapshot", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_snapshot"
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

    def test_hudi_snapshot_querys = { table_name ->
        // Query all records ordered by event_time in descending order
        order_qt_q01 """SELECT user_id, event_time, action FROM ${table_name} ORDER BY event_time DESC LIMIT 10;"""

        // Query specific user's activity records
        order_qt_q02 """SELECT user_id, event_time, action FROM ${table_name} WHERE user_id = 1 ORDER BY event_time LIMIT 5;"""

        // Query events within a specific time range
        order_qt_q03 """SELECT user_id, event_time, action FROM ${table_name} WHERE event_time BETWEEN 1710000000000 AND 1710000003000 ORDER BY event_time LIMIT 10;"""

        // Query by action type
        order_qt_q04 """SELECT user_id, event_time, action FROM ${table_name} WHERE action = 'login' ORDER BY event_time LIMIT 5;"""

        // Count records by action
        order_qt_q05 """SELECT action, COUNT(*) AS action_count FROM ${table_name} GROUP BY action ORDER BY action_count DESC;"""

        // Query user_id and action
        order_qt_q06 """SELECT user_id, action FROM ${table_name} ORDER BY user_id LIMIT 5;"""
    }
    
    def test_hudi_snapshot_querys_partitioned = { table_name ->
        // Query all records ordered by event_time in descending order
        order_qt_q01 """SELECT user_id, event_time, action, dt FROM ${table_name} ORDER BY event_time DESC LIMIT 10;"""

        // Query specific user's activity records
        order_qt_q02 """SELECT user_id, event_time, action, dt FROM ${table_name} WHERE user_id = 1 ORDER BY event_time LIMIT 5;"""

        // Query events within a specific time range
        order_qt_q03 """SELECT user_id, event_time, action, dt FROM ${table_name} WHERE event_time BETWEEN 1710000000000 AND 1710000003000 ORDER BY event_time LIMIT 10;"""

        // Query by action type
        order_qt_q04 """SELECT user_id, event_time, action, dt FROM ${table_name} WHERE action = 'login' ORDER BY event_time LIMIT 5;"""

        // Count records by action
        order_qt_q05 """SELECT action, COUNT(*) AS action_count FROM ${table_name} GROUP BY action ORDER BY action_count DESC;"""

        // Query user_id and action
        order_qt_q06 """SELECT user_id, action FROM ${table_name} ORDER BY user_id LIMIT 5;"""

        // Query by partition column (dt)
        order_qt_q07 """SELECT user_id, event_time, action, dt FROM ${table_name} WHERE dt = '2024-03-01' ORDER BY event_time LIMIT 5;"""

        // Query user_id and partition column
        order_qt_q08 """SELECT user_id, dt FROM ${table_name} ORDER BY dt, user_id LIMIT 5;"""
    }

    sql """set force_jni_scanner=true;"""
    test_hudi_snapshot_querys("user_activity_log_mor_non_partition")
    test_hudi_snapshot_querys_partitioned("user_activity_log_mor_partition")
    test_hudi_snapshot_querys("user_activity_log_cow_non_partition")
    test_hudi_snapshot_querys_partitioned("user_activity_log_cow_partition")

    sql """set force_jni_scanner=false;"""
    test_hudi_snapshot_querys("user_activity_log_mor_non_partition")
    test_hudi_snapshot_querys_partitioned("user_activity_log_mor_partition")
    test_hudi_snapshot_querys("user_activity_log_cow_non_partition")
    test_hudi_snapshot_querys_partitioned("user_activity_log_cow_partition")

    sql """drop catalog if exists ${catalog_name};"""
}

