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

suite("test_be_metrics") {
    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

    String catalogName = "test_jdbc_catalog_for_jni_metrics"
    String dbName = "test_jni_metrics_db"
    String tblName = "test_jni_metrics_tbl"
    set_be_param("jdbc_connection_pool_cache_clear_time_sec", "28800")

    sql "drop catalog if exists ${catalogName}"

    sql """ CREATE CATALOG if not exists `${catalogName}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "enable_connection_pool" = "true",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""

    sql "create database if not exists ${dbName}"

    sql "use ${dbName}"

    sql "drop table if exists ${tblName}"

    sql  """
          CREATE TABLE ${tblName} (
            `id` INT NULL COMMENT "主键id",
            `name` string NULL COMMENT "名字"
            ) DISTRIBUTED BY HASH(id) BUCKETS 10
            PROPERTIES("replication_num" = "1");
    """

    sql """ insert into ${tblName} values (1, 'doris1')"""
    sql """ insert into ${tblName} values (2, 'doris2')"""
    sql """ insert into ${tblName} values (3, 'doris3')"""
    sql """ insert into ${tblName} values (4, 'doris4')"""
    sql """ insert into ${tblName} values (5, 'doris5')"""
    sql """ insert into ${tblName} values (6, 'doris6')"""

    sql "switch ${catalogName}"

    sql "use ${dbName}"

    sql """ select count(1) from ${tblName}"""

    Thread.sleep(20000)

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    boolean hasValue = false
    for (def backendId : backendId_to_backendHttpPort.keySet()) {
        def backendIP = backendId_to_backendIP.get(backendId)
        def backendHttpPort = backendId_to_backendHttpPort.get(backendId)
        def (is_exist, value) = get_be_metric(backendIP, backendHttpPort, "jdbc_scan_connection_percent")
        if (is_exist) {
            hasValue = true
            break
        }
    }
    assertTrue(hasValue)

    set_be_param("jdbc_connection_pool_cache_clear_time_sec", "3")

    sql """ select max(id) from ${tblName}"""

    Thread.sleep(30000)

    hasValue = false
    for (def backendId : backendId_to_backendHttpPort.keySet()) {
        def backendIP = backendId_to_backendIP.get(backendId)
        def backendHttpPort = backendId_to_backendHttpPort.get(backendId)
        def (is_exist, value)  = get_be_metric(backendIP, backendHttpPort, "jdbc_scan_connection_percent")
        if (is_exist) {
            hasValue = true
            break
        }
    }
    assertTrue(!hasValue)

    set_be_param("jdbc_connection_pool_cache_clear_time_sec", "28800")

    sql "drop table if exists ${tblName}"
    sql "drop database if exists ${dbName}"
    sql "drop catalog if exists ${catalogName}"
}
