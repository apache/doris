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

import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

suite("test_query_limit", "external_docker,hive,external_docker_hive,p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    // Check backend configuration prerequisites
    // Note: This test case assumes a single backend scenario. Testing with single backend is logically equivalent 
    // to testing with multiple backends having identical configurations, but simpler in logic.
    def enableFileCacheResult = sql """show backend config like 'enable_file_cache';"""
    logger.info("enable_file_cache configuration: " + enableFileCacheResult)
    
    if (enableFileCacheResult.size() == 0 || !enableFileCacheResult[0][3].equalsIgnoreCase("true")) {
        logger.info(ENABLE_FILE_CACHE_CHECK_FAILED_MSG)
        assertTrue(false, ENABLE_FILE_CACHE_CHECK_FAILED_MSG)
    }
    
    def fileCachePathResult = sql """show backend config like 'file_cache_path';"""
    logger.info("file_cache_path configuration: " + fileCachePathResult)
    
    if (fileCachePathResult.size() == 0 || fileCachePathResult[0][3] == null || fileCachePathResult[0][3].trim().isEmpty()) {
        logger.info(FILE_CACHE_PATH_CHECK_FAILED_MSG)
        assertTrue(false, FILE_CACHE_PATH_CHECK_FAILED_MSG)
    }

    String catalog_name = "test_query_limit"
    String ex_db_name = "tpch1_parquet"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

    sql """set enable_file_cache=true"""
    sql """drop catalog if exists ${catalog_name} """

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
        'hadoop.username' = 'hive'
    );"""

    sql """switch ${catalog_name}"""

    sql """SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) AS sum_qty,
                SUM(l_extendedprice) AS sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                AVG(l_quantity) AS avg_qty,
                AVG(l_extendedprice) AS avg_price,
                AVG(l_discount) AS avg_disc,
                COUNT(*) AS count_order
            FROM
                ${catalog_name}.${ex_db_name}.lineitem
            WHERE
                l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus;"""
    // brpc metrics will be updated at most 20 seconds
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until{
        def result = sql """select METRIC_VALUE from information_schema.file_cache_statistics where METRIC_NAME like "%hits_ratio%" order by METRIC_VALUE limit 1;"""
        logger.info("result " + result)
        if (result.size() == 0) {
            return false;
        }
        if (Double.valueOf(result[0][0]) > 0) {
            return true;
        }
        return false;
    }
}

