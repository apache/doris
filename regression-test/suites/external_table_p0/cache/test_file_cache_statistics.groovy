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

suite("test_file_cache_statistics", "external_docker,hive,external_docker_hive,p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    String catalog_name = "test_file_cache_statistics"
    String ex_db_name = "`default`"
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

    order_qt_2 """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table order by l_orderkey limit 1;"""
    // brpc metrics will be updated at most 20 seconds
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until{
        result = sql """select METRIC_VALUE from information_schema.file_cache_statistics where METRIC_NAME like "%hits_ratio%" order by METRIC_VALUE limit 1;"""
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

