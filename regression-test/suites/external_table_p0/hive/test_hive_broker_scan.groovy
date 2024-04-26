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

suite("test_hive_broker_scan", "p0,external,hive,external_docker,external_docker_hive,external_docker_broker") {

    def q01 = {
        qt_q01 """
        select * from test_different_column_orders_parquet
    """
    }

    def q02 = {
        qt_q02 """
        select count(*) from student;
    """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_hdfs_broker_catalog"
            String broker_name = "hdfs"

            sql """drop catalog if exists ${catalog_name}"""
            // create HMS catalog with broker binding
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris'='thrift://${externalEnvIp}:${hms_port}',
                'broker.name'='${broker_name}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            q01()
            q02()

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
