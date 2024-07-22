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

suite("test_hive_serde_prop", "external_docker,hive,external_docker_hive,p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String catalog_name = "test_${hivePrefix}_serde_prop"
        String ex_db_name = "`stats_test`"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")

        sql """drop catalog if exists ${catalog_name} """

        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'hadoop.username' = 'hive'
            );"""

		qt_1 """select * from ${catalog_name}.${ex_db_name}.employee_gz order by name;"""


        qt_2 """select * from ${catalog_name}.regression.serde_test1 order by id;"""
        qt_3 """select * from ${catalog_name}.regression.serde_test2 order by id;"""
        qt_4 """select * from ${catalog_name}.regression.serde_test3 order by id;"""
        qt_5 """select * from ${catalog_name}.regression.serde_test4 order by id;"""
        qt_6 """select * from ${catalog_name}.regression.serde_test5 order by id;"""
        qt_7 """select * from ${catalog_name}.regression.serde_test6 order by id;"""
    }
}

