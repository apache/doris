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

suite("test_hms_partitions_tvf","p0,external,tvf,external_docker") {
    String suiteName = "test_hms_partitions_tvf"
    String tableName = "mtmv_base1"
    String dbName = "default"
    String enabled = context.config.otherConfigs.get("enableHiveTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("diable Hive test.")
            return;
        }

        for (String hivePrefix : ["hive3"]) {
            try {
                String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
                String catalogName = "${suiteName}_${hivePrefix}_catalog"
                String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

                sql """drop catalog if exists ${catalogName}"""
                sql """create catalog if not exists ${catalogName} properties (
                    "type"="hms",
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
                );"""
                order_qt_desc "desc function partitions('catalog'='${catalogName}','database'='${dbName}','table'='${tableName}');"
                order_qt_partitions "select * from partitions('catalog'='${catalogName}','database'='${dbName}','table'='${tableName}');"

                sql """drop catalog if exists ${catalogName}"""
            } finally {
            }
        }
}
