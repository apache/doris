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

suite("test_paimon_table_stats", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
            String catalog_name = "ctl_test_paimon_table_stats"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type" = "paimon",
                "paimon.catalog.type"="filesystem",
                "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
            );"""

            def assert_stats = { table_name, cnt ->
                def retry = 0
                def act = ""
                sql """desc ${table_name}"""
                while (retry < 100) {
                    def result = sql """ show table stats ${table_name} """
                    act = result[0][2]
                    logger.info("current table row count is " + act)
                    if (!act.equals("-1")) {
                        break;
                    }
                    Thread.sleep(2000)
                    retry++
                }
                assertEquals(cnt, act)
            }

            // select
            sql """ switch ${catalog_name} """
            sql """ use db1 """
            assert_stats("all_table", "2")
            assert_stats("auto_bucket", "2")
            assert_stats("complex_all", "3")
            assert_stats("complex_tab", "3")
        } finally {
        }
    }
}

