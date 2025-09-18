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

suite("test_select_external_view_auth","p0,auth") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalogName = "${hivePrefix}_test_mtmv"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalogName}"""
            sql """create catalog if not exists ${catalogName} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""

            String suiteName = "test_select_external_view_auth"
            String user = "${suiteName}_user"
            String pwd = 'C123_567p'
            String dbName = "`default`"
            String tableName = "sale_table"
            String viewName = "test_view1"

            try_sql("drop user ${user}")
            sql """create user '${user}' IDENTIFIED by '${pwd}'"""
            sql """grant select_priv on regression_test to ${user}"""

            //cloud-mode
            if (isCloudMode()) {
                def clusters = sql " SHOW CLUSTERS; "
                assertTrue(!clusters.isEmpty())
                def validCluster = clusters[0][0]
                sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
            }

            sql """grant select_priv on ${catalogName}.${dbName}.${tableName} to ${user}"""
            // table column
            connect(user, "${pwd}", context.config.jdbcUrl) {
                try {
                    sql "select * from ${catalogName}.${dbName}.${viewName}"
                } catch (Exception e) {
                    log.info(e.getMessage())
                    assertTrue(e.getMessage().contains("denied"))
                }
            }
            sql """revoke select_priv on ${catalogName}.${dbName}.${tableName} from ${user}"""
            sql """grant select_priv on ${catalogName}.${dbName}.${viewName} to ${user}"""
            connect(user, "${pwd}", context.config.jdbcUrl) {
                    sql "select * from ${catalogName}.${dbName}.${viewName}"
                }
            try_sql("drop user ${user}")
            sql """drop catalog if exists ${catalogName}"""
        } finally {
        }
    }
}
