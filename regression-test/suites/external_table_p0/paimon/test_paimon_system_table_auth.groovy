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

suite("test_paimon_system_table_auth", "p0,external,doris,external_docker,external_docker_doris,system_table") {

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalog_name = "test_paimon_systable_auth"
    try {
        String db_name = "flink_paimon"
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='paimon',
                'warehouse' = 's3://warehouse/wh/',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use ${db_name};"""
        logger.info("use " + db_name)

        // Use fixed table for stable test results
        logger.info("Testing permission checks for Paimon system tables")
        String tableName = "ts_scale_orc"
        List<List<Object>> paimonTableList = sql """ show tables; """
        boolean targetTableExists = paimonTableList.any { row ->
            row.size() > 0 && row[0].toString().equals(tableName)
        }
        assertTrue(targetTableExists, "Target table '${tableName}' not found in database '${db_name}'")
        logger.info("Using table: " + tableName)

        // Create test user without table permission
        String user = "test_paimon_systable_auth_user"
        String pwd = 'C123_567p'
        try_sql("DROP USER '${user}'@'%'")
        sql """CREATE USER '${user}'@'%' IDENTIFIED BY '${pwd}'"""

        if (isCloudMode()) {
            def clusters = sql " SHOW CLUSTERS; "
            assertTrue(!clusters.isEmpty())
            def validCluster = clusters[0][0]
            sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${user}'@'%'""";
        }

        sql """create database if not exists internal.regression_test"""
        sql """GRANT SELECT_PRIV ON internal.regression_test.* TO '${user}'@'%'""" 

        // Test that user without table permission cannot query system tables
        connect(user, "${pwd}", context.config.jdbcUrl) {
            // Test snapshots system table via direct access
            test {
                  sql """
                     select * from ${catalog_name}.${db_name}.${tableName}\$snapshots
                  """
                  exception "denied"
            }
            // Test files system table via direct access
            test {
                  sql """
                     select * from ${catalog_name}.${db_name}.${tableName}\$files
                  """
                  exception "denied"
            }
            // Test schemas system table via direct access
            test {
                  sql """
                     select * from ${catalog_name}.${db_name}.${tableName}\$schemas
                  """
                  exception "denied"
            }
            // Test partitions system table via direct access
            test {
                  sql """
                     select * from ${catalog_name}.${db_name}.${tableName}\$partitions
                  """
                  exception "denied"
            }
        }

        // Grant permission and verify user can query system tables
        sql """GRANT SELECT_PRIV ON ${catalog_name}.${db_name}.${tableName} TO '${user}'@'%'"""
        connect(user, "${pwd}", context.config.jdbcUrl) {
            // Test system tables with permission
            sql """select * from ${catalog_name}.${db_name}.${tableName}\$snapshots"""
            sql """select * from ${catalog_name}.${db_name}.${tableName}\$files"""
            sql """select * from ${catalog_name}.${db_name}.${tableName}\$schemas"""
            sql """select * from ${catalog_name}.${db_name}.${tableName}\$partitions"""
        }
        try_sql("DROP USER '${user}'@'%'")

    } catch (Exception e) {
        logger.error("Paimon system table auth test failed: " + e.getMessage())
        throw e
    } finally {
        // clean resource
        try {
            sql """drop catalog if exists ${catalog_name}"""
        } catch (Exception e) {
            logger.warn("Failed to cleanup catalog: " + e.getMessage())
        }
    }
}
