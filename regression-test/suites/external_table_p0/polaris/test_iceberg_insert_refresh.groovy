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

suite("test_iceberg_insert_refresh", "p0,external,iceberg,polaris,external_docker,external_docker_polaris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String polaris_port = context.config.otherConfigs.get("polaris_rest_uri_port")
        String minio_port = context.config.otherConfigs.get("polaris_minio_port")

        String iceberg_catalog_name = "test_iceberg_insert_refresh"
        sql """drop catalog if exists ${iceberg_catalog_name}"""
        sql """create catalog if not exists ${iceberg_catalog_name} properties (
            'type'='iceberg',
            'warehouse' = 'doris_test',
            'iceberg.catalog.type'='rest',
            'iceberg.rest.uri' = 'http://${externalEnvIp}:${polaris_port}/api/catalog',
            'iceberg.rest.security.type' = 'oauth2',
            'iceberg.rest.oauth2.credential' = 'root:secret123',
            'iceberg.rest.oauth2.server-uri' = 'http://${externalEnvIp}:${polaris_port}/api/catalog/v1/oauth/tokens',
            'iceberg.rest.oauth2.scope' = 'PRINCIPAL_ROLE:ALL',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
            's3.region' = 'us-east-1'
        )"""

        sql """switch ${iceberg_catalog_name}"""
        sql """create database if not exists db_iceberg_insert_refresh"""
        sql """use db_iceberg_insert_refresh"""
        sql """drop table if exists taxis"""
        sql """CREATE TABLE taxis
               (
                   vendor_id BIGINT,
                   trip_id BIGINT,
                   trip_distance FLOAT,
                   fare_amount DOUBLE,
                   store_and_fwd_flag STRING,
                   ts DATETIME
               )
               PARTITION BY LIST (vendor_id, DAY(ts)) ()
               PROPERTIES (
                   "compression-codec" = "zstd",
                   "write-format" = "parquet"
               );"""
        String insert_sql = """INSERT OVERWRITE  TABLE ${iceberg_catalog_name}.db_iceberg_insert_refresh.taxis
                               VALUES
                                (1, 1000371, 1.8, 15.32, 'N', '2024-01-01 9:15:23'),
                                (2, 1000372, 2.5, 22.15, 'N', '2024-01-02 12:10:11'),
                                (2, 1000373, 0.9, 9.01, 'N', '2024-01-01 3:25:15'),
                                (1, 1000374, 8.4, 42.13, 'Y', '2024-01-03 7:12:33');"""

        String refresh_sql = """REFRESH CATALOG ${iceberg_catalog_name};"""

        // Simple concurrent test: 10 inserts + refresh, each insert must complete within 1 minute

        def insertCount = 10
        def insertTimeoutMs = 60000L // 1 minute per insert

        def insertCompleted = false
        def insertException = null

        logger.info("Starting concurrent insert and refresh test")

        // Insert task: run 10 inserts, fail if any takes >1min
        def insertTask = {
            try {
                for (int i = 1; i <= insertCount; i++) {
                    def start = System.currentTimeMillis()
                    sql insert_sql
                    def duration = System.currentTimeMillis() - start

                    if (duration > insertTimeoutMs) {
                        throw new RuntimeException("Insert ${i} took ${duration}ms > ${insertTimeoutMs}ms")
                    }

                    logger.info("Insert ${i} completed in ${duration}ms")
                    Thread.sleep(100)
                }
                insertCompleted = true
            } catch (Exception e) {
                insertException = e
            }
        }

        // Refresh task: keep refreshing while inserts are running
        def refreshTask = {
            while (!insertCompleted && insertException == null) {
                try {
                    sql refresh_sql
                    Thread.sleep(200)
                } catch (Exception e) {
                    logger.warn("Refresh failed: ${e.message}")
                    Thread.sleep(200)
                }
            }
        }

        // Start both tasks
        def insertThread = Thread.start(insertTask)
        def refreshThread = Thread.start(refreshTask)

        // Wait for insert thread with 1 minute total timeout
        insertThread.join(60000) // 1 minute total

        // Force stop both threads if still running
        if (insertThread.isAlive()) {
            insertThread.interrupt()
        }
        refreshThread.interrupt()

        // Check results
        if (insertException != null) {
            throw new RuntimeException("Test failed: ${insertException.message}")
        }

        if (!insertCompleted) {
            throw new RuntimeException("Test failed: Inserts did not complete within 1 minute")
        }

        logger.info("✅ Test PASSED - All ${insertCount} inserts completed within timeout")

        // Cleanup
        sql """drop table if exists taxis"""
        sql """drop database if exists db_iceberg_insert_refresh"""
        sql """drop catalog if exists ${iceberg_catalog_name}"""

    }
}