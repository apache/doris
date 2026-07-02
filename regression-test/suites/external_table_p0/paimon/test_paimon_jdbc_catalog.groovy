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

suite("test_paimon_jdbc_catalog", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Paimon test is not enabled, skip this test")
        return
    }

    String enabledJdbc = context.config.otherConfigs.get("enableJdbcTest")
    if (enabledJdbc == null || !enabledJdbc.equalsIgnoreCase("true")) {
        logger.info("Paimon JDBC catalog test requires enableJdbcTest, skip this test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("paimon_jdbc_minio_port")
    if (minioPort == null || minioPort.isEmpty()) {
        minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    }
    String jdbcPort = context.config.otherConfigs.get("pg_14_port")
    if (externalEnvIp == null || externalEnvIp.isEmpty()
            || minioPort == null || minioPort.isEmpty()
            || jdbcPort == null || jdbcPort.isEmpty()) {
        logger.info("Paimon JDBC catalog test environment is not fully configured, skip this test")
        return
    }

    String minioAk = context.config.otherConfigs.get("paimon_jdbc_minio_ak")
    if (minioAk == null || minioAk.isEmpty()) {
        minioAk = "admin"
    }
    String minioSk = context.config.otherConfigs.get("paimon_jdbc_minio_sk")
    if (minioSk == null || minioSk.isEmpty()) {
        minioSk = "password"
    }
    String warehouseBucket = context.config.otherConfigs.get("paimon_jdbc_warehouse_bucket")
    if (warehouseBucket == null || warehouseBucket.isEmpty()) {
        warehouseBucket = "warehouse"
    }

    String catalogName = "test_paimon_jdbc_catalog"
    String dbName = "paimon_jdbc_db"
    String driverName = "postgresql-42.5.0.jar"
    String driverDownloadUrl = "${getS3Url()}/regression/jdbc_driver/${driverName}"
    String jdbcDriversDir = getFeConfig("jdbc_drivers_dir")
    String localDriverDir = "${context.config.dataPath}/jdbc_driver"
    String localDriverPath = "${localDriverDir}/${driverName}"
    String sparkDriverPath = "/tmp/${driverName}"
    String sparkSeedCatalogName = "${catalogName}_seed"

    assertTrue(jdbcDriversDir != null && !jdbcDriversDir.isEmpty(), "jdbc_drivers_dir must be configured")

    def executeCommand = { String cmd, Boolean mustSuc, int timeoutSeconds = 300 ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        try {
            logger.info("execute ${cmd}")
            def proc = new ProcessBuilder("/bin/bash", "-c", cmd).start()
            proc.consumeProcessOutput(stdout, stderr)
            proc.waitForOrKill(timeoutSeconds * 1000)
            int exitcode = proc.exitValue()
            String output = stdout.toString()
            String error = stderr.toString()
            if (exitcode != 0) {
                logger.info("exit code: ${exitcode}, stdout\n: ${output}\nstderr\n: ${error}")
                if (mustSuc) {
                    assertTrue(false, "Execute failed: ${cmd}\nstdout:\n${output}\nstderr:\n${error}")
                }
            }
            return output
        } catch (IOException e) {
            assertTrue(false, "Execute failed: ${cmd}, err: ${e.message}")
        }
    }

    executeCommand("mkdir -p ${localDriverDir}", false, 60)
    executeCommand("mkdir -p ${jdbcDriversDir}", true, 60)
    if (!new File(localDriverPath).exists()) {
        executeCommand("/usr/bin/curl --max-time 600 ${driverDownloadUrl} --output ${localDriverPath}", true, 660)
    }
    executeCommand("cp -f ${localDriverPath} ${jdbcDriversDir}/${driverName}", true, 60)

    String sparkContainerName = executeCommand("docker ps --filter name=spark-iceberg --format {{.Names}}", false, 30)
            ?.trim()
    if (sparkContainerName == null || sparkContainerName.isEmpty()) {
        logger.info("spark-iceberg container not found, skip this test")
        return
    }
    executeCommand("docker cp ${localDriverPath} ${sparkContainerName}:${sparkDriverPath}", true, 60)

    String sparkMinioEndpoint = "http://${externalEnvIp}:${minioPort}"
    if (sparkContainerName.contains("spark-iceberg")) {
        String sparkMinioContainerName = sparkContainerName.replaceFirst("spark-iceberg", "minio")
        String resolvedSparkMinioContainer = executeCommand(
                "docker ps --filter name=${sparkMinioContainerName} --format {{.Names}}",
                false,
                30
        )?.trim()
        if (resolvedSparkMinioContainer == sparkMinioContainerName) {
            // Spark runs inside the docker network and may not be able to reach the host-mapped MinIO port.
            sparkMinioEndpoint = "http://${resolvedSparkMinioContainer}:9000"
        }
    }
    logger.info("spark seed minio endpoint: ${sparkMinioEndpoint}")

    def sparkPaimonJdbc = { String sqlText ->
        String escapedSql = sqlText.replaceAll('"', '\\\\"')
        String command = """docker exec ${sparkContainerName} spark-sql --master spark://${sparkContainerName}:7077 \
--jars ${sparkDriverPath} \
--driver-class-path ${sparkDriverPath} \
--conf spark.driver.extraClassPath=${sparkDriverPath} \
--conf spark.executor.extraClassPath=${sparkDriverPath} \
--conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
--conf spark.sql.catalog.${sparkSeedCatalogName}=org.apache.paimon.spark.SparkCatalog \
--conf spark.sql.catalog.${sparkSeedCatalogName}.warehouse=s3://${warehouseBucket}/paimon_jdbc_catalog/ \
--conf spark.sql.catalog.${sparkSeedCatalogName}.metastore=jdbc \
--conf spark.sql.catalog.${sparkSeedCatalogName}.uri=jdbc:postgresql://${externalEnvIp}:${jdbcPort}/postgres \
--conf spark.sql.catalog.${sparkSeedCatalogName}.catalog-key=${catalogName} \
--conf spark.sql.catalog.${sparkSeedCatalogName}.jdbc.user=postgres \
--conf spark.sql.catalog.${sparkSeedCatalogName}.jdbc.password=123456 \
--conf spark.sql.catalog.${sparkSeedCatalogName}.lock.enabled=false \
--conf spark.sql.catalog.${sparkSeedCatalogName}.s3.endpoint=${sparkMinioEndpoint} \
--conf spark.sql.catalog.${sparkSeedCatalogName}.s3.access-key=${minioAk} \
--conf spark.sql.catalog.${sparkSeedCatalogName}.s3.secret-key=${minioSk} \
--conf spark.sql.catalog.${sparkSeedCatalogName}.s3.region=us-east-1 \
--conf spark.sql.catalog.${sparkSeedCatalogName}.s3.path.style.access=true \
-e "${escapedSql}" """
        executeCommand(command, true, 300)
    }

    def assertSystemTableReadable = { String tableExpr, List<String> expectedColumns = [], Integer minCount = null ->
        def descRows = sql """DESC ${tableExpr}"""
        assertTrue(descRows.size() > 0)
        expectedColumns.each { col ->
            assertTrue(descRows.toString().contains(col))
        }

        def countRows = sql """SELECT COUNT(*) FROM ${tableExpr}"""
        assertEquals(1, countRows.size())
        int countValue = countRows[0][0].toString().toInteger()
        if (minCount != null) {
            assertTrue(countValue >= minCount)
        }
        return countValue
    }

    try {
        sql """switch internal"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'paimon',
                'paimon.catalog.type' = 'jdbc',
                'uri' = 'jdbc:postgresql://${externalEnvIp}:${jdbcPort}/postgres',
                'warehouse' = 's3://${warehouseBucket}/paimon_jdbc_catalog/',
                'paimon.catalog-key' = '${catalogName}',
                'paimon.jdbc.driver_url' = 'file://${jdbcDriversDir}/${driverName}',
                'paimon.jdbc.driver_class' = 'org.postgresql.Driver',
                'paimon.jdbc.user' = 'postgres',
                'paimon.jdbc.password' = '123456',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.access_key' = '${minioAk}',
                's3.secret_key' = '${minioSk}',
                's3.region' = 'us-east-1',
                'use_path_style' = 'true'
            )
        """

        sql """SWITCH ${catalogName}"""
        def catalogs = sql """SHOW CATALOGS"""
        assertTrue(catalogs.toString().contains(catalogName))

        sql """DROP DATABASE IF EXISTS ${dbName} FORCE"""
        sql """CREATE DATABASE ${dbName}"""
        def databases = sql """SHOW DATABASES"""
        assertTrue(databases.toString().contains(dbName))

        sql """USE ${dbName}"""
        sql """DROP TABLE IF EXISTS paimon_jdbc_tbl"""
        sql """
            CREATE TABLE ${dbName}.paimon_jdbc_tbl (
                id INT,
                name STRING,
                dt DATE
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '2'
            )
        """

        def tables = sql """SHOW TABLES"""
        assertTrue(tables.toString().contains("paimon_jdbc_tbl"))

        sparkPaimonJdbc """
            INSERT INTO ${sparkSeedCatalogName}.${dbName}.paimon_jdbc_tbl VALUES
            (1, 'alice', DATE '2025-01-01'),
            (2, 'bob', DATE '2025-01-02')
        """

        def descResult = sql """DESC paimon_jdbc_tbl"""
        assertTrue(descResult.toString().contains("id"))
        assertTrue(descResult.toString().contains("name"))
        assertTrue(descResult.toString().contains("dt"))

        order_qt_paimon_jdbc_select """SELECT * FROM paimon_jdbc_tbl ORDER BY id"""

        def rowCount = sql """SELECT COUNT(*) FROM paimon_jdbc_tbl"""
        assertEquals(1, rowCount.size())
        assertEquals("2", rowCount[0][0].toString())

        assertSystemTableReadable("paimon_jdbc_tbl\$schemas", ["schema_id"], 1)
        assertSystemTableReadable("paimon_jdbc_tbl\$snapshots", ["snapshot_id"], 1)
        [
            "paimon_jdbc_tbl\$options",
            "paimon_jdbc_tbl\$audit_log",
            "paimon_jdbc_tbl\$files",
            "paimon_jdbc_tbl\$tags",
            "paimon_jdbc_tbl\$branches",
            "paimon_jdbc_tbl\$consumers",
            "paimon_jdbc_tbl\$ro",
            "paimon_jdbc_tbl\$aggregation_fields",
            "paimon_jdbc_tbl\$binlog",
            "paimon_jdbc_tbl\$manifests",
            "paimon_jdbc_tbl\$partitions",
            "paimon_jdbc_tbl\$buckets",
            "paimon_jdbc_tbl\$statistics",
            "paimon_jdbc_tbl\$table_indexes"
        ].each { tableExpr ->
            assertSystemTableReadable(tableExpr)
        }

        sql """DROP TABLE IF EXISTS paimon_jdbc_row_tracking_tbl"""
        sql """
            CREATE TABLE ${dbName}.paimon_jdbc_row_tracking_tbl (
                id INT,
                name STRING,
                dt DATE
            ) ENGINE=paimon
            PROPERTIES (
                'bucket' = '-1',
                'row-tracking.enabled' = 'true'
            )
        """

        sparkPaimonJdbc """
            INSERT INTO ${sparkSeedCatalogName}.${dbName}.paimon_jdbc_row_tracking_tbl VALUES
            (3, 'carol', DATE '2025-01-03'),
            (4, 'dave', DATE '2025-01-04')
        """

        assertSystemTableReadable(
            "paimon_jdbc_row_tracking_tbl\$row_tracking",
            ["_row_id", "_sequence_number"],
            1
        )
    } finally {
        try {
            sql """SWITCH ${catalogName}"""
            sql """DROP TABLE IF EXISTS ${dbName}.paimon_jdbc_row_tracking_tbl"""
            sql """DROP TABLE IF EXISTS ${dbName}.paimon_jdbc_tbl"""
            sql """DROP DATABASE IF EXISTS ${dbName} FORCE"""
        } catch (Exception e) {
            logger.info("Cleanup in catalog ${catalogName} failed: ${e.getMessage()}")
        }
        try {
            sql """SWITCH internal"""
        } catch (Exception e) {
            logger.info("Switch back to internal catalog failed: ${e.getMessage()}")
        }
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
