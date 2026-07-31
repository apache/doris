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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

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
    // Reuse the fixture-wide Docker command so local and CI permission models behave identically.
    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"

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

    def runConcurrent = { String leftName, Closure leftAction,
                          String rightName, Closure rightAction ->
        CountDownLatch ready = new CountDownLatch(2)
        CountDownLatch start = new CountDownLatch(1)
        def left = thread(leftName) {
            ready.countDown()
            start.await()
            leftAction()
        }
        def right = thread(rightName) {
            ready.countDown()
            start.await()
            rightAction()
        }
        assertTrue(ready.await(30, TimeUnit.SECONDS),
                "Both Paimon writers must reach the dispatch barrier")
        start.countDown()
        left.get()
        right.get()
    }

    executeCommand("mkdir -p ${localDriverDir}", false, 60)
    if (!new File(localDriverPath).exists()) {
        executeCommand("/usr/bin/curl --max-time 600 ${driverDownloadUrl} --output ${localDriverPath}", true, 660)
    }

    def clusterHostIps = new ArrayList()
    String[][] backends = sql """show backends"""
    for (def backend in backends) {
        clusterHostIps.add(backend[1])
    }
    String[][] frontends = sql """show frontends"""
    for (def frontend in frontends) {
        clusterHostIps.add(frontend[1])
    }
    clusterHostIps = clusterHostIps.unique()

    Set<String> localHostIps = ["127.0.0.1", "localhost", "::1"] as Set
    java.util.Collections.list(java.net.NetworkInterface.getNetworkInterfaces()).each { networkInterface ->
        java.util.Collections.list(networkInterface.getInetAddresses()).each { address ->
            localHostIps.add(address.getHostAddress().split("%")[0])
        }
    }
    localHostIps.add(java.net.InetAddress.getLocalHost().getHostName())
    localHostIps.add(java.net.InetAddress.getLocalHost().getCanonicalHostName())

    for (def hostIp in clusterHostIps) {
        // Scenario: every FE/BE receives the JDBC driver so metadata failover and distributed
        // scan scheduling do not depend on a driver installed only on the regression runner.
        if (localHostIps.contains(hostIp)) {
            executeCommand("mkdir -p ${jdbcDriversDir}", true, 60)
            executeCommand("cp -f ${localDriverPath} ${jdbcDriversDir}/${driverName}", true, 60)
        } else {
            executeCommand(
                    "ssh -o BatchMode=yes -o StrictHostKeyChecking=no root@${hostIp} \"mkdir -p ${jdbcDriversDir}\"",
                    true,
                    60
            )
            scpFiles("root", hostIp, localDriverPath, jdbcDriversDir, false)
        }
    }

    String sparkContainerName = executeCommand(
            "${dockerCommand} ps --filter name=spark-iceberg --format {{.Names}}",
            false,
            30
    )
            ?.trim()
    if (sparkContainerName == null || sparkContainerName.isEmpty()) {
        logger.info("spark-iceberg container not found, skip this test")
        return
    }
    executeCommand("${dockerCommand} cp ${localDriverPath} ${sparkContainerName}:${sparkDriverPath}", true, 60)

    String sparkMinioEndpoint = "http://${externalEnvIp}:${minioPort}"
    if (sparkContainerName.contains("spark-iceberg")) {
        String sparkMinioContainerName = sparkContainerName.replaceFirst("spark-iceberg", "minio")
        String resolvedSparkMinioContainer = executeCommand(
                "${dockerCommand} ps --filter name=${sparkMinioContainerName} --format {{.Names}}",
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
        String command = """${dockerCommand} exec ${sparkContainerName} spark-sql --master spark://${sparkContainerName}:7077 \
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
--conf spark.sql.catalog.${sparkSeedCatalogName}.lock.enabled=true \
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
        // Paimon requires a catalog lock for safe concurrent snapshot commits on object storage.
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
                'paimon.lock.enabled' = 'true',
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

        // Scenario TC09-JDBC: Paimon JDBC catalog preserves the old snapshot schema after rename.
        String jdbcOldSnapshot = sql("""
            select snapshot_id
            from paimon_jdbc_tbl\$snapshots
            order by snapshot_id desc
            limit 1
        """)[0][0].toString()
        sparkPaimonJdbc """
            CALL ${sparkSeedCatalogName}.sys.create_tag(
                table => '${dbName}.paimon_jdbc_tbl',
                tag => 'jdbc_before_rename',
                snapshot => ${jdbcOldSnapshot}
            )
        """
        sparkPaimonJdbc """
            ALTER TABLE ${sparkSeedCatalogName}.${dbName}.paimon_jdbc_tbl
                RENAME COLUMN name TO jdbc_renamed_name
        """
        sql """REFRESH TABLE paimon_jdbc_tbl"""
        assertEquals([[1, "alice"], [2, "bob"]], sql("""
            select id, name
            from paimon_jdbc_tbl for version as of ${jdbcOldSnapshot}
            order by id
        """))
        assertEquals([[1, "alice"], [2, "bob"]], sql("""
            select id, name
            from paimon_jdbc_tbl@tag(jdbc_before_rename)
            order by id
        """))
        assertEquals([[1, "alice"], [2, "bob"]], sql("""
            select id, jdbc_renamed_name from paimon_jdbc_tbl order by id
        """))
        test {
            sql """select name from paimon_jdbc_tbl"""
            exception "Unknown column 'name'"
        }

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
            ["_ROW_ID", "_SEQUENCE_NUMBER"],
            1
        )

        // Append writers cover both independent partitions and snapshot-isolated writes
        // to the same partition. Every successful transaction must publish one snapshot.
        sql """DROP TABLE IF EXISTS paimon_jdbc_concurrent_append"""
        sql """
            CREATE TABLE ${dbName}.paimon_jdbc_concurrent_append (
                id BIGINT,
                writer_id INT,
                payload STRING,
                pt STRING
            ) ENGINE=paimon
            PARTITION BY (pt) ()
            PROPERTIES (
                'bucket' = '-1',
                'write-only' = 'true'
            )
        """

        long appendSnapshots = (sql """
            SELECT COUNT(*) FROM paimon_jdbc_concurrent_append\$snapshots
        """)[0][0] as long
        runConcurrent("paimon-jdbc-append-left", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_append
                SELECT number, 1, concat('left-', number), 'left'
                FROM numbers('number' = '128')
            """
        }, "paimon-jdbc-append-right", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_append
                SELECT number + 1000, 2, concat('right-', number), 'right'
                FROM numbers('number' = '128')
            """
        })
        sql """REFRESH TABLE paimon_jdbc_concurrent_append"""
        assertEquals([
                ["left", 128L, 128L, 8128L],
                ["right", 128L, 128L, 136128L]
        ], sql("""
            SELECT pt, COUNT(*), COUNT(DISTINCT id), SUM(id)
            FROM paimon_jdbc_concurrent_append
            GROUP BY pt
            ORDER BY pt
        """))
        assertEquals(appendSnapshots + 2L, (sql """
            SELECT COUNT(*) FROM paimon_jdbc_concurrent_append\$snapshots
        """)[0][0] as long)

        runConcurrent("paimon-jdbc-same-partition-left", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_append
                SELECT number + 2000, 3, concat('same-left-', number), 'same'
                FROM numbers('number' = '64')
            """
        }, "paimon-jdbc-same-partition-right", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_append
                SELECT number + 3000, 4, concat('same-right-', number), 'same'
                FROM numbers('number' = '64')
            """
        })
        sql """REFRESH TABLE paimon_jdbc_concurrent_append"""
        assertEquals([[128L, 128L, 324032L]], sql("""
            SELECT COUNT(*), COUNT(DISTINCT id), SUM(id)
            FROM paimon_jdbc_concurrent_append
            WHERE pt = 'same'
        """))
        assertEquals(appendSnapshots + 4L, (sql """
            SELECT COUNT(*) FROM paimon_jdbc_concurrent_append\$snapshots
        """)[0][0] as long)

        // Fixed-bucket deduplication may expose either value, but it must preserve
        // primary-key uniqueness and publish both successful transactions.
        sql """DROP TABLE IF EXISTS paimon_jdbc_concurrent_pk"""
        sql """
            CREATE TABLE ${dbName}.paimon_jdbc_concurrent_pk (
                id INT,
                payload STRING
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '1',
                'merge-engine' = 'deduplicate'
            )
        """

        long pkSnapshots = (sql """
            SELECT COUNT(*) FROM paimon_jdbc_concurrent_pk\$snapshots
        """)[0][0] as long
        runConcurrent("paimon-jdbc-pk-left", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_pk
                VALUES (1, 'left')
            """
        }, "paimon-jdbc-pk-right", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_pk
                VALUES (1, 'right')
            """
        })
        sql """REFRESH TABLE paimon_jdbc_concurrent_pk"""
        def pkRows = sql """SELECT id, payload FROM paimon_jdbc_concurrent_pk"""
        assertEquals(1, pkRows.size())
        assertEquals(1, pkRows[0][0] as int)
        assertTrue(["left", "right"].contains(pkRows[0][1].toString()))
        assertEquals(pkSnapshots + 2L, (sql """
            SELECT COUNT(*) FROM paimon_jdbc_concurrent_pk\$snapshots
        """)[0][0] as long)

        // Aggregation is a lost-update oracle because both deltas must remain visible.
        sql """DROP TABLE IF EXISTS paimon_jdbc_concurrent_aggregation"""
        sql """
            CREATE TABLE ${dbName}.paimon_jdbc_concurrent_aggregation (
                id INT,
                total BIGINT
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.total.aggregate-function' = 'sum'
            )
        """

        runConcurrent("paimon-jdbc-aggregation-left", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_aggregation
                VALUES (1, 10)
            """
        }, "paimon-jdbc-aggregation-right", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_aggregation
                VALUES (1, 20)
            """
        })
        sql """REFRESH TABLE paimon_jdbc_concurrent_aggregation"""
        assertEquals([[1, 30L]], sql("""
            SELECT id, total FROM paimon_jdbc_concurrent_aggregation
        """))

        // Dynamic bucket only permits multiple jobs when they own disjoint partitions.
        sql """DROP TABLE IF EXISTS paimon_jdbc_concurrent_dynamic"""
        sql """
            CREATE TABLE ${dbName}.paimon_jdbc_concurrent_dynamic (
                id INT,
                pt STRING,
                payload STRING
            ) ENGINE=paimon
            PARTITION BY (pt) ()
            PROPERTIES (
                'primary-key' = 'id,pt',
                'bucket' = '-1',
                'dynamic-bucket.target-row-num' = '32'
            )
        """

        runConcurrent("paimon-jdbc-dynamic-left", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_dynamic
                SELECT number, 'left', concat('left-', number)
                FROM numbers('number' = '64')
            """
        }, "paimon-jdbc-dynamic-right", {
            sql """
                INSERT INTO ${catalogName}.${dbName}.paimon_jdbc_concurrent_dynamic
                SELECT number + 1000, 'right', concat('right-', number)
                FROM numbers('number' = '64')
            """
        })
        sql """REFRESH TABLE paimon_jdbc_concurrent_dynamic"""
        assertEquals([
                ["left", 64L, 64L],
                ["right", 64L, 64L]
        ], sql("""
            SELECT pt, COUNT(*), COUNT(DISTINCT id)
            FROM paimon_jdbc_concurrent_dynamic
            GROUP BY pt
            ORDER BY pt
        """))
    } finally {
        try {
            sql """SWITCH ${catalogName}"""
            sql """DROP TABLE IF EXISTS ${dbName}.paimon_jdbc_concurrent_dynamic"""
            sql """DROP TABLE IF EXISTS ${dbName}.paimon_jdbc_concurrent_aggregation"""
            sql """DROP TABLE IF EXISTS ${dbName}.paimon_jdbc_concurrent_pk"""
            sql """DROP TABLE IF EXISTS ${dbName}.paimon_jdbc_concurrent_append"""
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
