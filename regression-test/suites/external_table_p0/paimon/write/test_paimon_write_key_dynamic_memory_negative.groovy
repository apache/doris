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

import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicReference

suite("test_paimon_write_key_dynamic_memory_negative", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    // This opt-in case intentionally puts sustained pressure on the embedded JVM.
    String knownBugTestEnabled = context.config.otherConfigs.get("enablePaimonKnownBugTest")
    if (knownBugTestEnabled == null || !knownBugTestEnabled.equalsIgnoreCase("true")) {
        logger.info("skip isolated Paimon known-bug resource regression")
        return
    }

    long stressRows = (context.config.otherConfigs.get("paimonKeyDynamicStressRows")
            ?: "4000000").toLong()
    long queryMemoryLimit = 128L * 1024 * 1024
    long allowedJvmGrowth = queryMemoryLimit + 64L * 1024 * 1024
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_key_dynamic_memory_catalog"
    String dbName = "test_pw_key_dynamic_memory_db"

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
    def backendEndpoints = backendIdToIp.collectEntries { backendId, ip ->
        [(backendId): [ip.toString(), backendIdToHttpPort[backendId].toString()]]
    }
    assertFalse(backendEndpoints.isEmpty())
    def heapUsed = {
        backendEndpoints.collectEntries { backendId, endpoint ->
            [(backendId): (get_be_metric(endpoint[0], endpoint[1],
                    "jvm_heap_size_bytes", "used") as long)]
        }
    }

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_key_dynamic_memory;
        CREATE TABLE paimon.${dbName}.t_key_dynamic_memory (
            pt STRING, id STRING, payload STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '10000',
            'dynamic-bucket.max-buckets' = '64',
            'write-buffer-size' = '16 mb',
            'page-size' = '64 kb',
            'write-buffer-spillable' = 'true'
        );
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        sql """INSERT INTO t_key_dynamic_memory VALUES ('warmup', 'warmup', 'warmup')"""
        sleep(3000)
        def baseline = heapUsed()
        def peak = new LinkedHashMap(baseline)
        def writeFailure = new AtomicReference<Throwable>()
        def activeStatement = new AtomicReference<java.sql.Statement>()

        Thread writerThread = Thread.start("paimon-key-dynamic-memory-writer") {
            try (def connection = DriverManager.getConnection(context.config.jdbcUrl,
                    context.config.jdbcUser, context.config.jdbcPassword);
                    def statement = connection.createStatement()) {
                activeStatement.set(statement)
                statement.execute("SET exec_mem_limit = ${queryMemoryLimit}")
                statement.execute("SWITCH ${catalogName}")
                statement.execute("USE ${dbName}")
                statement.execute("""
                    INSERT INTO t_key_dynamic_memory
                    SELECT concat('p', CAST(number % 64 AS STRING)),
                           concat(lpad(CAST(number AS STRING), 20, '0'), repeat('k', 76)),
                           repeat('v', 32)
                    FROM numbers("number" = "${stressRows}")
                """)
            } catch (Throwable t) {
                writeFailure.set(t)
            } finally {
                activeStatement.set(null)
            }
        }

        long deadline = System.currentTimeMillis() + 20L * 60 * 1000
        while (writerThread.isAlive() && System.currentTimeMillis() < deadline) {
            sleep(1000)
            heapUsed().each { backendId, used ->
                peak[backendId] = Math.max(peak[backendId], used)
            }
        }
        writerThread.join(10000)
        if (writerThread.isAlive()) {
            // Cancel the stress query before failing so a timeout cannot leave its
            // JDBC writer running after the regression suite has already finished.
            activeStatement.get()?.cancel()
            writerThread.join(10000)
        }
        assertFalse(writerThread.isAlive(), "KEY_DYNAMIC stress insert did not finish within 20 minutes")

        def growth = peak.collectEntries { backendId, used ->
            [(backendId): used - baseline[backendId]]
        }
        def failureMessages = []
        Throwable failure = writeFailure.get()
        while (failure != null && !failureMessages.contains(failure.toString())) {
            failureMessages.add(failure.toString())
            failure = failure.getCause()
        }
        String failureMessage = failureMessages.join(" caused by ")
        logger.info("Paimon KEY_DYNAMIC memory result: rows=${stressRows}, baseline=${baseline}, "
                + "peak=${peak}, growth=${growth}, failure=${failureMessage}")

        // A valid query may be rejected by a memory limit, but the embedded JVM
        // must not be the component that exhausts memory outside Doris accounting.
        assertFalse(failureMessage.contains("OutOfMemoryError"),
                "KEY_DYNAMIC write exhausted the embedded JVM: ${failureMessage}")
        assertTrue(growth.values().every { delta -> delta <= allowedJvmGrowth },
                "KEY_DYNAMIC Java heap growth escaped the query memory limit: "
                        + "limit=${queryMemoryLimit}, allowed=${allowedJvmGrowth}, growth=${growth}")
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
