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

// Http is a framework utility class, not an injected Suite DSL property.
import org.apache.doris.regression.util.Http

suite("test_paimon_write_thread_lifecycle", "p0,external,paimon,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_thread_lifecycle_catalog"
    String dbName = "test_pw_thread_lifecycle_db"

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
    def backendEndpoints = backendIdToIp.collectEntries { backendId, ip ->
        [(backendId): [ip.toString(), backendIdToHttpPort[backendId].toString()]]
    }
    assertFalse(backendEndpoints.isEmpty())

    def jvmThreadCounts = {
        backendEndpoints.collectEntries { backendId, endpoint ->
            [(backendId): (get_be_metric(endpoint[0], endpoint[1], "jvm_thread", "count") as long)]
        }
    }
    def processThreadCounts = {
        backendEndpoints.collectEntries { backendId, endpoint ->
            def body = Http.GET("http://${endpoint[0]}:${endpoint[1]}/api/be_process_thread_num",
                    false, false).toString()
            def item = parseJson(body).find { row -> row[0].toString() == "total_thread_count" }
            assertNotNull(item)
            [(backendId): item[1].toString().toLong()]
        }
    }
    def minimumThreadCounts = { counter ->
        def minimums = null
        for (int sample = 0; sample < 5; sample++) {
            def counts = counter()
            minimums = minimums == null ? counts : counts.collectEntries { backendId, count ->
                [(backendId): Math.min(minimums[backendId], count)]
            }
            sleep(1000)
        }
        minimums
    }

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_thread_lifecycle;
        CREATE TABLE paimon.${dbName}.t_thread_lifecycle (
            id BIGINT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '3',
            'bucket-key' = 'id',
            'num-sorted-run.compaction-trigger' = '2',
            'target-file-size' = '1 gb'
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
        // Warm all writer and metrics paths before taking the baseline. This keeps
        // one-time JVM attachment and SDK class initialization out of the leak oracle.
        for (int round = 0; round < 12; round++) {
            sql """
                INSERT INTO t_thread_lifecycle
                SELECT number + ${round * 1000}, repeat('w', 32)
                FROM numbers("number" = "1000")
            """
        }
        sleep(3000)

        def jvmBefore = minimumThreadCounts(jvmThreadCounts)
        def processBefore = minimumThreadCounts(processThreadCounts)
        logger.info("Paimon thread baseline: jvm=${jvmBefore}, process=${processBefore}")

        def writePhase = { int firstRound ->
            for (int round = firstRound; round < firstRound + 12; round++) {
                sql """
                    INSERT INTO t_thread_lifecycle
                    SELECT number + ${round * 1000}, repeat('x', 32)
                    FROM numbers("number" = "1000")
                """
            }
        }

        def jvmPhases = []
        def processPhases = []
        for (int phase = 0; phase < 4; phase++) {
            writePhase(12 + phase * 12)
            sleep(5000)
            jvmPhases.add(minimumThreadCounts(jvmThreadCounts))
            processPhases.add(minimumThreadCounts(processThreadCounts))
            logger.info("Paimon thread phase ${phase + 1}: jvm=${jvmPhases[-1]}, "
                    + "process=${processPhases[-1]}")
        }

        assertEquals(60000L,
                (sql """SELECT COUNT(*) FROM t_thread_lifecycle""")[0][0] as long)

        backendEndpoints.keySet().each { backendId ->
            // Warm-up now performs the same workload as every measured phase, so compare every
            // phase with the actual pre-phase baseline. A bounded per-writer leak can no longer be
            // hidden by using phase one as the oracle.
            jvmPhases.eachWithIndex { counts, phase ->
                assertTrue(counts[backendId] <= jvmBefore[backendId] + 2,
                        "JVM threads grew after warm-up on backend ${backendId}, phase ${phase + 1}: "
                                + "baseline=${jvmBefore[backendId]}, phases="
                                + jvmPhases.collect { sample -> sample[backendId] })
            }
            processPhases.eachWithIndex { counts, phase ->
                assertTrue(counts[backendId] <= processBefore[backendId] + 4,
                        "Process threads grew after warm-up on backend ${backendId}, phase ${phase + 1}: "
                                + "baseline=${processBefore[backendId]}, phases="
                                + processPhases.collect { sample -> sample[backendId] })
            }
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
