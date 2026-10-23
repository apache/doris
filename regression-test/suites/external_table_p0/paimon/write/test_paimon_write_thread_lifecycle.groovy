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

    // Native execution workers remain attached to the JVM until they exit, so total JVM
    // thread growth does not identify a writer leak. Track the writer-owned executor instead.
    // Linux /proc comm names are truncated to 15 bytes; this prefix also matches
    // PaimonJniWriter's "doris-paimon-compaction-thread-N" after truncation.
    def writerThreadCounts = {
        backendEndpoints.collectEntries { backendId, endpoint ->
            def body = Http.GET("http://${endpoint[0]}:${endpoint[1]}/api/be_process_thread_num",
                    false, false).toString()
            def rows = parseJson(body)
            def total = rows.find { row -> row[0].toString() == "total_thread_count" }
            assertNotNull(total)
            def writers = rows.findAll { row -> row[0].toString().startsWith("doris-paimon-") }
                    .collectEntries { row -> [(row[0].toString()): row[1].toString().toLong()] }
            [(backendId): [total: total[1].toString().toLong(), writers: writers]]
        }
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
        int roundsPerPhase = 12
        int phaseCount = 10
        for (int phase = 0; phase < phaseCount; phase++) {
            for (int round = phase * roundsPerPhase; round < (phase + 1) * roundsPerPhase; round++) {
                sql """
                    INSERT INTO t_thread_lifecycle
                    SELECT number + ${round * 1000}, repeat('x', 32)
                    FROM numbers("number" = "1000")
                """
            }

            // Each writer owns its compaction executor and closes it with the sink. Require all
            // of those threads to exit after every phase, without a baseline that can absorb leaks.
            def counts = [:]
            try {
                awaitUntil(30) {
                    counts = writerThreadCounts()
                    counts.values().every { sample -> sample.writers.values().every { count -> count == 0 } }
                }
            } finally {
                logger.info("Paimon writer threads after phase ${phase + 1}: ${counts}")
            }
        }

        assertEquals(phaseCount * roundsPerPhase * 1000L,
                (sql """SELECT COUNT(*) FROM t_thread_lifecycle""")[0][0] as long)
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
