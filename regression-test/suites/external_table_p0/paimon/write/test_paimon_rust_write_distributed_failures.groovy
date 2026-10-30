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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.util.Http
import java.util.concurrent.atomic.AtomicReference

suite("test_paimon_rust_write_distributed_failures", "p0,external,paimon,nonConcurrent") {
    if (!context.config.otherConfigs.get("enablePaimonTest")?.equalsIgnoreCase("true")) {
        return
    }
    def hosts = [:]
    def ports = [:]
    getBackendIpHttpPort(hosts, ports)
    ports = ports.collectEntries { id, port -> [id, port as int] }
    def backends = ports.keySet().findAll { ports[it] > 0 }
    if (backends.size() < 2) {
        logger.info("Paimon distributed failure tests require at least two BEs")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(
                    "http://${externalEnvIp}:${minioPort}", "us-east-1"))
            .withPathStyleAccessEnabled(true)
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("admin", "password")))
            .build()
    def keys = { String table, String directory ->
        def result = [] as Set
        String token = null
        while (true) {
            def listing = client.listObjectsV2(new ListObjectsV2Request()
                    .withBucketName("warehouse")
                    .withPrefix("wh/test_paimon_rust_distributed_db.db/${table}/${directory}/")
                    .withContinuationToken(token))
            result.addAll(listing.objectSummaries.collect { it.key }.findAll { !it.endsWith("/") })
            if (!listing.truncated) {
                break
            }
            token = listing.nextContinuationToken
        }
        return result
    }
    def awaitCondition = { String description, Closure condition ->
        long deadline = System.currentTimeMillis() + 60000
        while (!condition()) {
            assertTrue(System.currentTimeMillis() < deadline, "Timed out: ${description}")
            sleep(100)
        }
    }
    def insertSql = { String table ->
        """INSERT INTO test_paimon_rust_distributed_catalog.test_paimon_rust_distributed_db.${table}
            SELECT id, payload FROM internal.regression_test.paimon_rust_distributed_source"""
    }
    def bePoint = { backend, String point, boolean enabled ->
        if (enabled) {
            DebugPoint.enableDebugPoint(hosts[backend], ports[backend], NodeType.BE, point, [timeout:120])
        } else {
            DebugPoint.disableDebugPoint(hosts[backend], ports[backend], NodeType.BE, point)
        }
    }
    def hasPaimonTask = { backend ->
        Http.GET("http://${hosts[backend]}:${ports[backend]}/api/running_pipeline_tasks", false, false)
                .toString().toLowerCase().contains("paimon")
    }
    def master = sql_return_maparray("SHOW FRONTENDS").find { it.IsMaster.toString().equalsIgnoreCase("true") }
    def transactionEvents = {
        String metrics = Http.GET("http://${master.Host}:${master.HttpPort}/metrics", false, false).toString()
        def events = [:]
        ["committed", "aborted", "outcome_unknown", "commit_retry", "late_report"].each { event ->
            def value = metrics =~ /(?m)^doris_fe_paimon_write_events_total\{[^}]*event="${event}"[^}]*\}\s+([0-9.]+)/
            assertTrue(value.find(), "Missing Paimon transaction metric: ${event}")
            events[event] = value.group(1).toDouble() as long
        }
        return events
    }
    def originalRust = sql "SELECT @@enable_paimon_rust_writer"
    def originalScanner = sql "SELECT @@force_jni_scanner"
    def originalParallelism = sql "SELECT @@parallel_pipeline_task_num"
    String blockPoint = "FfiPaimonWriteBackend.prepare.block"
    String failPoint = "FfiPaimonWriteBackend.prepare.fail"
    def healthy = backends[0]
    def failing = backends[1]
    try {
        sql "SWITCH internal"
        sql "CREATE DATABASE IF NOT EXISTS regression_test"
        sql "USE regression_test"
        sql "DROP TABLE IF EXISTS paimon_rust_distributed_source"
        sql """CREATE TABLE paimon_rust_distributed_source (id BIGINT, payload STRING)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8
            PROPERTIES ('replication_num'='1')"""
        sql """INSERT INTO paimon_rust_distributed_source
            SELECT number, repeat('x', 1024) FROM numbers('number'='10000')"""
        def sourceBackends = sql_return_maparray("SHOW TABLETS FROM paimon_rust_distributed_source")
                .collect { it.BackendId.toString() }.toSet()
        assertTrue(sourceBackends.size() >= 2, "Source tablets must be placed on multiple BEs")
        // Use BEs that actually own source tablets.
        def writers = backends.findAll { sourceBackends.contains(it.toString()) }
        healthy = writers[0]
        failing = writers[1]
        spark_paimon_multi "CREATE DATABASE IF NOT EXISTS paimon.test_paimon_rust_distributed_db"
        ["t_multi", "t_be_failure", "t_fe_before", "t_fe_after", "t_fe_unknown"].each { table ->
            spark_paimon_multi """
                DROP TABLE IF EXISTS paimon.test_paimon_rust_distributed_db.${table};
                CREATE TABLE paimon.test_paimon_rust_distributed_db.${table} (id BIGINT, payload STRING)
                USING paimon TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet',
                    'target-file-size'='1 b');
            """
        }
        sql "DROP CATALOG IF EXISTS test_paimon_rust_distributed_catalog"
        sql """CREATE CATALOG test_paimon_rust_distributed_catalog PROPERTIES (
            'type'='paimon', 'paimon.catalog.type'='filesystem', 'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true')"""
        sql "SET enable_paimon_rust_writer=true"
        sql "SET force_jni_scanner=true"
        sql "SET parallel_pipeline_task_num=1"
        ["t_multi", "t_be_failure", "t_fe_before", "t_fe_after", "t_fe_unknown"].each { table ->
            sql """INSERT INTO test_paimon_rust_distributed_catalog.test_paimon_rust_distributed_db.${table}
                VALUES (-1, 'baseline')"""
        }
        sql "SWITCH test_paimon_rust_distributed_catalog"
        sql "USE test_paimon_rust_distributed_db"
        def beforeEvents = transactionEvents()
        sql insertSql("t_multi")
        order_qt_multi "SELECT COUNT(*), SUM(id) FROM t_multi"
        assertSparkDorisResultEquals(
                spark_paimon("SELECT COUNT(*), SUM(id) FROM paimon.test_paimon_rust_distributed_db.t_multi"),
                sql("SELECT COUNT(*), SUM(id) FROM t_multi"))

        def oldData = keys("t_be_failure", "bucket-0")
        def oldSnapshots = keys("t_be_failure", "snapshot")
        bePoint(healthy, blockPoint, true)
        bePoint(failing, blockPoint, true)
        bePoint(failing, failPoint, true)
        // Delay the successful BE's final report to exercise cancellation/report ordering.
        bePoint(healthy, "FragmentMgr::coordinator_callback.report_delay", true)
        def controlError = new AtomicReference<Throwable>()
        Thread controller = Thread.start("paimon-partial-be-failure") {
            try {
                awaitCondition("both BEs hold prepared input") { hasPaimonTask(healthy) && hasPaimonTask(failing) }
                bePoint(healthy, blockPoint, false)
                awaitCondition("healthy BE finishes its writer") { !hasPaimonTask(healthy) }
            } catch (Throwable t) {
                controlError.set(t)
            } finally {
                bePoint(healthy, blockPoint, false)
                bePoint(failing, blockPoint, false)
            }
        }
        try {
            test {
                sql insertSql("t_be_failure")
                exception "Injected Paimon Rust prepare failure"
            }
        } finally {
            controller.join(65000)
            bePoint(healthy, blockPoint, false)
            bePoint(failing, blockPoint, false)
            bePoint(failing, failPoint, false)
            bePoint(healthy, "FragmentMgr::coordinator_callback.report_delay", false)
        }
        assertFalse(controller.alive)
        assertNull(controlError.get())
        awaitCondition("partial BE failure removes uncommitted data") { keys("t_be_failure", "bucket-0") == oldData }
        assertEquals(oldSnapshots, keys("t_be_failure", "snapshot"))
        order_qt_be_failure "SELECT COUNT(*), SUM(id) FROM t_be_failure"

        oldData = keys("t_fe_before", "bucket-0")
        oldSnapshots = keys("t_fe_before", "snapshot")
        try {
            GetDebugPoint().enableDebugPointForAllFEs("PaimonTransaction.commit.before", [timeout:120])
            test {
                sql insertSql("t_fe_before")
                exception "Injected Paimon failure before commit"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("PaimonTransaction.commit.before")
        }
        assertEquals(oldData, keys("t_fe_before", "bucket-0"))
        assertEquals(oldSnapshots, keys("t_fe_before", "snapshot"))
        order_qt_fe_before "SELECT COUNT(*), SUM(id) FROM t_fe_before"

        oldSnapshots = keys("t_fe_after", "snapshot")
        try {
            GetDebugPoint().enableDebugPointForAllFEs("PaimonTransaction.commit.after", [timeout:120])
            sql insertSql("t_fe_after")
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("PaimonTransaction.commit.after")
        }
        assertEquals(oldSnapshots.size() + 1, keys("t_fe_after", "snapshot").size())
        order_qt_fe_after "SELECT COUNT(*), SUM(id) FROM t_fe_after"
        assertSparkDorisResultEquals(
                spark_paimon("SELECT COUNT(*), SUM(id) FROM paimon.test_paimon_rust_distributed_db.t_fe_after"),
                sql("SELECT COUNT(*), SUM(id) FROM t_fe_after"))

        oldData = keys("t_fe_unknown", "bucket-0")
        oldSnapshots = keys("t_fe_unknown", "snapshot")
        try {
            GetDebugPoint().enableDebugPointForAllFEs("PaimonTransaction.commit.unknown", [timeout:120])
            test {
                sql insertSql("t_fe_unknown")
                exception "Injected Paimon commit failure with unknown outcome"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("PaimonTransaction.commit.unknown")
        }
        // A missing snapshot does not prove an in-flight remote publish cannot finish.
        // Preserve prepared files when the FE cannot determine the commit outcome.
        assertTrue(keys("t_fe_unknown", "bucket-0").size() > oldData.size())
        assertEquals(oldSnapshots, keys("t_fe_unknown", "snapshot"))
        order_qt_fe_unknown "SELECT COUNT(*), SUM(id) FROM t_fe_unknown"
        def afterEvents = transactionEvents()
        assertEquals(2L, afterEvents.committed - beforeEvents.committed)
        assertEquals(2L, afterEvents.aborted - beforeEvents.aborted)
        assertEquals(1L, afterEvents.outcome_unknown - beforeEvents.outcome_unknown)
        assertEquals(2L, afterEvents.commit_retry - beforeEvents.commit_retry)
        // The successful report can arrive before rollback when the failing BE takes longer
        // to abort. File-set checks above cover either ordering; a late event is not guaranteed.
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("PaimonTransaction.commit.before")
        GetDebugPoint().disableDebugPointForAllFEs("PaimonTransaction.commit.after")
        GetDebugPoint().disableDebugPointForAllFEs("PaimonTransaction.commit.unknown")
        bePoint(healthy, blockPoint, false)
        bePoint(failing, blockPoint, false)
        bePoint(failing, failPoint, false)
        sql "SET enable_paimon_rust_writer=${originalRust[0][0]}"
        sql "SET force_jni_scanner=${originalScanner[0][0]}"
        sql "SET parallel_pipeline_task_num=${originalParallelism[0][0]}"
        bePoint(healthy, "FragmentMgr::coordinator_callback.report_delay", false)
        client.shutdown()
    }
}
