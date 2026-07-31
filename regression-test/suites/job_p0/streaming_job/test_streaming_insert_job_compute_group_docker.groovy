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

import org.apache.doris.regression.suite.ClusterOptions
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

// Docker-mode coverage for compute_group routing on StreamingInsertJob:
// spins up cloud cluster with two compute groups and asserts that the bound
// compute_group actually steers BE traffic. Complements the non-docker suite
// test_streaming_insert_job_compute_group which covers property lifecycle only.
suite("test_streaming_insert_job_compute_group_docker", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    def tableName = "test_sij_cg_docker_tbl"
    def jobName = "test_sij_cg_docker_job"
    def cgA = "compute_cluster"
    def cgB = "sij_cg_b_docker"

    docker(options) {
        // default BE (index 0) lives in ${cgA}; the BE added below joins ${cgB}.
        cluster.addBackend(1, cgB)
        Awaitility.await().atMost(60, SECONDS).pollInterval(2, SECONDS).until({
            def cgs = sql """SHOW COMPUTE GROUPS"""
            cgs.size() == 2
        })
        log.info("compute groups: ${sql """SHOW COMPUTE GROUPS"""}")

        def backends = cluster.getAllBackends().sort { it.backendId }
        assertEquals(2, backends.size())
        def beA = backends.get(0)
        def beB = backends.get(1)
        log.info("beA=${beA.host}:${beA.httpPort} (${cgA}) beB=${beB.host}:${beB.httpPort} (${cgB})")

        sql """drop table if exists `${tableName}` force"""
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `c1` int NULL,
                `c2` string NULL,
                `c3` int NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`c1`)
            DISTRIBUTED BY HASH(`c1`) BUCKETS 3;
        """

        def s3Source = """
            SELECT * FROM S3(
                "uri" = "s3://${s3BucketName}/regression/load/data/example_[0-1].csv",
                "format" = "csv",
                "provider" = "${getS3Provider()}",
                "column_separator" = ",",
                "s3.endpoint" = "${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}"
            )
        """

        try {
            def aBefore = get_be_metric(beA.host, beA.httpPort, "load_rows") as long
            def bBefore = get_be_metric(beB.host, beB.httpPort, "load_rows") as long
            log.info("phase0 a=${aBefore} b=${bBefore}")

            // Phase 1: bind to cgA, verify traffic stays on cgA
            sql """
                CREATE JOB ${jobName}
                PROPERTIES (
                    "s3.max_batch_files" = "1",
                    "compute_group" = "${cgA}"
                )
                ON STREAMING DO INSERT INTO ${tableName} ${s3Source};
            """

            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
                cnt.size() == 1 && Integer.parseInt(cnt.get(0).get(0).toString()) >= 2
            })

            def aAfter1 = get_be_metric(beA.host, beA.httpPort, "load_rows") as long
            def bAfter1 = get_be_metric(beB.host, beB.httpPort, "load_rows") as long
            log.info("phase1 a=${aAfter1} b=${bAfter1}")
            assertTrue(aAfter1 > aBefore, "phase1 expects cgA load_rows to increase")
            assertTrue(bAfter1 == bBefore, "phase1 expects cgB untouched")
            def rows1 = (sql """SELECT count(*) FROM ${tableName}""").get(0).get(0) as long
            assertTrue(rows1 > 0, "phase1 expects target table to receive rows")

            // Phase 2: ALTER compute_group to cgB with reset offset, verify traffic switches
            sql """PAUSE JOB where jobname = '${jobName}'"""
            Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
                def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                s.size() == 1 && 'PAUSED' == s.get(0).get(0)
            })

            sql """
                ALTER JOB ${jobName} PROPERTIES (
                    "compute_group" = "${cgB}",
                    "offset" = '{"fileName":"regression/load/data/anoexist1234.csv"}'
                )
            """
            sql """RESUME JOB where jobname = '${jobName}'"""

            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
                cnt.size() == 1 && Integer.parseInt(cnt.get(0).get(0).toString()) >= 4
            })

            def aAfter2 = get_be_metric(beA.host, beA.httpPort, "load_rows") as long
            def bAfter2 = get_be_metric(beB.host, beB.httpPort, "load_rows") as long
            log.info("phase2 a=${aAfter2} b=${bAfter2}")
            assertTrue(aAfter2 == aAfter1, "phase2 expects cgA unchanged")
            assertTrue(bAfter2 > bAfter1, "phase2 expects cgB load_rows to increase")

            // Phase 3: compute_group=cgA plus session.cloud_cluster=cgB; compute_group must win
            sql """PAUSE JOB where jobname = '${jobName}'"""
            Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
                def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                s.size() == 1 && 'PAUSED' == s.get(0).get(0)
            })

            sql """
                ALTER JOB ${jobName} PROPERTIES (
                    "compute_group" = "${cgA}",
                    "session.cloud_cluster" = "${cgB}",
                    "offset" = '{"fileName":"regression/load/data/anoexist56789.csv"}'
                )
            """
            sql """RESUME JOB where jobname = '${jobName}'"""

            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
                cnt.size() == 1 && Integer.parseInt(cnt.get(0).get(0).toString()) >= 6
            })

            def aAfter3 = get_be_metric(beA.host, beA.httpPort, "load_rows") as long
            def bAfter3 = get_be_metric(beB.host, beB.httpPort, "load_rows") as long
            log.info("phase3 a=${aAfter3} b=${bAfter3}")
            assertTrue(aAfter3 > aAfter2, "phase3 expects cgA to increase (compute_group wins)")
            assertTrue(bAfter3 == bAfter2, "phase3 expects cgB untouched")
        } finally {
            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
            sql """drop table if exists `${tableName}` force"""
        }
    }
}
