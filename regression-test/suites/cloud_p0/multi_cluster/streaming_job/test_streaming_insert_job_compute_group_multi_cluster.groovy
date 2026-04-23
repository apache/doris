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

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_insert_job_compute_group_multi_cluster") {
    def tableName = "test_sij_cg_mc_tbl"
    def jobName   = "test_sij_cg_mc_job"

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',')
    for (String values : bes) {
        String[] beInfo = values.split(':')
        ipList.add(beInfo[0])
        hbPortList.add(beInfo[1])
        httpPortList.add(beInfo[2])
        beUniqueIdList.add(beInfo[3])
    }
    assert ipList.size() >= 2 : "need at least two BEs for multi-cluster test"

    for (unique_id : beUniqueIdList) {
        def resp = get_cluster.call(unique_id)
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id)
            }
        }
    }
    sleep(20000)

    def cgA = "sij_cg_a"
    def cgB = "sij_cg_b"
    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0], cgA, "sij_cg_a_id")
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1], cgB, "sij_cg_b_id")
    sleep(20000)

    def result = sql "show clusters"
    assert result.size() == 2

    sql "SET PROPERTY 'default_cloud_cluster' = ''"
    sql "use @${cgA}"

    try {
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

        def aBefore = get_be_metric(ipList[0], httpPortList[0], "load_rows") as long
        def bBefore = get_be_metric(ipList[1], httpPortList[1], "load_rows") as long
        log.info("a_before=${aBefore} b_before=${bBefore}")

        // Phase 1: bind to cluster A, consume example_[0-1].csv (2 files * 10 rows).
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

        def aAfterPhase1 = get_be_metric(ipList[0], httpPortList[0], "load_rows") as long
        def bAfterPhase1 = get_be_metric(ipList[1], httpPortList[1], "load_rows") as long
        log.info("phase1 a_after=${aAfterPhase1} b_after=${bAfterPhase1}")
        assertTrue(aAfterPhase1 > aBefore, "phase1 expects cluster A load_rows to increase")
        assertTrue(bAfterPhase1 == bBefore, "phase1 expects cluster B untouched")
        def rowsAfterPhase1 = (sql """SELECT count(*) FROM ${tableName}""").get(0).get(0) as long
        assertTrue(rowsAfterPhase1 > 0, "phase1 expects target table to receive rows")

        // Phase 2: pause, switch to cluster B, reset offset to re-consume the same files
        // on the new cluster (see test_streaming_insert_job_offset.groovy for the pattern).
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

        def aAfterPhase2 = get_be_metric(ipList[0], httpPortList[0], "load_rows") as long
        def bAfterPhase2 = get_be_metric(ipList[1], httpPortList[1], "load_rows") as long
        log.info("phase2 a_after=${aAfterPhase2} b_after=${bAfterPhase2}")
        assertTrue(aAfterPhase2 == aAfterPhase1, "phase2 expects cluster A load_rows unchanged")
        assertTrue(bAfterPhase2 > bAfterPhase1, "phase2 expects cluster B load_rows to increase")
        def rowsAfterPhase2 = (sql """SELECT count(*) FROM ${tableName}""").get(0).get(0) as long
        assertTrue(rowsAfterPhase2 > rowsAfterPhase1, "phase2 expects target table to receive more rows")

        // Phase 3: set session.cloud_cluster=cgB but compute_group=cgA at the same time.
        // compute_group must win over session.cloud_cluster; traffic stays on A.
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

        def aAfterPhase3 = get_be_metric(ipList[0], httpPortList[0], "load_rows") as long
        def bAfterPhase3 = get_be_metric(ipList[1], httpPortList[1], "load_rows") as long
        log.info("phase3 a_after=${aAfterPhase3} b_after=${bAfterPhase3}")
        assertTrue(aAfterPhase3 > aAfterPhase2, "phase3 expects cluster A load_rows to increase (compute_group wins)")
        assertTrue(bAfterPhase3 == bAfterPhase2, "phase3 expects cluster B untouched despite session.cloud_cluster=cgB")
    } finally {
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists `${tableName}` force"""
    }
}
