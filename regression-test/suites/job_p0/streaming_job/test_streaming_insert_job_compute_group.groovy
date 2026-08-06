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

suite("test_streaming_insert_job_compute_group") {
    def tableName = "test_streaming_insert_job_cg_tbl"
    def jobName = "test_streaming_insert_job_cg_job"

    sql """drop table if exists `${tableName}` force"""
    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `c1` int NULL,
            `c2` string NULL,
            `c3` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`)
        DISTRIBUTED BY HASH(`c1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
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

    // ---------- Non-cloud mode: compute_group must be rejected ----------
    if (!isCloudMode()) {
        test {
            sql """
                CREATE JOB ${jobName}
                PROPERTIES ("compute_group" = "any_group")
                ON STREAMING DO INSERT INTO ${tableName} ${s3Source};
            """
            exception "only supported in cloud mode"
        }
        return
    }

    // ---------- Cloud mode ----------
    def clusterRows = sql "show clusters"
    assert clusterRows.size() >= 1 : "cloud mode expects at least one cluster"
    def cg = clusterRows.get(0).get(0)

    try {
        // 0) Empty compute_group -> CREATE rejected
        test {
            sql """
                CREATE JOB ${jobName}
                PROPERTIES ("compute_group" = "")
                ON STREAMING DO INSERT INTO ${tableName} ${s3Source};
            """
            exception "compute_group cannot be empty"
        }

        // 1) Invalid compute_group -> CREATE should fail
        test {
            sql """
                CREATE JOB ${jobName}
                PROPERTIES (
                    "s3.max_batch_files" = "1",
                    "compute_group" = "__not_exist_cg__"
                )
                ON STREAMING DO INSERT INTO ${tableName} ${s3Source};
            """
            exception "not exist"
        }

        // 2) Valid compute_group -> CREATE succeeds; properties reflect it
        sql """
            CREATE JOB ${jobName}
            PROPERTIES (
                "s3.max_batch_files" = "1",
                "compute_group" = "${cg}"
            )
            ON STREAMING DO INSERT INTO ${tableName} ${s3Source};
        """

        def props = sql """select properties from jobs("type"="insert") where Name='${jobName}'"""
        log.info("job properties: " + props)
        assert props.get(0).get(0).contains("\"compute_group\":\"${cg}\"")

        // Wait for at least one successful task so the cluster binding is exercised end-to-end
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
                cnt.size() == 1 && Integer.parseInt(cnt.get(0).get(0).toString()) >= 1
            })
        } catch (Exception ex) {
            log.info("job: " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("task: " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // 3) ALTER without PAUSE -> rejected by upstream guard (Only PAUSED job can be altered)
        test {
            sql """ALTER JOB ${jobName} PROPERTIES ("compute_group" = "${cg}")"""
            exception "Only PAUSED job can be altered"
        }

        sql """PAUSE JOB where jobname = '${jobName}'"""
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            s.size() == 1 && 'PAUSED' == s.get(0).get(0)
        })

        // 4) ALTER to non-existent cluster -> rejected; state + bound cg unchanged
        test {
            sql """ALTER JOB ${jobName} PROPERTIES ("compute_group" = "__not_exist_cg__")"""
            exception "not exist"
        }
        def afterBadAlter = sql """select status, properties from jobs("type"="insert") where Name='${jobName}'"""
        assert afterBadAlter.get(0).get(0) == "PAUSED"
        assert afterBadAlter.get(0).get(1).contains("\"compute_group\":\"${cg}\"")

        // 5) ALTER with empty compute_group -> rejected; bound cg unchanged
        test {
            sql """ALTER JOB ${jobName} PROPERTIES ("compute_group" = "")"""
            exception "compute_group cannot be empty"
        }
        def afterEmptyAlter = sql """select properties from jobs("type"="insert") where Name='${jobName}'"""
        assert afterEmptyAlter.get(0).get(0).contains("\"compute_group\":\"${cg}\"")

        // 6) ALTER to the same valid cluster -> succeeds, properties updated
        sql """ALTER JOB ${jobName} PROPERTIES ("compute_group" = "${cg}")"""
        def afterAlter = sql """select status, properties from jobs("type"="insert") where Name='${jobName}'"""
        assert afterAlter.get(0).get(0) == "PAUSED"
        assert afterAlter.get(0).get(1).contains("\"compute_group\":\"${cg}\"")
    } finally {
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists `${tableName}` force"""
    }
}
