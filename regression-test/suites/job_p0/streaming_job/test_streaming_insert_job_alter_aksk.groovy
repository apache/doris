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

suite("test_streaming_insert_job_alter_aksk") {
    def tableName = "test_streaming_insert_job_alter_aksk_tbl"
    def jobName = "test_streaming_insert_job_alter_aksk"

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

    // Step 1: create job with correct aksk so that plan generation succeeds
    sql """
        CREATE JOB ${jobName}
        ON STREAMING DO INSERT INTO ${tableName}
        SELECT * FROM S3
        (
            "uri" = "s3://${s3BucketName}/regression/load/data/example_[0-1].csv",
            "format" = "csv",
            "provider" = "${getS3Provider()}",
            "column_separator" = ",",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
    """

    // Step 2: wait for at least one successful task to confirm the job works
    try {
        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def r = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
                    log.info("check job succeed task count: " + r)
                    r.size() == 1 && r.get(0).get(0) >= '1'
                }
        )
    } catch (Exception ex) {
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        log.info("show job: " + showjob)
        throw ex
    }

    // Step 3: pause the job before altering (ALTER requires PAUSED state).
    // Ignore errors in case the job is already paused (e.g. auto-paused after consuming all files).
    try {
        sql """PAUSE JOB where jobname = '${jobName}'"""
    } catch (Exception ignored) {
        log.info("PAUSE job got exception (may already be paused): " + ignored.getMessage())
    }
    Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
        def r = sql """select status from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
        r.size() == 1 && 'PAUSED' == r.get(0).get(0)
    })

    // Step 4: alter to wrong aksk while job is PAUSED.
    // originTvfProps must be refreshed by the fix so that fetchMeta picks up the
    // bad credentials after resume. Without the fix, originTvfProps would still
    // hold the old valid aksk and the job would keep running after resume.
    sql """
        ALTER JOB ${jobName}
        INSERT INTO ${tableName}
        SELECT * FROM S3
        (
            "uri" = "s3://${s3BucketName}/regression/load/data/example_[0-1].csv",
            "format" = "csv",
            "provider" = "${getS3Provider()}",
            "column_separator" = ",",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "wrong_ak_for_test",
            "s3.secret_key" = "wrong_sk_for_test"
        )
    """

    // Step 5: resume the job and wait for it to pause again due to fetchMeta failure
    sql """RESUME JOB where jobname = '${jobName}'"""
    try {
        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def r = sql """select status from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
                    log.info("check job status paused after altering to wrong aksk: " + r)
                    r.size() == 1 && 'PAUSED' == r.get(0).get(0)
                }
        )
    } catch (Exception ex) {
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        log.info("show job: " + showjob)
        throw ex
    }

    // Step 6: verify the pause was caused by fetchMeta failure, not other reasons
    def errorMsg = sql """select ErrorMsg from jobs("type"="insert") where Name='${jobName}'"""
    log.info("error msg after altering to wrong aksk: " + errorMsg)
    assert errorMsg.get(0).get(0).contains("Failed to fetch meta"),
        "Expected fetchMeta failure after alter to wrong aksk, got: " + errorMsg.get(0).get(0)

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

    def cnt = sql """select count(1) from jobs("type"="insert") where Name='${jobName}'"""
    assert cnt.get(0).get(0) == 0
}
