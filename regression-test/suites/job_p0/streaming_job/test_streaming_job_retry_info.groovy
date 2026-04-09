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

// Verify that RetryInfo and LastTaskSuccessTime TVF columns are populated correctly.
// 1. After a successful task, LastTaskSuccessTime should be set.
// 2. After failures and retries, RetryInfo should contain retryCount.
// 3. After manual resume, retryCount should be reset to 0.
suite("test_streaming_job_retry_info", "nonConcurrent") {
    def tableName = "test_streaming_job_retry_info_tbl"
    def jobName = "test_streaming_job_retry_info_job"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname = '${jobName}'
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `c1` int NULL,
            `c2` string NULL,
            `c3` int  NULL,
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // case1: Verify LastTaskSuccessTime is set after successful task
    sql """
       CREATE JOB ${jobName}
       PROPERTIES(
           "s3.max_batch_files" = "1"
       )
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

    try {
        // Wait for at least one successful task
        Awaitility.await().atMost(120, SECONDS)
                .pollInterval(2, SECONDS).until(
                {
                    def jobRes = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                    log.info("jobRes: " + jobRes)
                    jobRes.size() == 1 && '0' < jobRes.get(0).get(0)
                }
        )

        // Verify LastTaskSuccessTime is set and RetryInfo is empty
        def jobInfo = sql """select LastTaskSuccessTime, RetryInfo from jobs("type"="insert") where Name='${jobName}'"""
        log.info("jobInfo after success: " + jobInfo)
        assert jobInfo.get(0).get(0) != null && jobInfo.get(0).get(0) != "" : "LastTaskSuccessTime should be set after successful task"
        assert jobInfo.get(0).get(1) == null || jobInfo.get(0).get(1) == "" : "RetryInfo should be empty when no retries"

        // case2: Inject failure, verify RetryInfo is populated
        GetDebugPoint().enableDebugPointForAllBEs("FlushToken.submit_flush_error")
        try {
            Awaitility.await().atMost(120, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def jobRes = sql """ select Status, RetryInfo from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobRes waiting for RETRYING: " + jobRes)
                        jobRes.size() == 1 && 'RETRYING'.equals(jobRes.get(0).get(0))
                    }
            )

            // Verify RetryInfo contains retryCount
            def retryInfo = sql """select RetryInfo from jobs("type"="insert") where Name='${jobName}'"""
            log.info("retryInfo: " + retryInfo)
            assert retryInfo.get(0).get(0) != null && retryInfo.get(0).get(0) != "" : "RetryInfo should be set during RETRYING"
            assert retryInfo.get(0).get(0).contains("retryCount") : "RetryInfo should contain retryCount"
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("FlushToken.submit_flush_error")
        }

        // case3: Manual resume should reset retryCount
        sql """
            PAUSE JOB where jobname = '${jobName}'
        """
        def pausedStatus = sql """select Status from jobs("type"="insert") where Name='${jobName}'"""
        assert pausedStatus.get(0).get(0) == "PAUSED"

        sql """
            RESUME JOB where jobname = '${jobName}'
        """

        // Wait for job to run successfully again
        Awaitility.await().atMost(120, SECONDS)
                .pollInterval(2, SECONDS).until(
                {
                    def jobRes = sql """ select Status, SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                    log.info("jobRes after resume: " + jobRes)
                    jobRes.size() == 1 && 'RUNNING'.equals(jobRes.get(0).get(0))
                }
        )

        // Verify RetryInfo is empty after manual resume
        def jobInfoAfterResume = sql """select RetryInfo from jobs("type"="insert") where Name='${jobName}'"""
        log.info("jobInfo after resume: " + jobInfoAfterResume)
        assert jobInfoAfterResume.get(0).get(0) == null || jobInfoAfterResume.get(0).get(0) == "" : "RetryInfo should be empty after manual resume"

    } catch (Exception ex) {
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        log.info("show job: " + showjob)
        throw ex
    } finally {
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
