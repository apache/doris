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

// Verify that after exceeding streaming_job_max_auto_resume_count, the streaming
// job's failure reason is rewritten to CANNOT_RESUME_ERR so the auto-resume
// handler stops trying. The job stays in PAUSED but effectively requires a
// manual RESUME to try again.
suite("test_streaming_job_max_retry", "nonConcurrent") {
    def tableName = "test_streaming_job_max_retry_tbl"
    def jobName = "test_streaming_job_max_retry_job"

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

    // Capture the original value so other suites (or a non-default env) are not clobbered.
    def originalMaxRetry = sql """ADMIN SHOW FRONTEND CONFIG LIKE 'streaming_job_max_auto_resume_count'"""
    def originalMaxRetryValue = originalMaxRetry.get(0).get(1)
    // Shrink the retry budget so the test finishes quickly.
    sql "ADMIN SET FRONTEND CONFIG ('streaming_job_max_auto_resume_count' = '3')"

    GetDebugPoint().enableDebugPointForAllFEs('StreamingJob.scheduleTask.exception')
    try {
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

        // Wait until the retry budget is exhausted and the failure reason
        // is rewritten to CANNOT_RESUME_ERR. Status stays PAUSED throughout.
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def jobRes = sql """ select Status, ErrorMsg from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobRes waiting for CANNOT_RESUME_ERR: " + jobRes)
                        if (jobRes.size() != 1 || !'PAUSED'.equals(jobRes.get(0).get(0))) {
                            return false
                        }
                        def errMsg = jobRes.get(0).get(1) as String
                        return errMsg != null && errMsg.contains("CANNOT_RESUME_ERR")
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            log.info("show job: " + showjob)
            throw ex
        }

        // ErrorMsg is a FailureReason JSON blob containing both code and msg,
        // so the frontend can switch on the structured code instead of grepping the message.
        def jobInfo = sql """select Status, ErrorMsg from jobs("type"="insert") where Name='${jobName}'"""
        assert jobInfo.get(0).get(0) == "PAUSED"
        def errorMsgJson = jobInfo.get(0).get(1) as String
        assert errorMsgJson.contains("\"code\"") && errorMsgJson.contains("\"msg\""),
                "ErrorMsg should be a FailureReason JSON with code and msg fields, got: " + errorMsgJson
        assert errorMsgJson.contains("CANNOT_RESUME_ERR"),
                "ErrorMsg should contain CANNOT_RESUME_ERR code, got: " + errorMsgJson
        assert errorMsgJson.contains("Auto resume failed after"),
                "ErrorMsg should contain the burn-out message, got: " + errorMsgJson

    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('StreamingJob.scheduleTask.exception')
        sql "ADMIN SET FRONTEND CONFIG ('streaming_job_max_auto_resume_count' = '${originalMaxRetryValue}')"
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
