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

// Verify that when fetchRemoteMeta receives a failed GlobListResult (e.g. S3 auth error),
// the streaming job is correctly PAUSED rather than hanging indefinitely.
suite("test_streaming_insert_job_fetch_meta_error", "nonConcurrent") {
    def tableName = "test_streaming_insert_job_fetch_meta_error_tbl"
    def jobName = "test_streaming_insert_job_fetch_meta_error_job"

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

    GetDebugPoint().enableDebugPointForAllFEs("S3SourceOffsetProvider.fetchRemoteMeta.error")
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

        try {
            Awaitility.await().atMost(120, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def jobRes = sql """ select Status from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobRes: " + jobRes)
                        jobRes.size() == 1 && 'PAUSED'.equals(jobRes.get(0).get(0))
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        def jobStatus = sql """select Status, ErrorMsg from jobs("type"="insert") where Name='${jobName}'"""
        assert jobStatus.get(0).get(0) == "PAUSED"
        assert jobStatus.get(0).get(1).contains("simulated S3 auth error")

        sql """
            DROP JOB IF EXISTS where jobname = '${jobName}'
        """

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0

    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("S3SourceOffsetProvider.fetchRemoteMeta.error")
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
