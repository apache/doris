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

suite("test_streaming_job_auto_resume", "nonConcurrent") {
    def tableName = "test_streaming_job_auto_resume_tbl"
    def jobName = "test_streaming_job_auto_resume"
    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
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

    try {
        GetDebugPoint().enableDebugPointForAllBEs("FlushToken.submit_flush_error")
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
            Awaitility.await().atMost(100, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def count = sql """ select status, FailedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='STREAMING' """
                        log.info("FailedTaskCount: " + count)
                        count.size() == 1 && '0' < count.get(1).get(0)
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("FlushToken.submit_flush_error")
    }

    // case1: test job fault recovery
    try {
        Awaitility.await().atMost(100, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='STREAMING' """
                    log.info("jobSuccendCount: " + jobSuccendCount)
                    // check job status and succeed task count larger than 2
                    jobSuccendCount.size() == 1 && '2' <= jobSuccendCount.get(0).get(0)
                }
        )
    } catch (Exception ex){
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
        log.info("show job: " + showjob)
        log.info("show task: " + showtask)
        throw ex;
    }

    // case2: test user pause will not auto resume
    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def count = 0
    while(count < 10) {
        def pausedJobStatus = sql """
            select status from jobs("type"="insert") where Name='${jobName}'
        """
        assert pausedJobStatus.get(0).get(0) == "PAUSED"
        sleep(1000)
        count++
    }
}