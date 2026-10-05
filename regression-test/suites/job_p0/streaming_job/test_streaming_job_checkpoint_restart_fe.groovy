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

// Test that streaming job loadStatistic (fileNumber, fileSize, filteredRows)
// survives FE checkpoint restart. This guards against fields missing
// @SerializedName in StreamingJobStatistic being lost when the checkpoint
// image is written via GsonUtils.GSON (which uses HiddenAnnotationExclusionStrategy
// to skip fields without @SerializedName).
suite("test_streaming_job_checkpoint_restart_fe", "docker") {
    def tableName = "test_streaming_job_ckpt_restart_tbl"
    def jobName = "test_streaming_job_ckpt_restart_name"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null
    // Set edit_log_roll_num=1 so that every journal write triggers a roll,
    // ensuring finalized journals exist for the checkpoint thread to pick up.
    options.feConfigs += [
        'edit_log_roll_num=1'
    ]

    docker(options) {
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

        // Create streaming job
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
            // Wait for all files to be processed. With example_[0-1].csv and
            // s3.max_batch_files=1, processing completes in two successful tasks.
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSucceedCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='STREAMING' """
                        log.info("jobSucceedCount: " + jobSucceedCount)
                        jobSucceedCount.size() == 1 && '2' <= jobSucceedCount.get(0).get(0)
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        def jobInfo = sql """
            select currentOffset, endoffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo before checkpoint: " + jobInfo)
        def loadStatBefore = parseJson(jobInfo.get(0).get(2))
        log.info("loadStatistic before checkpoint: " + jobInfo.get(0).get(2))
        assert loadStatBefore.scannedRows > 0
        assert loadStatBefore.loadBytes > 0
        assert loadStatBefore.fileNumber > 0
        assert loadStatBefore.fileSize > 0

        // Wait for checkpoint thread to execute.
        // Checkpoint thread runs every 60 seconds (FeConstants.checkpoint_interval_second).
        // With edit_log_roll_num=1, journals are finalized immediately,
        // so the next checkpoint cycle will generate a new image containing
        // the streaming job state.
        log.info("Waiting 90 seconds for checkpoint to complete...")
        sleep(90000)

        // Restart FE - this time it should recover from the checkpoint image
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // Verify loadStatistic is preserved after checkpoint restart
        def jobInfoAfter = sql """
            select currentOffset, endoffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo after checkpoint restart: " + jobInfoAfter)
        def loadStatAfter = parseJson(jobInfoAfter.get(0).get(2))
        log.info("loadStatistic after checkpoint restart: " + jobInfoAfter.get(0).get(2))

        // These assertions verify that all statistics fields survive the checkpoint.
        // Without @SerializedName on these fields, GsonUtils.GSON (which uses
        // HiddenAnnotationExclusionStrategy) would skip them during serialization,
        // causing them to reset to 0 after loading from the checkpoint image.
        assert loadStatAfter.scannedRows == loadStatBefore.scannedRows
        assert loadStatAfter.loadBytes == loadStatBefore.loadBytes
        assert loadStatAfter.fileNumber == loadStatBefore.fileNumber
        assert loadStatAfter.fileSize == loadStatBefore.fileSize
        assert loadStatAfter.filteredRows == loadStatBefore.filteredRows

        sql """ DROP JOB IF EXISTS where jobname =  '${jobName}' """
        sql """drop table if exists `${tableName}` force"""
    }
}
