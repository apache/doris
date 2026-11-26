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

suite("test_streaming_job_restart_fe", "docker") {
    def tableName = "test_streaming_job_restart_fe_tbl"
    def jobName = "test_streaming_job_restart_fe_name"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null
    
    docker(options) {
        def bucket = getS3BucketName()
        def prefix = "regression/load/streaming_job/${jobName}/${UUID.randomUUID()}"
        def s3Client = getS3Client()
        (0..1).each { index ->
            s3Client.copyObject(bucket, "regression/load/data/example_${index}.csv",
                    bucket, "${prefix}/example_${index}.csv")
        }
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

        // create streaming job
        sql """
           CREATE JOB ${jobName}  
           PROPERTIES(
            "s3.max_batch_files" = "1"
           )
           ON STREAMING DO INSERT INTO ${tableName} 
           SELECT * FROM S3
            (
                "uri" = "s3://${bucket}/${prefix}/example_*.csv",
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
            // Wait for streaming job to process some data
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        jobSuccendCount.size() == 1 && (jobSuccendCount[0][0] as long) >= 2
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        // Capture LastTaskSuccessTime before restart to verify it survives editlog replay.
        def lastSuccessBefore = sql """
            select LastTaskSuccessTime from jobs("type"="insert") where Name='${jobName}'
        """
        assert lastSuccessBefore.get(0).get(0) != null && lastSuccessBefore.get(0).get(0) != "",
                "LastTaskSuccessTime should be set before restart"

        sql """
            PAUSE JOB where jobname =  '${jobName}'
        """
        sql """
            RESUME JOB where jobname =  '${jobName}'
        """
        def jobInfo = sql """
            select currentOffset, endoffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert parseJson(jobInfo[0][0]).fileName == "${prefix}/example_1.csv"
        def loadStat = parseJson(jobInfo.get(0).get(2))
        assert loadStat.scannedRows == 20
        assert loadStat.loadBytes == 425
        assert loadStat.fileNumber == 2
        assert loadStat.fileSize == 256

        Awaitility.await().pollInSameThread().atMost(300, SECONDS).pollInterval(1, SECONDS).until {
            sql("SELECT COUNT(*) FROM ${tableName}")[0][0] == 20
        }
        order_qt_before_restart "SELECT * FROM ${tableName} ORDER BY c1, c2, c3"

        // Restart FE
        cluster.restartFrontends()
        Awaitility.await().pollInSameThread().atMost(120, SECONDS)
                .pollInterval(1, SECONDS).ignoreExceptions().until {
                    context.reconnectFe()
                    sql("SELECT 1")[0][0] == 1
                }

        Awaitility.await().pollInSameThread().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            def jobStatus = sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
            jobStatus.size() == 1 && jobStatus[0][0] in ["RUNNING", "PENDING"]
        }
        jobInfo = sql """
            select currentOffset, endoffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert parseJson(jobInfo[0][0]).fileName == "${prefix}/example_1.csv"
        def loadStatAfter = parseJson(jobInfo.get(0).get(2))
        assert loadStatAfter.scannedRows == 20
        assert loadStatAfter.loadBytes == 425
        assert loadStatAfter.fileNumber == 2
        assert loadStatAfter.fileSize == 256

        // Verify LastTaskSuccessTime survived the FE restart (replayOnUpdated must copy it).
        def lastSuccessAfter = sql """
            select LastTaskSuccessTime from jobs("type"="insert") where Name='${jobName}'
        """
        assert lastSuccessAfter.get(0).get(0) != null && lastSuccessAfter.get(0).get(0) != "",
                "LastTaskSuccessTime should survive FE restart, got: " + lastSuccessAfter.get(0).get(0)

        order_qt_after_restart "SELECT * FROM ${tableName} ORDER BY c1, c2, c3"

        s3Client.putObject(bucket, "${prefix}/example_2.csv", "10001,after_restart,10001\n")
        Awaitility.await().pollInSameThread().atMost(300, SECONDS).pollInterval(1, SECONDS).until {
            def job = sql """SELECT CurrentOffset FROM jobs("type"="insert") WHERE Name='${jobName}'"""
            job.size() == 1 && parseJson(job[0][0]).fileName == "${prefix}/example_2.csv" &&
                    sql("SELECT COUNT(*) FROM ${tableName}")[0][0] == 21
        }
        order_qt_new_data "SELECT * FROM ${tableName} ORDER BY c1, c2, c3"

        sql """ DROP JOB IF EXISTS where jobname =  '${jobName}' """
    }
}
