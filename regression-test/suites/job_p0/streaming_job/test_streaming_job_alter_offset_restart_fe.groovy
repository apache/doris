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

suite("test_streaming_job_alter_offset_restart_fe", "docker") {
    def tableName = "test_streaming_job_alter_offset_restart_fe_tbl"
    def jobName = "test_streaming_job_alter_offset_restart_fe_name"

    def options = new ClusterOptions()
    options.setFeNum(1)
    // run in cloud and not cloud
    options.cloudMode = null
    
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

        // create streaming job
        sql """
           CREATE JOB ${jobName}  
           PROPERTIES (
            'offset' = '{"fileName":"regression/load/data/example_0.csv"}'
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
            // Wait for streaming job to process some data
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        // check job status and succeed task count larger than 1
                        jobSuccendCount.size() == 1 && '1' <= jobSuccendCount.get(0).get(0)
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
        log.info("jobInfo: " + jobInfo)
        assert jobInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
        assert jobInfo.get(0).get(1) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
        def loadStat = parseJson(jobInfo.get(0).get(2))
        assert loadStat.scannedRows == 10
        assert loadStat.loadBytes == 218
        assert loadStat.fileNumber == 1
        assert loadStat.fileSize == 138

        sql """
            PAUSE JOB where jobname =  '${jobName}'
        """

        // alter offset
        sql """
        ALTER JOB ${jobName} 
        PROPERTIES (
            'offset' = '{"fileName":"regression/load/data/anoexist1234.csv"}'
        )
        """

        jobInfo = sql """
        select currentOffset, loadStatistic, properties from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert jobInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/anoexist1234.csv\"}";
        def loadStat1 = parseJson(jobInfo.get(0).get(1))
        assert loadStat1.scannedRows == 10
        assert loadStat1.loadBytes == 218
        assert loadStat1.fileNumber == 1
        assert loadStat1.fileSize == 138
        assert jobInfo.get(0).get(2) == "{\"offset\":\"{\\\"fileName\\\":\\\"regression/load/data/anoexist1234.csv\\\"}\"}"

        // Restart FE
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // check is it consistent after restart
        def jobStatus = sql """
            select status, SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobstatus: " + jobStatus)
        assert jobStatus.get(0).get(0) == "PAUSED"
        jobInfo = sql """
        select currentOffset, loadStatistic, properties from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert jobInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/anoexist1234.csv\"}";
        def loadStat2 = parseJson(jobInfo.get(0).get(1))
        assert loadStat2.scannedRows == 10
        assert loadStat2.loadBytes == 218
        assert loadStat2.fileNumber == 1
        assert loadStat2.fileSize == 138
        assert jobInfo.get(0).get(2) == "{\"offset\":\"{\\\"fileName\\\":\\\"regression/load/data/anoexist1234.csv\\\"}\"}"

        // resume to check whether consumption will resume
        sql """
        RESUME JOB where jobname = '${jobName}'
        """
        // wait to running
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        jobStatus = sql """ select status, SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobStatus: " + jobStatus)
                        // check job status and succeed task count larger than 1
                        jobStatus.size() == 1 && jobStatus.get(0).get(0) == 'RUNNING' && '2' <= jobStatus.get(0).get(1)
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        jobInfo = sql """
        select currentOffset, endoffset, loadStatistic, properties from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert jobInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
        assert jobInfo.get(0).get(1) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
        def loadStat3 = parseJson(jobInfo.get(0).get(2))
        assert loadStat3.scannedRows == 30
        assert loadStat3.loadBytes == 643
        assert loadStat3.fileNumber == 3
        assert loadStat3.fileSize == 394
        assert jobInfo.get(0).get(3) == "{\"offset\":\"{\\\"fileName\\\":\\\"regression/load/data/anoexist1234.csv\\\"}\"}"

        sql """ DROP JOB IF EXISTS where jobname =  '${jobName}' """
        sql """drop table if exists `${tableName}` force"""
    }
}