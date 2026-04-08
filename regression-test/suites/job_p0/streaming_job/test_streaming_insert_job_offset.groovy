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

suite("test_streaming_insert_job_offset") {
    def tableName = "test_streaming_insert_job_offset_tbl"
    def jobName = "test_streaming_insert_job_offset_name"
    def database = sql "select database()"
    log.info("current database is " + database)
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

    // create streaming job with error offset
    // empty offset
    test {
        sql """
       CREATE JOB ${jobName}  
       PROPERTIES(
        "offset" = ""
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
        exception "Invalid format for offset"
    }

    // illegal offset
    test {
        sql """
       CREATE JOB ${jobName}  
       PROPERTIES(
        "offset" = "xxxx"
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
        exception "Failed to initialize offset"
    }

    // illegal offset key
    test {
        sql """
           CREATE JOB ${jobName}  
           PROPERTIES(
            "offset" = '{"errorkey":"1.csv"}'
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
        exception "Invalid format for offset"
    }

    // create streaming job with normal offset
    sql """
       CREATE JOB ${jobName}  
       PROPERTIES(
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
        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                    log.info("jobSuccendCount: " + jobSuccendCount)
                    // check job status and succeed task count larger than 1
                    jobSuccendCount.size() == 1 && '1' <= jobSuccendCount.get(0).get(0)
                }
        )
    } catch (Exception ex){
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
        log.info("show job: " + showjob)
        log.info("show task: " + showtask)
        throw ex;
    }

    def jobResult = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
    log.info("show success job: " + jobResult)

    // only has example_1.csv data
    qt_select """ SELECT * FROM ${tableName} order by c1 """

    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def pausedJobStatus = sql """
        select status, SucceedTaskCount + FailedTaskCount + CanceledTaskCount  from jobs("type"="insert") where Name='${jobName}'
    """
    assert pausedJobStatus.get(0).get(0) == "PAUSED"

    def pauseShowTask = sql """select count(1) from tasks("type"="insert") where JobName='${jobName}'"""
    assert pauseShowTask.get(0).get(0) == pausedJobStatus.get(0).get(1)

    def jobInfo = sql """
        select currentOffset, endoffset, loadStatistic, properties from jobs("type"="insert") where Name='${jobName}'
    """
    log.info("jobInfo: " + jobInfo)
    assert jobInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
    assert jobInfo.get(0).get(1) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
    def loadStat = parseJson(jobInfo.get(0).get(2))
    assert loadStat.scannedRows == 10
    assert loadStat.loadBytes == 218
    assert loadStat.fileNumber == 1
    assert loadStat.fileSize == 138
    assert jobInfo.get(0).get(3) == "{\"offset\":\"{\\\"fileName\\\":\\\"regression/load/data/example_0.csv\\\"}\"}"

    // alter job init offset, Lexicographic order includes example_[0-1]
    sql """
        ALTER JOB ${jobName} 
        PROPERTIES (
            'offset' = '{"fileName":"regression/load/data/anoexist1234.csv"}'
        )
    """
    sql """
        RESUME JOB where jobname =  '${jobName}'
    """

    try {
        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def jobStatus = sql """ select status, SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
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
    def loadStat2 = parseJson(jobInfo.get(0).get(2))
    assert loadStat2.scannedRows == 30
    assert loadStat2.loadBytes == 643
    assert loadStat2.fileNumber == 3
    assert loadStat2.fileSize == 394
    assert jobInfo.get(0).get(3) == "{\"offset\":\"{\\\"fileName\\\":\\\"regression/load/data/anoexist1234.csv\\\"}\"}"

    // has double example_1.csv and example_0.csv data
    qt_select """ SELECT * FROM ${tableName} order by c1 """

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

    def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
    assert jobCountRsp.get(0).get(0) == 0
}
