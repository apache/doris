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

suite("test_streaming_insert_job") {
    def tableName = "test_streaming_insert_job_tbl"
    def jobName = "test_streaming_insert_job_name"

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

    def jobResult = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
    log.info("show success job: " + jobResult)

    qt_select """ SELECT * FROM ${tableName} order by c1 """

    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def pausedJobStatus = sql """
        select status, SucceedTaskCount + FailedTaskCount + CanceledTaskCount from jobs("type"="insert") where Name='${jobName}'
    """
    assert pausedJobStatus.get(0).get(0) == "PAUSED"

    def pauseShowTask = sql """select count(1) from tasks("type"="insert") where JobName='${jobName}'"""
    assert pauseShowTask.get(0).get(0) == pausedJobStatus.get(0).get(1)

    // check encrypt sk
    def jobExecuteSQL = sql """
        select ExecuteSql from jobs("type"="insert") where Name='${jobName}'
    """
    assert !jobExecuteSQL.get(0).get(0).contains("${getS3AK()}")
    assert !jobExecuteSQL.get(0).get(0).contains("${getS3SK()}")

    def jobInfo = sql """
        select currentOffset, endoffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'
    """
    log.info("jobInfo: " + jobInfo)
    assert jobInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
    assert jobInfo.get(0).get(1) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
    assert jobInfo.get(0).get(2) == "{\"scannedRows\":20,\"loadBytes\":425,\"fileNumber\":2,\"fileSize\":256}"

    def showTask = sql """select * from tasks("type"="insert") where jobName='${jobName}'"""
    log.info("showTask is : " + showTask )

    // check task show
    def taskInfo = sql """select Status,RunningOffset from tasks("type"="insert") where jobName='${jobName}'"""
    log.info("taskInfo is : " + taskInfo + ", size: " + taskInfo.size())
    assert taskInfo.size() > 0
    assert taskInfo.get(taskInfo.size()-1).get(0) == "SUCCESS"
    assert taskInfo.get(taskInfo.size()-1).get(1) == "{\"startFileName\":\"regression/load/data/example_0.csv\",\"endFileName\":\"regression/load/data/example_0.csv\"}"

    // alter streaming job
    sql """
       ALTER JOB ${jobName}
       PROPERTIES(
        "session.insert_max_filter_ratio" = 0.5
       )
       INSERT INTO ${tableName}
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

    def alterJobProperties = sql """
        select status,properties,currentOffset from jobs("type"="insert") where Name='${jobName}'
    """
    assert alterJobProperties.get(0).get(0) == "PAUSED"
    assert alterJobProperties.get(0).get(1) == "{\"s3.max_batch_files\":\"1\",\"session.insert_max_filter_ratio\":\"0.5\"}"
    assert alterJobProperties.get(0).get(2) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";

    sql """
        RESUME JOB where jobname =  '${jobName}'
    """
    def resumeJobStatus = sql """
        select status,properties,currentOffset, SucceedTaskCount + FailedTaskCount + CanceledTaskCount from jobs("type"="insert") where Name='${jobName}'
    """
    assert resumeJobStatus.get(0).get(0) == "RUNNING" || resumeJobStatus.get(0).get(0) == "PENDING"
    assert resumeJobStatus.get(0).get(1) == "{\"s3.max_batch_files\":\"1\",\"session.insert_max_filter_ratio\":\"0.5\"}"
    assert resumeJobStatus.get(0).get(2) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";


    Awaitility.await().atMost(60, SECONDS)
            .pollInterval(1, SECONDS).until(
            {
                print("check create streaming task count")
                def resumeShowTask = sql """select count(1) from tasks("type"="insert") where JobName='${jobName}'"""
                def lastTaskStatus = sql """select status from tasks("type"="insert") where JobName='${jobName}' limit 1 """
                // A new task is generated
                resumeShowTask.get(0).get(0) > resumeJobStatus.get(0).get(3) &&
                        ( lastTaskStatus.get(0).get(0) == "PENDING" || lastTaskStatus.get(0).get(0) == "RUNNING" )
            }
    )

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

    def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
    assert jobCountRsp.get(0).get(0) == 0
}
