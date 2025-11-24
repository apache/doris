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

suite("test_streaming_insert_job_alter") {
    def tableName = "test_streaming_insert_job_alter_tbl"
    def jobName = "test_streaming_insert_job_alter"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `c1` int NULL,
            `c2` varchar(2) NULL,
            `c3` int  NULL,
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Create a job, let it have data quality errors, modify the schema, and resume it
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
        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    log.info("check job status pause")
                    def jobSuccendCount = sql """ select status from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                    // check job status pause
                    jobSuccendCount.size() == 1 && 'PAUSED' == jobSuccendCount.get(0).get(0)
                }
        )
    } catch (Exception ex){
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
        log.info("show job: " + showjob)
        log.info("show task: " + showtask)
        throw ex;
    }

    def errorMsg = sql """
        select ErrorMsg from jobs("type"="insert") where Name='${jobName}'
    """
    assert errorMsg.get(0).get(0).contains("Insert has filtered data in strict mode")


    // alter fail job
    sql """
       ALTER JOB ${jobName}
       PROPERTIES(
        "session.enable_insert_strict" = false,
        "session.insert_max_filter_ratio" = 1
       )
        """

    def alterJobProperties = sql """
        select status,properties from jobs("type"="insert") where Name='${jobName}'
    """
    assert alterJobProperties.get(0).get(0) == "PAUSED"
    assert alterJobProperties.get(0).get(1) == "{\"session.enable_insert_strict\":\"false\",\"session.insert_max_filter_ratio\":\"1\"}"

    // modify column c2 to varchar(3), Exceeding the limit will be automatically truncated
    sql """
        ALTER TABLE ${tableName} MODIFY COLUMN c2 VARCHAR(3) NULL;
    """

    def descTbls = sql """Desc ${tableName}"""
    assert descTbls.size() == 3
    assert descTbls.get(1).get(0)  == "c2"
    assert descTbls.get(1).get(1)  == "varchar(3)"

    //resume job
    sql """
        RESUME JOB where jobname =  '${jobName}'
    """

    def resumeJobInfo = sql """
        select * from jobs("type"="insert") where Name='${jobName}'
    """
    log.info("resume jobInfo: " + resumeJobInfo)

    try {
        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def jobCountStatus = sql """ select status, SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                    log.info("check job status running: " + jobCountStatus)
                    // check job status running
                    jobCountStatus.size() == 1 && jobCountStatus.get(0).get(0) == 'RUNNING' && jobCountStatus.get(0).get(1) >= '1'
                }
        )
    } catch (Exception ex){
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
        log.info("show job: " + showjob)
        log.info("show task: " + showtask)
        throw ex;
    }

    def runningJobInfo = sql """
        select * from jobs("type"="insert") where Name='${jobName}'
    """
    log.info("running jobInfo: " + runningJobInfo)

    def jobOffset = sql """
        select currentOffset from jobs("type"="insert") where Name='${jobName}'
    """
    assert jobOffset.get(0).get(0) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";

    qt_select """ SELECT * FROM ${tableName} order by c1 """

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

    def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
    assert jobCountRsp.get(0).get(0) == 0

}
