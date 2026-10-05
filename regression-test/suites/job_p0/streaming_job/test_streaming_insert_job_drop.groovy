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

suite("test_streaming_insert_job_drop") {
    def tableName = "test_streaming_insert_job_tbl_drop"
    def jobName = "test_streaming_insert_job_name_drop"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    def dropDatabase = "test_streaming_insert_job_drop_db"
    sql """create database if not exists ${dropDatabase}"""
    sql """use ${dropDatabase}"""

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

    // drop dataabse
    sql """drop database ${dropDatabase} force"""

    // drop job
    sql """
        DROP JOB IF EXISTS where jobname = '${jobName}'
    """

    def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
    assert jobCountRsp.get(0).get(0) == 0
}
