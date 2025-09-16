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


    // create recurring job
    sql """
       CREATE JOB ${jobName}  ON STREAMING DO INSERT INTO ${tableName} 
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
    Awaitility.await().atMost(30, SECONDS).until(
            {
                print("check success task count")
                def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='STREAMING' """
                // check job status and succeed task count larger than 1
                jobSuccendCount.size() == 1 && '1' <= jobSuccendCount.get(0).get(0)
            }
    )

    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def pausedJobStatus = sql """
        select status from jobs("type"="insert") where Name='${jobName}'
    """
    assert pausedJobStatus.get(0).get(0) == "PAUSED"

    qt_select """ SELECT * FROM ${tableName} order by c1 """

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

    def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
    assert jobCountRsp.get(0).get(0) == 0

}
