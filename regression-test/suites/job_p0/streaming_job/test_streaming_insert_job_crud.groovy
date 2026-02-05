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

suite("test_streaming_insert_job_crud") {
    def tableName = "test_streaming_insert_job_crud_tbl"
    def jobName = "test_streaming_insert_job_crud"
    def jobNameError = "test_streaming_insert_job_error"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobNameError}'
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

    /************************ create **********************/

    // create Illegal jobname
    expectExceptionLike({
        sql """
           CREATE JOB a+b
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
    }, "mismatched input")

    def jobCount = sql """ select count(1) from jobs("type"="insert") where Name = 'a+b' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    // create Illegal job params key
    expectExceptionLike({
        sql """
           CREATE JOB ${jobNameError}
           PROPERTIES(
            "xxx" = "sss"
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
    }, "Invalid properties: [xxx]")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    // create Illegal job params
    expectExceptionLike({
        sql """
           CREATE JOB ${jobNameError}
           PROPERTIES(
            "s3.max_batch_files" = "0"
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
    }, "s3.max_batch_files should >=1")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    expectExceptionLike({
        sql """
           CREATE JOB ${jobNameError}
           PROPERTIES(
            "s3.max_batch_bytes" = "102400"
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
    }, "s3.max_batch_bytes should between 100MB and 10GB")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    expectExceptionLike({
        sql """
           CREATE JOB ${jobNameError}
           PROPERTIES(
            "max_interval" = "-1"
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
    }, "max_interval should > 1")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    // create Illegal session params
    expectExceptionLike({
        sql """
           CREATE JOB ${jobNameError}
           PROPERTIES(
            "session.enable_insert_strict" = "xxx"
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
    }, "Variable enable_insert_strict can't be set to the value of xxx")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    expectExceptionLike({
        sql """
           CREATE JOB ${jobNameError}
           PROPERTIES(
            "session.xxx" = "abc"
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
    }, "Invalid session variable, [xxx]")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    // create error format url
    expectExceptionLike({
        sql """
       CREATE JOB ${jobNameError}
       ON STREAMING DO INSERT INTO ${tableName}
       SELECT * FROM S3
        (
            "uri" = "${s3BucketName}/regression/load/data/example_0.csv",
            "format" = "csv",
            "provider" = "${getS3Provider()}",
            "column_separator" = ",",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
        """
    }, "Invalid uri")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    // error sk
    expectExceptionLike({
        sql """
       CREATE JOB ${jobNameError}
       ON STREAMING DO INSERT INTO ${tableName}
       SELECT * FROM S3
        (
            "uri" = "s3://${s3BucketName}/regression/load/data/example_0.csv",
            "format" = "csv",
            "provider" = "${getS3Provider()}",
            "column_separator" = ",",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "xxxx"
        );
        """
    }, "Can not build s3")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    // no exist path
    expectExceptionLike({
        sql """
       CREATE JOB ${jobNameError}
       ON STREAMING DO INSERT INTO ${tableName}
       SELECT * FROM S3
        (
            "uri" = "s3://${s3BucketName}/regression/load/NoexistPath/Noexist.csv",
            "format" = "csv",
            "provider" = "${getS3Provider()}",
            "column_separator" = ",",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
        """
    }, "insert into cols should be corresponding to the query output")

    jobCount = sql """ select count(1) from jobs("type"="insert") where Name = '${jobNameError}' and ExecuteType='STREAMING' """
    assert jobCount.get(0).get(0) == 0

    /************************ select **********************/

    // select no exist job
    jobCount = sql """ select * from jobs("type"="insert") where Name = 'NoExistJob'"""
    assert jobCount.size() == 0


    /************************ alter **********************/

    // create normal streaming job
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
                    print("check success task count")
                    def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name like '%${jobName}%' and ExecuteType='STREAMING' """
                    // check job status and succeed task count larger than 2
                    jobSuccendCount.size() == 1 && '2' <= jobSuccendCount.get(0).get(0)
                }
        )
    } catch (Exception ex){
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
        println("show job: " + showjob)
        println("show task: " + showtask)
        throw ex;
    }

    def jobStatus = sql """
        select status from jobs("type"="insert") where Name='${jobName}'
    """
    assert jobStatus.get(0).get(0) == "RUNNING"

    def jobOffset = sql """
        select currentOffset, endoffset from jobs("type"="insert") where Name='${jobName}'
    """
    assert jobOffset.get(0).get(0) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";
    assert jobOffset.get(0).get(1) == "{\"fileName\":\"regression/load/data/example_1.csv\"}";

    qt_select """ SELECT * FROM ${tableName} order by c1 """

    // alter running job
    expectExceptionLike({
        sql """
       ALTER JOB ${jobName}
       PROPERTIES(
        "session.enable_insert_strict" = false
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
    }, "Only PAUSED job can be altered")

    // pause job
    sql """
        PAUSE JOB where jobname =  '${jobName}'
    """
    def pausedJobStatus = sql """
        select status from jobs("type"="insert") where Name='${jobName}'
    """
    assert pausedJobStatus.get(0).get(0) == "PAUSED"

    // alter no exist job
    expectExceptionLike({
        sql """
       ALTER JOB noExistJob
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
    }, "job not exist")

    // alter error properties
    test {
        sql """
           ALTER JOB ${jobName}
           PROPERTIES(
            "s3.max_batch_files" = "-1"
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
        exception "s3.max_batch_files should >=1"
    }

    // alter session var
    sql """
       ALTER JOB ${jobName}
       PROPERTIES(
        "session.enable_insert_strict" = false,
        "session.insert_max_filter_ratio" = 0.5,
        "s3.max_batch_files" = "10"
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
        select properties from jobs("type"="insert") where Name='${jobName}'
    """
    assert alterJobProperties.get(0).get(0) == "{\"s3.max_batch_files\":\"10\",\"session.enable_insert_strict\":\"false\",\"session.insert_max_filter_ratio\":\"0.5\"}"

    // alter url
    expectExceptionLike({
        sql """
       ALTER JOB ${jobName}
       INSERT INTO ${tableName}
       SELECT * FROM S3
        (
            "uri" = "s3://${s3BucketName}/regression/load/data/xxxx.csv",
            "format" = "csv",
            "provider" = "${getS3Provider()}",
            "column_separator" = ",",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
        """
    }, "The uri property cannot be modified in ALTER JOB")

    // alter target table
    expectExceptionLike({
        sql """
       ALTER JOB ${jobName}
       INSERT INTO NoExistTable123
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
    }, "The target table cannot be modified in ALTER JOB")

    /************************ delete **********************/
    // drop pause job
    pausedJobStatus = sql """
        select status from jobs("type"="insert") where Name='${jobName}'
    """
    assert pausedJobStatus.get(0).get(0) == "PAUSED"

    sql """ DROP JOB IF EXISTS where jobname =  '${jobName}' """

    def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
    assert jobCountRsp.get(0).get(0) == 0

    // drop no exist job
    expectExceptionLike({
        sql """ DROP JOB IF EXISTS where jobname =  'NoExistJob' """
    }, "unregister job error")

}
