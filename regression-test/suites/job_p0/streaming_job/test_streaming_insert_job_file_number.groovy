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

// fileNumber must be physical file count, not BE split count. Tiny split_size forces multi-split per file.
suite("test_streaming_insert_job_file_number") {
    def tableName = "test_streaming_insert_job_file_number_tbl"
    def jobName = "test_streaming_insert_job_file_number_name"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `c1` int NULL,
            `c2` string NULL,
            `c3` int  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """
       CREATE JOB ${jobName}
       PROPERTIES(
        "s3.max_batch_files" = "2",
        "session.max_initial_file_split_size" = "50"
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

    sql """ PAUSE JOB where jobname =  '${jobName}' """

    def jobInfo = sql """
        select loadStatistic from jobs("type"="insert") where Name='${jobName}'
    """
    log.info("jobInfo: " + jobInfo)
    def loadStat = parseJson(jobInfo.get(0).get(0))
    assert loadStat.scannedRows == 20
    assert loadStat.loadBytes == 425
    assert loadStat.fileSize == 256
    // Without fix this is BE split count (6); with fix it's physical file count (2).
    assert loadStat.fileNumber == 2

    def taskInfo = sql """
        select Status, LoadStatistic from tasks("type"="insert") where JobName='${jobName}'
    """
    log.info("taskInfo: " + taskInfo)
    assert taskInfo.size() > 0
    def lastTask = taskInfo.get(taskInfo.size() - 1)
    assert lastTask.get(0) == "SUCCESS"
    def taskStat = parseJson(lastTask.get(1))
    assert taskStat.FileNumber == 2

    qt_select """ SELECT * FROM ${tableName} order by c1 """

    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """
    sql """drop table if exists `${tableName}` force"""
}
