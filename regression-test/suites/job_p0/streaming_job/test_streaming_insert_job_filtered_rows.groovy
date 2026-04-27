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

suite("test_streaming_insert_job_filtered_rows") {
    def tableName = "test_streaming_insert_job_filtered_rows_tbl"
    def jobName = "test_streaming_insert_job_filtered_rows_name"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

    // c2 INT NOT NULL forces BE to filter every row when CSV provides
    // non-parseable strings in that column.
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `c1` int NULL,
            `c2` int NOT NULL,
            `c3` int  NULL,
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // insert_max_filter_ratio=1 lets the task succeed even if every row is filtered.
    sql """
       CREATE JOB ${jobName}
       PROPERTIES(
        "s3.max_batch_files" = "1",
        "session.enable_insert_strict" = "false",
        "session.insert_max_filter_ratio" = "1"
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
                    def jobSucceedCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
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
    log.info("jobInfo: " + jobInfo)
    def loadStat = parseJson(jobInfo.get(0).get(2))
    log.info("loadStatistic: " + jobInfo.get(0).get(2))

    assert loadStat.scannedRows == 20
    assert loadStat.fileNumber == 2
    assert loadStat.filteredRows == 20

    def rowCount = sql """ select count(1) from ${tableName} """
    assert rowCount.get(0).get(0) == 0

    sql """ DROP JOB IF EXISTS where jobname =  '${jobName}' """
    sql """drop table if exists `${tableName}` force"""
}
