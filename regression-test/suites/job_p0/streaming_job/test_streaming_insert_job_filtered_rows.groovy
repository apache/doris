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

// Regression for DORIS-25333: single-table S3 streaming job must accumulate
// filteredRows into jobStatistic. Previously the txn commit attachment /
// LoadStatistic / updateJobStatisticAndOffset chain had no filteredRows
// channel, so jobStatistic.filteredRows stayed at 0 even when BE had
// filtered rows out due to column type constraints.
suite("test_streaming_insert_job_filtered_rows") {
    def tableName = "test_streaming_insert_job_filtered_rows_tbl"
    def jobName = "test_streaming_insert_job_filtered_rows_name"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
    """

    // c2 is declared as INT NOT NULL, but the CSV has string names in that
    // column. Non-parseable int + NOT NULL forces BE to reject every row
    // (DPP_ABNORMAL_ALL). Mirrors the pattern in
    // test_streaming_mysql_job_errormsg.groovy (age int NOT NULL + 'abc').
    // Using varchar here would not work because non-strict mode truncates
    // silently instead of filtering.
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

    // Core assertion for the fix: filteredRows must be propagated end-to-end.
    // All 20 rows are filtered because c2 values (Emily/Benjamin/...) can't
    // parse as int and c2 is NOT NULL, so every row fails the NOT NULL check.
    assert loadStat.scannedRows == 20
    assert loadStat.fileNumber == 2
    assert loadStat.filteredRows == 20

    // Rows actually persisted = scanned - filtered = 0.
    def rowCount = sql """ select count(1) from ${tableName} """
    assert rowCount.get(0).get(0) == 0

    sql """ DROP JOB IF EXISTS where jobname =  '${jobName}' """
    sql """drop table if exists `${tableName}` force"""
}
