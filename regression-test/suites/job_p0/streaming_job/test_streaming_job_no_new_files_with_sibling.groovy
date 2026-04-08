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

// Regression test for: streaming job second scheduling fails with "No new files found"
// when S3 listing returns non-matching sibling keys (e.g. example_1.csv) after the last
// matched file, causing currentMaxFile to be set to a non-matching raw S3 key.
//
// Pattern example_[0-0].csv matches only example_0.csv, but getLongestPrefix strips
// the bracket so S3 lists both example_0.csv and example_1.csv in the same page.
// Without the fix, currentMaxFile = "example_1.csv" triggers a second scheduling
// that finds no matching files and errors. With the fix, currentMaxFile = "example_0.csv"
// and hasMoreDataToConsume() correctly returns false.
suite("test_streaming_job_no_new_files_with_sibling") {
    def tableName = "test_streaming_job_no_new_files_with_sibling_tbl"
    def jobName = "test_streaming_job_no_new_files_with_sibling_job"

    sql """drop table if exists `${tableName}` force"""
    sql """
        DROP JOB IF EXISTS where jobname = '${jobName}'
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

    // Use example_[0-0].csv: glob matches only example_0.csv, but S3 listing prefix
    // "example_" also returns example_1.csv, which does not match the pattern.
    // This reproduces the "non-matching sibling key" scenario.
    sql """
        CREATE JOB ${jobName}
        ON STREAMING DO INSERT INTO ${tableName}
        SELECT * FROM S3
        (
            "uri" = "s3://${s3BucketName}/regression/load/data/example_[0-0].csv",
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
        // Wait for the first task to succeed
        Awaitility.await().atMost(120, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def res = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                    log.info("SucceedTaskCount: " + res)
                    res.size() == 1 && '1' <= res.get(0).get(0)
                }
        )

        // Wait extra time to allow a potential second (buggy) scheduling attempt
        Thread.sleep(10000)

        // Verify no failed tasks: the job should not have tried to re-schedule and
        // hit "No new files found" after all matched files are consumed.
        def jobStatus = sql """
            select Status, SucceedTaskCount, FailedTaskCount, ErrorMsg
            from jobs("type"="insert") where Name = '${jobName}'
        """
        log.info("jobStatus: " + jobStatus)
        assert jobStatus.get(0).get(2) == '0' : "Expected no failed tasks, but got: " + jobStatus
        assert jobStatus.get(0).get(0) != "STOPPED" : "Job should not be stopped, status: " + jobStatus

        qt_select """ SELECT * FROM ${tableName} order by c1 """

    } catch (Exception ex) {
        def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
        log.info("show job: " + showjob)
        log.info("show task: " + showtask)
        throw ex
    } finally {
        sql """
            DROP JOB IF EXISTS where jobname = '${jobName}'
        """
        sql """drop table if exists `${tableName}` force"""
    }
}
