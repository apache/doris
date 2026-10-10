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

suite("test_streaming_insert_job_s3_once") {
    sql """DROP JOB IF EXISTS WHERE jobname = 'test_streaming_insert_job_s3_once'"""
    sql """DROP TABLE IF EXISTS test_streaming_insert_job_s3_once_tbl FORCE"""

    sql """
        CREATE TABLE test_streaming_insert_job_s3_once_tbl (
            `c1` INT NULL,
            `c2` STRING NULL,
            `c3` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`)
        DISTRIBUTED BY HASH(`c1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    test {
        sql """
            CREATE JOB test_streaming_insert_job_s3_once
            PROPERTIES (
                "s3.ingestion_mode" = "ONCE",
                "offset" = '{"fileName":"regression/load/data/example_0.csv"}'
            )
            ON STREAMING DO INSERT INTO test_streaming_insert_job_s3_once_tbl
            SELECT * FROM S3 (
                "uri" = "s3://${s3BucketName}/regression/load/data/example_[0-1].csv",
                "format" = "csv",
                "provider" = "${getS3Provider()}",
                "column_separator" = ",",
                "s3.endpoint" = "${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}"
            )
        """
        exception "offset is not supported when s3.ingestion_mode is ONCE"
    }

    sql """
        CREATE JOB test_streaming_insert_job_s3_once
        PROPERTIES (
            "s3.ingestion_mode" = "ONCE",
            "s3.max_batch_files" = "1"
        )
        ON STREAMING DO INSERT INTO test_streaming_insert_job_s3_once_tbl
        SELECT * FROM S3 (
            "uri" = "s3://${s3BucketName}/regression/load/data/example_[0-1].csv",
            "format" = "csv",
            "provider" = "${getS3Provider()}",
            "column_separator" = ",",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        )
    """

    try {
        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(1, SECONDS).until {
                    def job = sql """
                        SELECT Status, SucceedTaskCount
                        FROM jobs("type"="insert")
                        WHERE Name = 'test_streaming_insert_job_s3_once'
                          AND ExecuteType = 'STREAMING'
                    """
                    def rows = sql """SELECT COUNT(*) FROM test_streaming_insert_job_s3_once_tbl"""
                    log.info("S3 ONCE job: ${job}, row count: ${rows}")
                    job.size() == 1
                            && job.get(0).get(0) == "FINISHED"
                            && job.get(0).get(1).toString() == "2"
                            && rows.get(0).get(0).toString() == "20"
                }
    } catch (Exception ex) {
        def showJob = sql """
            SELECT * FROM jobs("type"="insert")
            WHERE Name = 'test_streaming_insert_job_s3_once'
        """
        def showTask = sql """
            SELECT * FROM tasks("type"="insert")
            WHERE JobName = 'test_streaming_insert_job_s3_once'
        """
        log.info("show job: " + showJob)
        log.info("show task: " + showTask)
        throw ex
    }
}
