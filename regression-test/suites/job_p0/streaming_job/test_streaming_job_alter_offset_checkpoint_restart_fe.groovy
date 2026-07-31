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


import org.apache.doris.regression.suite.ClusterOptions
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

// Test that ALTER'd S3 offset survives FE checkpoint restart.
// Flow: consume data → PAUSE → ALTER offset → checkpoint → restart FE
// → verify offset preserved → RESUME → verify re-consumption from altered offset.
suite("test_streaming_job_alter_offset_checkpoint_restart_fe", "docker") {
    def tableName = "test_streaming_job_alter_ckpt_tbl"
    def jobName = "test_streaming_job_alter_ckpt_name"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null
    options.feConfigs += [
        'edit_log_roll_num=1'
    ]

    docker(options) {
        sql """drop table if exists `${tableName}` force"""
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

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

        // Create job with offset starting after example_0.csv
        sql """
           CREATE JOB ${jobName}
           PROPERTIES (
            'offset' = '{"fileName":"regression/load/data/example_0.csv"}'
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
                        def cnt = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSucceedCount: " + cnt)
                        cnt.size() == 1 && '1' <= cnt.get(0).get(0)
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        // Pause and ALTER offset to a position before all files
        sql """PAUSE JOB where jobname = '${jobName}'"""
        sql """
        ALTER JOB ${jobName}
        PROPERTIES (
            'offset' = '{"fileName":"regression/load/data/anoexist1234.csv"}'
        )
        """

        def alterInfo = sql """
            select currentOffset from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("after ALTER: " + alterInfo)
        assert alterInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/anoexist1234.csv\"}"

        // Wait for checkpoint to capture the altered offset in the image
        log.info("Waiting 90 seconds for checkpoint to complete...")
        sleep(90000)

        // Restart FE - recover from checkpoint image
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // Verify altered offset is preserved after checkpoint restart
        def afterRestart = sql """
            select status, currentOffset from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("after checkpoint restart: " + afterRestart)
        assert afterRestart.get(0).get(0) == "PAUSED"
        assert afterRestart.get(0).get(1) == "{\"fileName\":\"regression/load/data/anoexist1234.csv\"}"

        // Resume and verify job consumes all files from the altered offset
        sql """RESUME JOB where jobname = '${jobName}'"""
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def st = sql """ select status, SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobStatus: " + st)
                        st.size() == 1 && st.get(0).get(0) == 'RUNNING' && '2' <= st.get(0).get(1)
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        def finalInfo = sql """
            select currentOffset from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("final offset: " + finalInfo)
        assert finalInfo.get(0).get(0) == "{\"fileName\":\"regression/load/data/example_1.csv\"}"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists `${tableName}` force"""
    }
}
