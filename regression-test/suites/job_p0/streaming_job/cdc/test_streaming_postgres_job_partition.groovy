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

suite("test_streaming_postgres_job_partition", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_partition_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_orders" 
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // 1. create postgres partition table and insert snapshot data
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgSchema}.${table1}"""

            sql """
                CREATE TABLE ${pgSchema}.${table1} (
                    id BIGINT,
                    user_id BIGINT,
                    order_date DATE,
                    PRIMARY KEY (id, order_date)
                ) PARTITION BY RANGE (order_date)
            """

            // create two partitions: 2024-01, 2024-02
            sql """CREATE TABLE ${table1}_p202401 PARTITION OF ${pgSchema}.${table1}
                   FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')"""
            sql """CREATE TABLE ${table1}_p202402 PARTITION OF ${pgSchema}.${table1}
                   FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')"""

            // make snapshot data, insert into two partitions
            sql """INSERT INTO ${pgSchema}.${table1} (id, user_id, order_date) VALUES (1, 1001, DATE '2024-01-10');"""
            sql """INSERT INTO ${pgSchema}.${table1} (id, user_id, order_date) VALUES (2, 1002, DATE '2024-02-05');"""
        }

        // 2. create streaming job
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // wait snapshot data sync completed
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert")
                                                     where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
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

        // 3. check snapshot data
        qt_select_orders_partition_snapshot """
            SELECT id, user_id, order_date
            FROM ${table1}
            ORDER BY id
        """

        // 4. mock insert, update, delete and create new partition
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // insert
            sql """INSERT INTO ${pgSchema}.${table1} (id, user_id, order_date)
                   VALUES (3, 1003, DATE '2024-01-20');"""
		
            def xminResult = sql """SELECT xmin, xmax , * FROM ${pgSchema}.${table1} WHERE id = 3"""
            log.info("xminResult: " + xminResult)

            // update
            sql """UPDATE ${pgSchema}.${table1}
                   SET user_id = 2002
                   WHERE id = 2 AND order_date = DATE '2024-02-05';"""

            // delete
            sql """DELETE FROM ${pgSchema}.${table1}
                   WHERE id = 1 AND order_date = DATE '2024-01-10';"""

            // create new partition and insert data
            sql """CREATE TABLE ${table1}_p202403 PARTITION OF ${pgSchema}.${table1}
                   FOR VALUES FROM ('2024-03-01') TO ('2024-04-01')"""

            sql """INSERT INTO ${pgSchema}.${table1} (id, user_id, order_date)
                   VALUES (4, 1004, DATE '2024-03-15');"""

            def xminResult1 = sql """SELECT xmin, xmax , * FROM ${pgSchema}.${table1} WHERE id = 4"""
            log.info("xminResult1: " + xminResult1)
        }

        // wait for all incremental data 
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert")
                                                     where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        jobSuccendCount.size() == 1 && '3' <= jobSuccendCount.get(0).get(0)
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        def jobInfo = sql """
            select loadStatistic, status from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert jobInfo.get(0).get(1) == "RUNNING"

        // check binlog data
        qt_select_orders_partition_binlog_all """
            SELECT id, user_id, order_date
            FROM ${table1}
            ORDER BY id
        """

        sql """ DROP JOB IF EXISTS where jobname =  '${jobName}' """
        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}

