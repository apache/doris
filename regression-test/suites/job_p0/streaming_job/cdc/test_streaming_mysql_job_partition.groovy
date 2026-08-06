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

// MySQL RANGE-partitioned table cdc, symmetric to test_streaming_postgres_job_partition.
// binlog emits row events at the logical-table level, so INSERT/UPDATE/DELETE
// across partitions and ADD PARTITION + INSERT on the new partition all flow
// through normally. DROP PARTITION is intentionally not exercised here: it
// emits no row events, so the dropped rows would NOT be removed in Doris.
suite("test_streaming_mysql_job_partition", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_partition_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_mysql_orders"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // 1. create MySQL partitioned table and insert snapshot data
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""

            // MySQL requires the partition expression columns to be part of every
            // unique/primary key, so (id, order_date) is used as PK.
            sql """
                CREATE TABLE ${mysqlDb}.${table1} (
                    id BIGINT,
                    user_id BIGINT,
                    order_date DATE,
                    PRIMARY KEY (id, order_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8
                PARTITION BY RANGE COLUMNS(order_date) (
                    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
                    PARTITION p202402 VALUES LESS THAN ('2024-03-01')
                )
            """

            // snapshot rows, one per partition
            sql """INSERT INTO ${mysqlDb}.${table1} (id, user_id, order_date) VALUES (1, 1001, '2024-01-10')"""
            sql """INSERT INTO ${mysqlDb}.${table1} (id, user_id, order_date) VALUES (2, 1002, '2024-02-05')"""
        }

        // 2. create streaming job
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // wait snapshot to land
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 2
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // 3. check snapshot data
        qt_select_orders_partition_snapshot """
            SELECT id, user_id, order_date FROM ${table1} ORDER BY id
        """

        // 4. binlog phase: DML across partitions + ADD PARTITION + insert new partition
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            // insert into existing partition p202401
            sql """INSERT INTO ${mysqlDb}.${table1} (id, user_id, order_date) VALUES (3, 1003, '2024-01-20')"""

            // update in p202402
            sql """UPDATE ${mysqlDb}.${table1} SET user_id = 2002 WHERE id = 2 AND order_date = '2024-02-05'"""

            // delete from p202401
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE id = 1 AND order_date = '2024-01-10'"""

            // add a new partition then insert into it
            sql """ALTER TABLE ${mysqlDb}.${table1} ADD PARTITION (PARTITION p202403 VALUES LESS THAN ('2024-04-01'))"""
            sql """INSERT INTO ${mysqlDb}.${table1} (id, user_id, order_date) VALUES (4, 1004, '2024-03-15')"""
        }

        // wait for binlog to deliver: +2 inserts -1 delete -> 3 rows, id=2 user_id should be 2002
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def updated = sql """select user_id from ${currentDb}.${table1} where id=2"""
                        def deleted = sql """select count(1) from ${currentDb}.${table1} where id=1"""
                        def newPart = sql """select count(1) from ${currentDb}.${table1} where id=4"""
                        def u2 = updated.size() == 0 ? null : updated.get(0).get(0)
                        log.info("incr count=" + cnt + " id2.user_id=" + u2 + " id1.exists=" + deleted + " id4.exists=" + newPart)
                        cnt.get(0).get(0) == 3 &&
                                u2 != null && u2.toString() == '2002' &&
                                deleted.get(0).get(0) == 0 &&
                                newPart.get(0).get(0) == 1
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        def jobInfo = sql """
            select loadStatistic, status from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert jobInfo.get(0).get(1) == "RUNNING"

        qt_select_orders_partition_binlog_all """
            SELECT id, user_id, order_date FROM ${table1} ORDER BY id
        """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
