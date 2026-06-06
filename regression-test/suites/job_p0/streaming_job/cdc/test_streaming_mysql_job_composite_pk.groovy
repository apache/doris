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

suite("test_streaming_mysql_job_composite_pk", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_composite_pk_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_composite_pk_mysql"
    def table2 = "streaming_full_pk_map_mysql"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""
    sql """drop table if exists ${currentDb}.${table2} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // ===== Prepare MySQL side =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """
            create table ${mysqlDb}.${table1} (
              `tenant_id` int not null,
              `order_id` bigint not null,
              `order_no` varchar(64),
              `amount` decimal(10,2),
              primary key (`tenant_id`, `order_id`)
            ) engine=innodb default charset=utf8;
            """

            // Snapshot rows: 2 tenants x 5 orders each.
            sql """insert into ${mysqlDb}.${table1} values (1, 1001, 'A_001', 10.00)"""
            sql """insert into ${mysqlDb}.${table1} values (1, 1002, 'A_002', 20.00)"""
            sql """insert into ${mysqlDb}.${table1} values (1, 1003, 'A_003', 30.00)"""
            sql """insert into ${mysqlDb}.${table1} values (1, 1004, 'A_004', 40.00)"""
            sql """insert into ${mysqlDb}.${table1} values (1, 1005, 'A_005', 50.00)"""
            sql """insert into ${mysqlDb}.${table1} values (2, 2001, 'B_001', 100.00)"""
            sql """insert into ${mysqlDb}.${table1} values (2, 2002, 'B_002', 200.00)"""
            sql """insert into ${mysqlDb}.${table1} values (2, 2003, 'B_003', 300.00)"""
            sql """insert into ${mysqlDb}.${table1} values (2, 2004, 'B_004', 400.00)"""
            sql """insert into ${mysqlDb}.${table1} values (2, 2005, 'B_005', 500.00)"""

            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table2}"""
            sql """
            create table ${mysqlDb}.${table2} (
              `user_id` int not null,
              `role_id` int not null,
              primary key (`user_id`, `role_id`)
            ) engine=innodb default charset=utf8;
            """
            sql """insert into ${mysqlDb}.${table2} values (10, 100)"""
            sql """insert into ${mysqlDb}.${table2} values (10, 101)"""
            sql """insert into ${mysqlDb}.${table2} values (10, 102)"""
            sql """insert into ${mysqlDb}.${table2} values (20, 200)"""
            sql """insert into ${mysqlDb}.${table2} values (20, 201)"""
            sql """insert into ${mysqlDb}.${table2} values (20, 202)"""
        }

        // snapshot_split_size=3 -> chunks cross the tenant boundary.
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1},${table2}",
                    "offset" = "initial",
                    "snapshot_split_size" = "3"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt1 = sql """select count(1) from ${currentDb}.${table1}"""
                        def cnt2 = sql """select count(1) from ${currentDb}.${table2}"""
                        log.info("snapshot row count table1=${cnt1} table2=${cnt2}")
                        cnt1.get(0).get(0) == 10 && cnt2.get(0).get(0) == 6
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        def showTbl = sql """show create table ${currentDb}.${table1}"""
        def createInfo = showTbl[0][1]
        log.info("create table: " + createInfo)
        assert createInfo.contains("UNIQUE KEY(`order_id`, `tenant_id`)")

        def showTbl2 = sql """show create table ${currentDb}.${table2}"""
        def createInfo2 = showTbl2[0][1]
        log.info("create table2: " + createInfo2)
        assert createInfo2.contains("UNIQUE KEY(`role_id`, `user_id`)")

        qt_desc_composite_pk """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select tenant_id, order_id, order_no, amount from ${currentDb}.${table1} order by tenant_id, order_id;"""
        qt_select_snapshot_map """select user_id, role_id from ${currentDb}.${table2} order by user_id, role_id;"""

        // ===== Binlog phase: INSERT / UPDATE / DELETE that all require composite-PK locating =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """insert into ${mysqlDb}.${table1} values (1, 1006, 'A_006', 60.00)"""
            sql """insert into ${mysqlDb}.${table1} values (2, 2006, 'B_006', 600.00)"""

            // UPDATEs targeting composite PK; both halves of the PK in WHERE.
            sql """update ${mysqlDb}.${table1} set amount=999.99 where tenant_id=1 and order_id=1003"""
            sql """update ${mysqlDb}.${table1} set amount=888.88 where tenant_id=2 and order_id=2002"""

            // DELETEs targeting composite PK.
            sql """delete from ${mysqlDb}.${table1} where tenant_id=1 and order_id=1001"""
            sql """delete from ${mysqlDb}.${table1} where tenant_id=2 and order_id=2005"""

            // table2 full-PK mapping table: INSERT + DELETE only (no value column to UPDATE).
            sql """insert into ${mysqlDb}.${table2} values (10, 103)"""
            sql """insert into ${mysqlDb}.${table2} values (30, 300)"""
            sql """delete from ${mysqlDb}.${table2} where user_id=10 and role_id=100"""
            sql """delete from ${mysqlDb}.${table2} where user_id=20 and role_id=202"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def cntMap = sql """select count(1) from ${currentDb}.${table2}"""
                        def upd1 = sql """select amount from ${currentDb}.${table1} where tenant_id=1 and order_id=1003"""
                        def upd2 = sql """select amount from ${currentDb}.${table1} where tenant_id=2 and order_id=2002"""
                        def del1 = sql """select count(1) from ${currentDb}.${table1} where tenant_id=1 and order_id=1001"""
                        def del2 = sql """select count(1) from ${currentDb}.${table1} where tenant_id=2 and order_id=2005"""
                        def mapNew = sql """select count(1) from ${currentDb}.${table2} where user_id=30 and role_id=300"""
                        def mapDel = sql """select count(1) from ${currentDb}.${table2} where user_id=10 and role_id=100"""
                        def a1 = upd1.size() == 0 ? null : upd1.get(0).get(0)
                        def a2 = upd2.size() == 0 ? null : upd2.get(0).get(0)
                        log.info("incr count=" + cnt + " map.count=" + cntMap + " upd1.amount=" + a1 + " upd2.amount=" + a2
                                + " del1.exists=" + del1 + " del2.exists=" + del2
                                + " mapNew=" + mapNew + " mapDel=" + mapDel)
                        cnt.get(0).get(0) == 10 &&
                                cntMap.get(0).get(0) == 6 &&
                                a1 != null && a1.toString() == '999.99' &&
                                a2 != null && a2.toString() == '888.88' &&
                                del1.get(0).get(0) == 0 &&
                                del2.get(0).get(0) == 0 &&
                                mapNew.get(0).get(0) == 1 &&
                                mapDel.get(0).get(0) == 0
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select tenant_id, order_id, order_no, amount from ${currentDb}.${table1} order by tenant_id, order_id;"""
        qt_select_after_incr_map """select user_id, role_id from ${currentDb}.${table2} order by user_id, role_id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
