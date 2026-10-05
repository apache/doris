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

suite("test_streaming_postgres_job_composite_pk", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_composite_pk_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_composite_pk_pg"
    def table2 = "streaming_full_pk_map_pg"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""
    sql """drop table if exists ${currentDb}.${table2} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // ===== Prepare PG side =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table1} (
                org_id     integer NOT NULL,
                item_id    bigint  NOT NULL,
                item_no    varchar(64),
                qty        integer,
                PRIMARY KEY (org_id, item_id)
            );
            """

            // Snapshot rows: 2 orgs x 5 items each.
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 1001, 'A_001', 10)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 1002, 'A_002', 20)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 1003, 'A_003', 30)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 1004, 'A_004', 40)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 1005, 'A_005', 50)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 2001, 'B_001', 100)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 2002, 'B_002', 200)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 2003, 'B_003', 300)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 2004, 'B_004', 400)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 2005, 'B_005', 500)"""

            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table2}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table2} (
                user_id integer NOT NULL,
                role_id integer NOT NULL,
                PRIMARY KEY (user_id, role_id)
            );
            """
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (10, 100)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (10, 101)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (10, 102)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (20, 200)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (20, 201)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (20, 202)"""
        }

        // snapshot_split_size=3 -> chunks cross the org boundary.
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
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
        assert createInfo.contains("UNIQUE KEY(`org_id`, `item_id`)")

        def showTbl2 = sql """show create table ${currentDb}.${table2}"""
        def createInfo2 = showTbl2[0][1]
        log.info("create table2: " + createInfo2)
        assert createInfo2.contains("UNIQUE KEY(`user_id`, `role_id`)")

        qt_desc_composite_pk """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select org_id, item_id, item_no, qty from ${currentDb}.${table1} order by org_id, item_id;"""
        qt_select_snapshot_map """select user_id, role_id from ${currentDb}.${table2} order by user_id, role_id;"""

        // ===== Binlog phase: INSERT / UPDATE / DELETE that all require composite-PK locating =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 1006, 'A_006', 60)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 2006, 'B_006', 600)"""

            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET qty=999 WHERE org_id=1 AND item_id=1003"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET qty=888 WHERE org_id=2 AND item_id=2002"""

            sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE org_id=1 AND item_id=1001"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE org_id=2 AND item_id=2005"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (10, 103)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} VALUES (30, 300)"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${table2} WHERE user_id=10 AND role_id=100"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${table2} WHERE user_id=20 AND role_id=202"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def cntMap = sql """select count(1) from ${currentDb}.${table2}"""
                        def upd1 = sql """select qty from ${currentDb}.${table1} where org_id=1 and item_id=1003"""
                        def upd2 = sql """select qty from ${currentDb}.${table1} where org_id=2 and item_id=2002"""
                        def del1 = sql """select count(1) from ${currentDb}.${table1} where org_id=1 and item_id=1001"""
                        def del2 = sql """select count(1) from ${currentDb}.${table1} where org_id=2 and item_id=2005"""
                        def mapNew = sql """select count(1) from ${currentDb}.${table2} where user_id=30 and role_id=300"""
                        def mapDel = sql """select count(1) from ${currentDb}.${table2} where user_id=10 and role_id=100"""
                        def q1 = upd1.size() == 0 ? null : upd1.get(0).get(0)
                        def q2 = upd2.size() == 0 ? null : upd2.get(0).get(0)
                        log.info("incr count=" + cnt + " map.count=" + cntMap + " upd1.qty=" + q1 + " upd2.qty=" + q2
                                + " del1.exists=" + del1 + " del2.exists=" + del2
                                + " mapNew=" + mapNew + " mapDel=" + mapDel)
                        cnt.get(0).get(0) == 10 &&
                                cntMap.get(0).get(0) == 6 &&
                                q1 != null && q1.toString() == '999' &&
                                q2 != null && q2.toString() == '888' &&
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

        qt_select_after_incr """select org_id, item_id, item_no, qty from ${currentDb}.${table1} order by org_id, item_id;"""
        qt_select_after_incr_map """select user_id, role_id from ${currentDb}.${table2} order by user_id, role_id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
