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

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_mysql_job_snapshot_with_concurrent_dml", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_snapshot_with_concurrent_dml_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_snapshot_dml_mysql"
    def table2 = "streaming_snapshot_dml_other_pk_mysql"
    def excludedTable = "streaming_snapshot_dml_excluded_mysql"
    def mysqlDb = "test_cdc_db"
    def totalRows = 1000

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""
    sql """drop table if exists ${currentDb}.${table2} force"""
    sql """drop table if exists ${currentDb}.${excludedTable} force"""

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
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `id` int NOT NULL,
                  `tag` varchar(64),
                  `version` int,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB"""

            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${mysqlDb}.${table1} (id, tag, version) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) sb.append(", ")
                sb.append("(${i}, 'snap', 0)")
            }
            sql sb.toString()

            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table2}"""
            sql """CREATE TABLE ${mysqlDb}.${table2} (
                  `other_id` int NOT NULL,
                  `tag` varchar(64),
                  `version` int,
                  PRIMARY KEY (`other_id`)
                ) ENGINE=InnoDB"""

            StringBuilder table2Rows = new StringBuilder()
            table2Rows.append("INSERT INTO ${mysqlDb}.${table2} (other_id, tag, version) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) table2Rows.append(", ")
                table2Rows.append("(${i}, 'snap', 0)")
            }
            sql table2Rows.toString()

            sql """DROP TABLE IF EXISTS ${mysqlDb}.${excludedTable}"""
            sql """CREATE TABLE ${mysqlDb}.${excludedTable} (
                  `id` int NOT NULL,
                  `tag` varchar(64),
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${excludedTable} (id, tag) VALUES (1, 'pre_snap')"""
        }

        // Two tables with different primary-key names make a foreign-table backfill record
        // incompatible with the current split key. Serial small splits keep the snapshot active.
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
                    "snapshot_split_size" = "10",
                    "snapshot_parallelism" = "1",
                    "skip_snapshot_backfill" = "false"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        def succeedTaskCount = {
            def rows = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
            rows.size() == 1 ? (rows.get(0).get(0).toString() as long) : 0L
        }
        Awaitility.await().atMost(120, SECONDS).pollInterval(100, MILLISECONDS).until({
            succeedTaskCount() >= 1
        })

        def writeProbeDml = {
            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
                sql """UPDATE ${mysqlDb}.${table1} SET version=version+1 WHERE id=500"""
                sql """UPDATE ${mysqlDb}.${table2} SET version=version+1 WHERE other_id=500"""
            }
        }

        writeProbeDml()
        long probeStartTaskCount = succeedTaskCount()
        long probeTargetTaskCount = probeStartTaskCount + 10
        Awaitility.await().atMost(120, SECONDS).pollInterval(200, MILLISECONDS).until({
            writeProbeDml()
            succeedTaskCount() >= probeTargetTaskCount
        })
        log.info("MySQL probe DML covered tasks ${probeStartTaskCount}..${succeedTaskCount()}")

        // Apply deterministic DML after the probe updates so final results remain stable.
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            for (int i = 1; i <= 10; i++) {
                sql """INSERT INTO ${mysqlDb}.${table1} (id, tag, version) VALUES (${totalRows + i}, 'concurrent_ins', 1)"""
                sql """INSERT INTO ${mysqlDb}.${table2} (other_id, tag, version) VALUES (${totalRows + i}, 'concurrent_ins', 1)"""
            }
            sql """UPDATE ${mysqlDb}.${table1} SET version=99 WHERE id IN (1, 100, 500, 999)"""
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE id IN (2, 200, 800)"""
            sql """UPDATE ${mysqlDb}.${table2} SET version=99 WHERE other_id IN (1, 100, 500, 999)"""
            sql """DELETE FROM ${mysqlDb}.${table2} WHERE other_id IN (2, 200, 800)"""

            sql """INSERT INTO ${mysqlDb}.${excludedTable} (id, tag) VALUES (2, 'concurrent_excluded_ins')"""
            sql """UPDATE ${mysqlDb}.${excludedTable} SET tag='concurrent_excluded_upd' WHERE id=1"""
        }

        def expectedRows = totalRows + 10 - 3
        try {
            Awaitility.await().atMost(600, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select version from ${currentDb}.${table1} where id=1"""
                        def upd999 = sql """select version from ${currentDb}.${table1} where id=999"""
                        def del2 = sql """select count(1) from ${currentDb}.${table1} where id=2"""
                        def del800 = sql """select count(1) from ${currentDb}.${table1} where id=800"""
                        def ins1010 = sql """select count(1) from ${currentDb}.${table1} where id=${totalRows + 10}"""
                        def cnt2 = sql """select count(1) from ${currentDb}.${table2}"""
                        def upd2 = sql """select version from ${currentDb}.${table2} where other_id=999"""
                        def del2Table2 = sql """select count(1) from ${currentDb}.${table2} where other_id=2"""
                        def ins2 = sql """select count(1) from ${currentDb}.${table2} where other_id=${totalRows + 10}"""
                        def v1 = upd1.size() == 0 ? null : upd1.get(0).get(0)
                        def v999 = upd999.size() == 0 ? null : upd999.get(0).get(0)
                        def v2 = upd2.size() == 0 ? null : upd2.get(0).get(0)
                        log.info("incr cnt=${cnt} cnt2=${cnt2} v1=${v1} v999=${v999} v2=${v2} "
                                + "del2=${del2} del800=${del800} del2Table2=${del2Table2} "
                                + "ins1010=${ins1010} ins2=${ins2}")
                        cnt.get(0).get(0) == expectedRows &&
                                cnt2.get(0).get(0) == expectedRows &&
                                v1 != null && v1.toString() == '99' &&
                                v999 != null && v999.toString() == '99' &&
                                v2 != null && v2.toString() == '99' &&
                                del2.get(0).get(0) == 0 &&
                                del800.get(0).get(0) == 0 &&
                                del2Table2.get(0).get(0) == 0 &&
                                ins1010.get(0).get(0) == 1 &&
                                ins2.get(0).get(0) == 1
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        def showExcluded = sql """show tables from ${currentDb} like '${excludedTable}'"""
        assert showExcluded.size() == 0

        qt_select_count """select count(1) from ${currentDb}.${table1}"""
        qt_select_count_t2 """select count(1) from ${currentDb}.${table2}"""
        qt_select_updates """select id, version from ${currentDb}.${table1} where id in (1, 100, 500, 999) order by id"""
        qt_select_updates_t2 """select other_id, version from ${currentDb}.${table2} where other_id in (1, 100, 500, 999) order by other_id"""
        qt_select_deletes """select count(1) from ${currentDb}.${table1} where id in (2, 200, 800)"""
        qt_select_deletes_t2 """select count(1) from ${currentDb}.${table2} where other_id in (2, 200, 800)"""
        qt_select_inserts """select id, tag, version from ${currentDb}.${table1} where id > ${totalRows} order by id"""
        qt_select_inserts_t2 """select other_id, tag, version from ${currentDb}.${table2} where other_id > ${totalRows} order by other_id"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
