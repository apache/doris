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

// Multi-table from-to snapshot + concurrent DML: with >=2 tables each snapshot split flips the
// shared publication to its single table, so a row written to a table while the publication
// temporarily excludes it would be filtered on stream replay and lost. The fix keeps the
// publication full-table, so no row is lost. Asserts every row of BOTH tables is synced.
suite("test_streaming_postgres_job_snapshot_with_concurrent_dml_multi_table",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_pg_snapshot_concurrent_dml_multi_table_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_snapshot_dml_multi_pg_t1"
    def table2 = "streaming_snapshot_dml_multi_pg_t2"
    def tables = [table1, table2]
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def totalRows = 1000

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    tables.each { sql """drop table if exists ${currentDb}.${it} force""" }

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // ===== Prepare PG side: two tables, each 1000 snapshot rows =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            tables.each { t ->
                sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${t}"""
                sql """
                create table ${pgDB}.${pgSchema}.${t} (
                    id      integer PRIMARY KEY,
                    tag     varchar(64),
                    version integer
                );
                """
                sql """INSERT INTO ${pgDB}.${pgSchema}.${t} (id, tag, version)
                       SELECT g, 'snap', 0 FROM generate_series(1, ${totalRows}) g"""
            }
        }

        // snapshot_split_size=10 + snapshot_parallelism=1 -> 100 serial splits per table, slow
        // enough that the concurrent DML overlaps snapshot while the publication keeps flipping.
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
                    "snapshot_split_size" = "10",
                    "snapshot_parallelism" = "1"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Wait until the first snapshot split commits (slot created, snapshot in progress) so the
        // DML below lands inside the snapshot window and overlaps the publication flipping.
        Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until({
            def c = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
            c.size() == 1 && (c.get(0).get(0).toString() as long) >= 1
        })

        // Concurrent DML on BOTH tables while still snapshotting. Same DML shape on each table.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            tables.each { t ->
                for (int i = 1; i <= 10; i++) {
                    sql """INSERT INTO ${pgDB}.${pgSchema}.${t} (id, tag, version) VALUES (${totalRows + i}, 'concurrent_ins', 1)"""
                }
                sql """UPDATE ${pgDB}.${pgSchema}.${t} SET version=99 WHERE id IN (1, 100, 500, 999)"""
                sql """DELETE FROM ${pgDB}.${pgSchema}.${t} WHERE id IN (2, 200, 800)"""
            }
        }

        // Each table: 1000 + 10 inserts - 3 deletes = 1007 rows, updates/deletes/inserts applied.
        def expectedRows = totalRows + 10 - 3
        try {
            Awaitility.await().atMost(600, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        boolean allOk = true
                        for (def t : tables) {
                            def showTbl = sql """show tables from ${currentDb} like '${t}'"""
                            if (showTbl.size() == 0) {
                                allOk = false
                                break
                            }
                            def cnt = sql """select count(1) from ${currentDb}.${t}"""
                            def upd1 = sql """select version from ${currentDb}.${t} where id=1"""
                            def upd999 = sql """select version from ${currentDb}.${t} where id=999"""
                            def del2 = sql """select count(1) from ${currentDb}.${t} where id=2"""
                            def del800 = sql """select count(1) from ${currentDb}.${t} where id=800"""
                            def ins = sql """select count(1) from ${currentDb}.${t} where id=${totalRows + 10}"""
                            def v1 = upd1.size() == 0 ? null : upd1.get(0).get(0)
                            def v999 = upd999.size() == 0 ? null : upd999.get(0).get(0)
                            log.info("table=${t} cnt=${cnt} v1=${v1} v999=${v999} del2=${del2} del800=${del800} ins=${ins}")
                            boolean ok = cnt.get(0).get(0) == expectedRows &&
                                    v1 != null && v1.toString() == '99' &&
                                    v999 != null && v999.toString() == '99' &&
                                    del2.get(0).get(0) == 0 &&
                                    del800.get(0).get(0) == 0 &&
                                    ins.get(0).get(0) == 1
                            if (!ok) {
                                allOk = false
                                break
                            }
                        }
                        allOk
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_select_count_t1 """select count(1) from ${currentDb}.${table1}"""
        qt_select_count_t2 """select count(1) from ${currentDb}.${table2}"""
        qt_select_updates_t1 """select id, version from ${currentDb}.${table1} where id in (1, 100, 500, 999) order by id"""
        qt_select_updates_t2 """select id, version from ${currentDb}.${table2} where id in (1, 100, 500, 999) order by id"""
        qt_select_inserts_t1 """select id, tag, version from ${currentDb}.${table1} where id > ${totalRows} order by id"""
        qt_select_inserts_t2 """select id, tag, version from ${currentDb}.${table2} where id > ${totalRows} order by id"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
