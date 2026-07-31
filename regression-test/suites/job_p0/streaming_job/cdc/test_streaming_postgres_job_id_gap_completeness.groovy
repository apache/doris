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

suite("test_streaming_postgres_job_id_gap_completeness", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_id_gap_completeness_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_id_gap_pg"
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

        // ===== Prepare PG side =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table1} (
                id      bigint PRIMARY KEY,
                tag     varchar(64),
                version integer
            );
            """
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag, version)
                   SELECT g, 'dense', 0 FROM generate_series(1, 100) g"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag, version) VALUES (10000000, 'outlier', 0)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag, version)
                   SELECT g, 'post_outlier', 0 FROM generate_series(10000001, 10000100) g"""
        }

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
                    "include_tables" = "${table1}",
                    "offset" = "initial",
                    "snapshot_split_size" = "20"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 201
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        def distinctCount = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
        assert distinctCount.get(0).get(0) == 201
        def outlierExists = sql """SELECT COUNT(1) FROM ${currentDb}.${table1} WHERE id=10000000"""
        assert outlierExists.get(0).get(0) == 1
        def postOutlierCount = sql """SELECT COUNT(1) FROM ${currentDb}.${table1} WHERE id BETWEEN 10000001 AND 10000100"""
        assert postOutlierCount.get(0).get(0) == 100

        qt_select_snapshot_count """select count(1) from ${currentDb}.${table1}"""
        qt_select_snapshot_dense_sample """select id, tag from ${currentDb}.${table1} where id in (1, 50, 100) order by id"""
        qt_select_snapshot_outlier_sample """select id, tag from ${currentDb}.${table1} where id in (10000000, 10000050, 10000100) order by id"""

        // ===== Binlog phase: cover dense + outlier + post-outlier ranges =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag, version) VALUES (10000200, 'incr_outlier', 1)"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET version=99 WHERE id=50"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE id=10000000"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd = sql """select version from ${currentDb}.${table1} where id=50"""
                        def del = sql """select count(1) from ${currentDb}.${table1} where id=10000000"""
                        def ins = sql """select count(1) from ${currentDb}.${table1} where id=10000200"""
                        def v = upd.size() == 0 ? null : upd.get(0).get(0)
                        log.info("incr cnt=${cnt} id50.version=${v} id10000000.exists=${del} id10000200.exists=${ins}")
                        cnt.get(0).get(0) == 201 &&
                                v != null && v.toString() == '99' &&
                                del.get(0).get(0) == 0 &&
                                ins.get(0).get(0) == 1
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr_count """select count(1) from ${currentDb}.${table1}"""
        qt_select_after_incr_changed """select id, tag, version from ${currentDb}.${table1} where id in (50, 10000200) order by id"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
