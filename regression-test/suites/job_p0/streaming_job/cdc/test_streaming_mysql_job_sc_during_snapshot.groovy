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

suite("test_streaming_mysql_job_sc_during_snapshot", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_sc_during_snapshot"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "test_streaming_mysql_sc_snapshot_t"
    def mysqlDb = "test_cdc_db"
    def totalRows = 1000

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${table1} FORCE"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        def waitForColumn = { String column, boolean expected ->
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                (sql "DESC ${table1}").any { it[0] == column } == expected
            })
        }
        def waitForValue = { int id, String column, String expected ->
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql "SELECT ${column} FROM ${table1} WHERE id=${id}"
                rows.size() == 1 && String.valueOf(rows[0][0]) == expected
            })
        }
        def dumpJobState = {
            log.info("jobs  : " + sql("""SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks : " + sql("""SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
        }

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                       id INT NOT NULL,
                       tag VARCHAR(64),
                       version INT,
                       PRIMARY KEY (id)
                   ) ENGINE=InnoDB"""

            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${mysqlDb}.${table1} (id, tag, version) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) {
                    sb.append(", ")
                }
                sb.append("(${i}, 'snap', 0)")
            }
            sql sb.toString()
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysqlPort}?serverTimezone=UTC",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial",
                    "snapshot_split_size" = "10",
                    "snapshot_parallelism" = "1"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1} ADD COLUMN sc_note VARCHAR(50)"""
            sql """INSERT INTO ${mysqlDb}.${table1} (id, tag, version, sc_note)
                   VALUES (${totalRows + 1}, 'after_add', 1, 'during_snapshot')"""
        }

        try {
            waitForColumn("sc_note", true)
            waitForValue(totalRows + 1, "sc_note", "during_snapshot")
            Awaitility.await().atMost(600, SECONDS).pollInterval(2, SECONDS).until({
                def cnt = sql """SELECT COUNT(1) FROM ${table1}"""
                def nullCnt = sql """SELECT COUNT(1) FROM ${table1} WHERE sc_note IS NULL"""
                log.info("snapshot schema change cnt=${cnt}, nullCnt=${nullCnt}")
                cnt[0][0] == totalRows + 1 && nullCnt[0][0] == totalRows
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        order_qt_counts """ SELECT COUNT(1),
                                   SUM(CASE WHEN sc_note IS NULL THEN 1 ELSE 0 END),
                                   SUM(CASE WHEN sc_note = 'during_snapshot' THEN 1 ELSE 0 END)
                            FROM ${table1} """
        qt_samples """ SELECT id, tag, version, sc_note
                       FROM ${table1}
                       WHERE id IN (1, 500, 1000, 1001)
                       ORDER BY id """

        assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
