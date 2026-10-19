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

suite("test_streaming_mysql_job_sc_advanced", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_sc_advanced"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "test_streaming_mysql_sc_adv_t"
    def mysqlDb = "test_cdc_db"

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
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                (sql "DESC ${table1}").any { it[0] == column } == expected
            })
        }
        def waitForRow = { String name ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                (sql "SELECT COUNT(*) FROM ${table1} WHERE name='${name}'")[0][0] as int > 0
            })
        }
        def waitForValue = { String name, String column, Object expected ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql "SELECT ${column} FROM ${table1} WHERE name='${name}'"
                rows.size() == 1 && String.valueOf(rows[0][0]) == String.valueOf(expected)
            })
        }
        def dumpJobState = {
            log.info("jobs  : " + sql("""SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks : " + sql("""SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
        }
        def succeedCount = {
            (sql """SELECT SucceedTaskCount FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0].toLong()
        }
        def waitForCommit = { long previous ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({ succeedCount() > previous })
        }
        def waitForPause = { String reason ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
                def rows = sql """SELECT Status, ErrorMsg FROM jobs("type"="insert") WHERE Name='${jobName}'"""
                rows.size() == 1 && rows[0][0] == "PAUSED" && rows[0][1].toString().contains(reason)
            })
        }

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                       name VARCHAR(200) NOT NULL,
                       age INT DEFAULT NULL,
                       note VARCHAR(50) DEFAULT NULL,
                       PRIMARY KEY (name)
                   ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES ('A1', 1, 'base')"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES ('B1', 2, 'base')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysqlPort}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """SELECT SucceedTaskCount FROM jobs("type"="insert")
                                  WHERE Name='${jobName}' AND ExecuteType='STREAMING'"""
                cnt.size() == 1 && cnt[0][0] as int >= 2
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        qt_snapshot """ SELECT name, age, note FROM ${table1} ORDER BY name """

        long beforePositionAdd = succeedCount()
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1}
                   ADD COLUMN first_col VARCHAR(20) FIRST,
                   ADD COLUMN after_name INT AFTER name"""
            sql """INSERT INTO ${mysqlDb}.${table1}
                   (first_col, name, after_name, age, note) VALUES ('F', 'C1', 7, 3, 'pos')"""
        }

        try {
            waitForColumn("first_col", true)
            waitForColumn("after_name", true)
            waitForRow("C1")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        qt_position_add """ SELECT name, age, note, first_col, after_name FROM ${table1} ORDER BY name """
        waitForCommit(beforePositionAdd)

        long beforeAddDrop = succeedCount()
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1}
                   ADD COLUMN city VARCHAR(30),
                   DROP COLUMN note"""
            sql """INSERT INTO ${mysqlDb}.${table1}
                   (first_col, name, after_name, age, city) VALUES ('G', 'D1', 8, 4, 'hz')"""
        }

        try {
            waitForColumn("city", true)
            waitForColumn("note", false)
            waitForRow("D1")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        qt_add_drop """ SELECT name, age, first_col, after_name, city FROM ${table1} ORDER BY name """
        waitForCommit(beforeAddDrop)

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1}
                   ADD COLUMN mixed_col INT,
                   MODIFY COLUMN age BIGINT"""
            sql """INSERT INTO ${mysqlDb}.${table1}
                   (first_col, name, after_name, age, city, mixed_col) VALUES ('X', 'E1', 9, 5, 'sh', 100)"""
        }

        waitForPause("SCHEMA_CHANGE_UNSUPPORTED")
        qt_mixed_paused """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
        qt_mixed_not_applied """SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema='${currentDb}' AND table_name='${table1}' AND column_name='mixed_col'"""
        qt_mixed_row_not_written "SELECT COUNT(*) FROM ${table1} WHERE name='E1'"
        sql "RESUME JOB WHERE jobname='${jobName}'"
        waitForPause("missing columns")
        qt_missing_column_pause """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
        sql "ALTER TABLE ${table1} ADD COLUMN mixed_col INT NULL"
        long beforeMixedResume = succeedCount()
        sql "RESUME JOB WHERE jobname='${jobName}'"
        try {
            waitForColumn("mixed_col", true)
            waitForRow("E1")
            waitForValue("E1", "mixed_col", 100)
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        assert (sql "DESC ${table1}").find { it[0] == "age" }[1] == "int"
        qt_mixed_add_modify """ SELECT name, age, first_col, after_name, city, mixed_col FROM ${table1} ORDER BY name """
        waitForCommit(beforeMixedResume)

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1} MODIFY COLUMN age SMALLINT"""
            sql """INSERT INTO ${mysqlDb}.${table1}
                   (first_col, name, after_name, age, city, mixed_col) VALUES ('M', 'F1', 10, 6, 'sz', 200)"""
        }

        waitForPause("SCHEMA_CHANGE_UNSUPPORTED")
        qt_modify_paused """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
        long beforeModifyResume = succeedCount()
        sql "RESUME JOB WHERE jobname='${jobName}'"
        try {
            waitForRow("F1")
            waitForValue("F1", "age", 6)
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        assert (sql "DESC ${table1}").find { it[0] == "age" }[1] == "int"
        qt_modify """ SELECT name, age, first_col, after_name, city, mixed_col FROM ${table1} ORDER BY name """
        waitForCommit(beforeModifyResume)

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1} CHANGE COLUMN age age2 INT"""
            sql """INSERT INTO ${mysqlDb}.${table1}
                   (first_col, name, after_name, age2, city, mixed_col) VALUES ('R', 'G1', 11, 7, 'bj', 300)"""
        }

        waitForPause("SCHEMA_CHANGE_UNSUPPORTED")
        qt_rename_paused """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
        // Keep age available for DML replayed from the last committed offset.
        sql "ALTER TABLE ${table1} ADD COLUMN age2 INT NULL"
        long beforeRenameResume = succeedCount()
        sql "RESUME JOB WHERE jobname='${jobName}'"
        try {
            waitForRow("G1")
            waitForValue("G1", "age2", 7)
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        qt_change """ SELECT name, age2, first_col, after_name, city, mixed_col FROM ${table1} ORDER BY name """
        waitForCommit(beforeRenameResume)

        // Change only the column comment; this is still a MODIFY COLUMN event.
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1} MODIFY COLUMN age2 INT COMMENT 'comment only'"""
            sql """INSERT INTO ${mysqlDb}.${table1}
                (first_col, name, after_name, age2, city, mixed_col) VALUES ('S', 'H1', 12, 8, 'cd', 400)"""
        }
        waitForPause("SCHEMA_CHANGE_UNSUPPORTED")
        qt_column_comment_paused """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
        long beforeCommentResume = succeedCount()
        sql "RESUME JOB WHERE jobname='${jobName}'"
        waitForValue("H1", "age2", 8)
        waitForCommit(beforeCommentResume)
        qt_column_comment_resumed """SELECT name, age2, first_col, after_name, city, mixed_col
            FROM ${table1} WHERE name='H1' ORDER BY name"""

        assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
