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


import groovy.json.JsonSlurper
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_mysql_job_alter_include_tables_latest",
        "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    def currentDb = (sql "select database()")[0][0]
    def host = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("mysql_57_port")
    def sourceUrl = "jdbc:mysql://${host}:${port}"
    def sourceDb = "test_cdc_db"
    def driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    List sourceOffsetBeforeWrite
    def sourceSql = { String statement ->
        connect("root", "123456", sourceUrl) {
            sourceOffsetBeforeWrite = (sql "SHOW MASTER STATUS")[0]
            sql statement
        }
    }

    // Added tables receive a snapshot even when the job starts from latest.
    def jobName = "mysql_alter_include_latest"
    def retainedTable = "${jobName}_a"
    def addedTable = "${jobName}_b"
    sql "DROP JOB IF EXISTS where jobname = '${jobName}'"
    sql "DROP TABLE IF EXISTS ${currentDb}.${retainedTable} FORCE"
    sql "DROP TABLE IF EXISTS ${currentDb}.${addedTable} FORCE"
    sourceSql("CREATE DATABASE IF NOT EXISTS ${sourceDb}")
    sourceSql("DROP TABLE IF EXISTS ${sourceDb}.${retainedTable}")
    sourceSql("CREATE TABLE ${sourceDb}.${retainedTable} (id INT PRIMARY KEY, v INT)")
    sourceSql("DROP TABLE IF EXISTS ${sourceDb}.${addedTable}")
    sourceSql("CREATE TABLE ${sourceDb}.${addedTable} (other_id INT PRIMARY KEY, v INT)")
    sourceSql("INSERT INTO ${sourceDb}.${retainedTable} VALUES (1, 1)")
    sourceSql("INSERT INTO ${sourceDb}.${addedTable} VALUES " +
            (1..200).collect { "(${it}, ${it})" }.join(","))

    def waitForCount = { String query, long expected ->
        Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
            (sql(query))[0][0] as long == expected
        })
    }
    // Data visibility can precede the FE callback; wait for the committed offset of the source write.
    def waitForIncrementalCatchUp = {
        Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
            String json = (sql """SELECT CurrentOffset FROM jobs("type"="insert")
                    WHERE Name='${jobName}'""")[0][0] as String
            if (!json?.startsWith("{")) {
                return false
            }
            def offset = new JsonSlurper().parseText(json)
            offset.file != null && offset.pos != null
                    && (offset.file > sourceOffsetBeforeWrite[0].toString()
                        || (offset.file == sourceOffsetBeforeWrite[0].toString()
                            && offset.pos.toString().toLong() > (sourceOffsetBeforeWrite[1] as long)))
        })
    }
    def pauseJob = {
        sql "PAUSE JOB where jobname = '${jobName}'"
        Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until({
            def rows = sql """select Status from jobs("type"="insert") where Name='${jobName}'"""
            rows.size() == 1 && rows[0][0] == "PAUSED"
        })
        waitForCount("""select count(*) from tasks("type"="insert")
                where JobName='${jobName}' and Status='RUNNING'""", 0)
    }
    def alterIncludeTablesSql = { String tables ->
        """ALTER JOB ${jobName} FROM MYSQL ("include_tables" = "${tables}")
                TO DATABASE ${currentDb}"""
    }
    def resumeJob = { sql "RESUME JOB where jobname = '${jobName}'" }

    sql """CREATE JOB ${jobName} ON STREAMING
            FROM MYSQL (
                "jdbc_url" = "${sourceUrl}",
                "driver_url" = "${driverUrl}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "user" = "root",
                "password" = "123456",
                "database" = "${sourceDb}",
                "include_tables" = "${retainedTable}",
                "offset" = "latest",
                "snapshot_split_size" = "10",
                "snapshot_parallelism" = "2"
            ) TO DATABASE ${currentDb} (
                "table.create.properties.replication_num" = "1"
            )"""
    try {
        // The first committed task initializes latest before the sentinel write.
        waitForCount("""SELECT COUNT(*) FROM jobs("type"="insert")
                WHERE Name='${jobName}' AND SucceedTaskCount > 0""", 1)
        sourceSql("INSERT INTO ${sourceDb}.${retainedTable} VALUES (2, 2)")
        waitForCount("SELECT COUNT(*) FROM ${retainedTable} WHERE id=2 AND v=2", 1)
        waitForIncrementalCatchUp()
        order_qt_latest_skips_history "SELECT id, v FROM ${retainedTable}"
        pauseJob()
        // Resnapshotting the retained table would overwrite this target-only change.
        sql "UPDATE ${retainedTable} SET v=-2 WHERE id=2"

        // Add the table, then verify its historical rows and changes around snapshot startup.
        sql alterIncludeTablesSql("${retainedTable},${addedTable}")
        resumeJob()
        sourceSql("UPDATE ${sourceDb}.${addedTable} SET v=900 WHERE other_id=10")
        sourceSql("DELETE FROM ${sourceDb}.${addedTable} WHERE other_id=20")
        sourceSql("INSERT INTO ${sourceDb}.${addedTable} VALUES (201, 201)")
        sourceSql("INSERT INTO ${sourceDb}.${retainedTable} VALUES (3, 3)")
        waitForCount("SELECT COUNT(*) FROM ${addedTable}", 200)
        waitForCount("SELECT COUNT(*) FROM ${addedTable} WHERE other_id=10 AND v=900", 1)
        waitForCount("SELECT COUNT(*) FROM ${addedTable} WHERE other_id=20", 0)
        waitForCount("SELECT COUNT(*) FROM ${addedTable} WHERE other_id=201", 1)
        waitForCount("SELECT COUNT(*) FROM ${retainedTable} WHERE id=3", 1)
        waitForIncrementalCatchUp()

        // These writes happen after the added-table snapshot, so only CDC can deliver them.
        sourceSql("INSERT INTO ${sourceDb}.${addedTable} VALUES (202, 202)")
        sourceSql("INSERT INTO ${sourceDb}.${retainedTable} VALUES (4, 4)")
        waitForCount("SELECT COUNT(*) FROM ${addedTable} WHERE other_id=202", 1)
        waitForCount("SELECT COUNT(*) FROM ${retainedTable} WHERE id=4", 1)
        waitForIncrementalCatchUp()
        order_qt_retained_after_add "SELECT id, v FROM ${retainedTable}"
        order_qt_added_summary "SELECT COUNT(*), SUM(other_id), SUM(v) FROM ${addedTable}"
    } catch (Exception ex) {
        log.info("jobs: " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
        log.info("tasks: " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
        throw ex
    } finally {
        sql "DROP JOB IF EXISTS where jobname = '${jobName}'"
    }
}
