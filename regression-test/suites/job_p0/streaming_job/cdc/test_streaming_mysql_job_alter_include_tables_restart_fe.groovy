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


import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_mysql_job_alter_include_tables_restart_fe",
        "docker,mysql,external_docker,external_docker_mysql,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null
    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def host = context.config.otherConfigs.get("externalEnvIp")
        def port = context.config.otherConfigs.get("mysql_57_port")
        def sourceUrl = "jdbc:mysql://${host}:${port}"
        def jobName = "mysql_alter_include_restart"
        def retainedTable = "mysql_alter_restart_a"
        def addedTable = "mysql_alter_restart_b"
        def driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
        List sourceOffsetBeforeWrite
        def sourceSql = { String statement ->
            connect("root", "123456", sourceUrl) {
                sourceOffsetBeforeWrite = (sql "SHOW MASTER STATUS")[0]
                sql statement
            }
        }
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
        def restartFe = {
            cluster.restartFrontends()
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS)
                    .ignoreExceptions().until({
                        context.reconnectFe()
                        (sql "SELECT 1")[0][0] as int == 1
                    })
        }
        sourceSql("CREATE DATABASE IF NOT EXISTS test_cdc_db")
        sourceSql("DROP TABLE IF EXISTS test_cdc_db.${retainedTable}")
        sourceSql("CREATE TABLE test_cdc_db.${retainedTable} (id INT PRIMARY KEY, v INT)")
        sourceSql("DROP TABLE IF EXISTS test_cdc_db.${addedTable}")
        sourceSql("CREATE TABLE test_cdc_db.${addedTable} (id INT PRIMARY KEY, v INT)")
        sourceSql("INSERT INTO test_cdc_db.${retainedTable} VALUES (1, 1)")
        sourceSql("INSERT INTO test_cdc_db.${addedTable} VALUES " +
                (1..200).collect { "(${it}, ${it})" }.join(","))
        sql """CREATE JOB ${jobName} ON STREAMING FROM MYSQL (
                "jdbc_url" = "${sourceUrl}",
                "driver_url" = "${driverUrl}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "user" = "root",
                "password" = "123456",
                "database" = "test_cdc_db",
                "include_tables" = "${retainedTable}",
                "offset" = "initial",
                "snapshot_split_size" = "10",
                "snapshot_parallelism" = "1"
            ) TO DATABASE ${currentDb} (
                "table.create.properties.replication_num" = "1"
            )"""
        try {
            waitForCount("SELECT COUNT(*) FROM ${retainedTable}", 1)
            waitForCount("""SELECT COUNT(*) FROM jobs("type"="insert")
                    WHERE Name='${jobName}' AND SucceedTaskCount > 0""", 1)
            sourceSql("INSERT INTO test_cdc_db.${retainedTable} VALUES (2, 2)")
            waitForCount("SELECT COUNT(*) FROM ${retainedTable} WHERE id=2", 1)
            waitForIncrementalCatchUp()
            sql "PAUSE JOB where jobname='${jobName}'"
            waitForCount("""select count(*) from jobs("type"="insert")
                    where Name='${jobName}' and Status='PAUSED'""", 1)
            waitForCount("""select count(*) from tasks("type"="insert")
                    where JobName='${jobName}' and Status='RUNNING'""", 0)
            sql "UPDATE ${retainedTable} SET v=-2 WHERE id=2"
            sql """ALTER JOB ${jobName} FROM MYSQL ("include_tables" = "${retainedTable},${addedTable}")
                    TO DATABASE ${currentDb}"""
            // This row must be read from the saved T0 after restart, without resnapshotting A.
            sourceSql("INSERT INTO test_cdc_db.${retainedTable} VALUES (3, 3)")
            // Replay the published round before its first split is dispatched.
            restartFe()
            waitForCount("""select count(*) from jobs("type"="insert")
                    where Name='${jobName}' and Status='PAUSED'""", 1)
            sql "RESUME JOB where jobname='${jobName}'"
            sourceSql("UPDATE test_cdc_db.${addedTable} SET v=900 WHERE id=10")
            sourceSql("DELETE FROM test_cdc_db.${addedTable} WHERE id=20")
            sourceSql("INSERT INTO test_cdc_db.${addedTable} VALUES (201, 201)")
            waitForCount("SELECT COUNT(*) FROM ${addedTable}", 200)
            waitForCount("SELECT COUNT(*) FROM ${addedTable} WHERE id=10 AND v=900", 1)
            waitForCount("SELECT COUNT(*) FROM ${addedTable} WHERE id=20", 0)
            waitForCount("SELECT COUNT(*) FROM ${addedTable} WHERE id=201", 1)
            waitForCount("SELECT COUNT(*) FROM ${retainedTable} WHERE id=3", 1)
            order_qt_retained_after_restart "SELECT id, v FROM ${retainedTable}"
            order_qt_added_after_restart "SELECT COUNT(*), SUM(id), SUM(v) FROM ${addedTable}"
        } catch (Exception ex) {
            log.info("jobs: " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        } finally {
            sql "DROP JOB IF EXISTS where jobname='${jobName}'"
        }
    }
}
