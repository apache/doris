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

suite("test_streaming_mysql_job_non_column_ddl", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    String sourcePort = context.config.otherConfigs.get("mysql_57_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    def currentDb = (sql "SELECT DATABASE()")[0][0]
    def sourceSql = { String statement ->
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${sourcePort}") {
            sql statement
        }
    }
    def succeedCount = {
        (sql """SELECT SucceedTaskCount FROM jobs("type"="insert")
            WHERE Name='mysql_non_column_ddl'""")[0][0].toLong()
    }
    long committedCount = 0L
    def waitForCommittedRow = { int id, String name ->
        Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
            def rows = sql "SELECT name FROM mysql_non_column_ddl WHERE id=${id}"
            rows.size() == 1 && rows[0][0] == name
        })
        // The task that made the row visible must also commit its source offset.
        Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
            succeedCount() > committedCount
        })
        committedCount = succeedCount()
    }
    sql "DROP JOB IF EXISTS WHERE jobname='mysql_non_column_ddl'"
    sql "DROP TABLE IF EXISTS mysql_non_column_ddl FORCE"
    sql "DROP TABLE IF EXISTS mysql_non_column_other FORCE"
    sourceSql("CREATE DATABASE IF NOT EXISTS test_cdc_db")
    sourceSql("DROP TABLE IF EXISTS test_cdc_db.mysql_non_column_ddl")
    sourceSql("DROP TABLE IF EXISTS test_cdc_db.mysql_non_column_other")
    sourceSql("""CREATE TABLE test_cdc_db.mysql_non_column_ddl
        (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB""")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (1, 'original'), (2, 'retained')")

    sql """CREATE JOB mysql_non_column_ddl PROPERTIES ("max_interval" = "5") ON STREAMING
        FROM MYSQL (
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${sourcePort}",
            "driver_url" = "${driverUrl}", "driver_class" = "com.mysql.cj.jdbc.Driver",
            "user" = "root", "password" = "123456", "database" = "test_cdc_db",
            "include_tables" = "mysql_non_column_ddl", "offset" = "initial"
        ) TO DATABASE ${currentDb} ("table.create.properties.replication_num" = "1")"""
    waitForCommittedRow(2, "retained")

    sourceSql("ALTER TABLE test_cdc_db.mysql_non_column_ddl ADD INDEX idx_name (name)")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (3, 'index_added')")
    waitForCommittedRow(3, "index_added")
    qt_ignored_index "SHOW INDEX FROM mysql_non_column_ddl"
    sourceSql("ALTER TABLE test_cdc_db.mysql_non_column_ddl DROP INDEX idx_name")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (4, 'index_dropped')")
    waitForCommittedRow(4, "index_dropped")
    sourceSql("ALTER TABLE test_cdc_db.mysql_non_column_ddl COMMENT='table metadata only'")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (5, 'table_comment')")
    waitForCommittedRow(5, "table_comment")

    sourceSql("""ALTER TABLE test_cdc_db.mysql_non_column_ddl
        ADD COLUMN extra INT, ADD INDEX idx_extra (extra)""")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (6, 'mixed_add', 60)")
    waitForCommittedRow(6, "mixed_add")
    qt_mixed_add "SELECT id, name, extra FROM mysql_non_column_ddl WHERE id=6 ORDER BY id"
    qt_ignored_mixed_index "SHOW INDEX FROM mysql_non_column_ddl"

    sourceSql("TRUNCATE TABLE test_cdc_db.mysql_non_column_ddl")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (1, 'after_truncate', 10), (7, 'new_after_truncate', 70)")
    waitForCommittedRow(7, "new_after_truncate")
    qt_after_truncate "SELECT id, name, extra FROM mysql_non_column_ddl ORDER BY id"

    sourceSql("DROP TABLE test_cdc_db.mysql_non_column_ddl")
    sourceSql("""CREATE TABLE test_cdc_db.mysql_non_column_ddl
        (id INT PRIMARY KEY, name VARCHAR(50), extra INT, INDEX idx_extra (extra))
        ENGINE=InnoDB COMMENT='table metadata only'""")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (1, 'after_recreate', 100), (8, 'new_after_recreate', 80)")
    waitForCommittedRow(8, "new_after_recreate")
    qt_after_recreate "SELECT id, name, extra FROM mysql_non_column_ddl ORDER BY id"

    sourceSql("CREATE TABLE test_cdc_db.mysql_non_column_other (id INT PRIMARY KEY)")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (9, 'other_created', 90)")
    waitForCommittedRow(9, "other_created")
    sourceSql("ALTER TABLE test_cdc_db.mysql_non_column_other ADD COLUMN ignored INT")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (10, 'other_altered', 100)")
    waitForCommittedRow(10, "other_altered")
    sourceSql("DROP TABLE test_cdc_db.mysql_non_column_other")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (11, 'other_dropped', 110)")
    waitForCommittedRow(11, "other_dropped")

    sql "PAUSE JOB WHERE jobname='mysql_non_column_ddl'"
    sql "RESUME JOB WHERE jobname='mysql_non_column_ddl'"
    sourceSql("ALTER TABLE test_cdc_db.mysql_non_column_ddl ADD COLUMN after_resume INT")
    sourceSql("INSERT INTO test_cdc_db.mysql_non_column_ddl VALUES (12, 'resumed', 120, 1200)")
    waitForCommittedRow(12, "resumed")
    qt_final_rows "SELECT id, name, extra, after_resume FROM mysql_non_column_ddl ORDER BY id"
    qt_final_columns """SELECT column_name FROM information_schema.columns
        WHERE table_schema='${currentDb}' AND table_name='mysql_non_column_ddl' ORDER BY column_name"""
    qt_unselected_table """SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema='${currentDb}' AND table_name='mysql_non_column_other'"""
    qt_final_job """SELECT Status, FailedTaskCount FROM jobs("type"="insert")
        WHERE Name='mysql_non_column_ddl'"""
    sql "DROP JOB IF EXISTS WHERE jobname='mysql_non_column_ddl'"
}
