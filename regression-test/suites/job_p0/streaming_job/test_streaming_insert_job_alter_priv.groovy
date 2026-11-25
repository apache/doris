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

suite("test_streaming_insert_job_alter_priv") {
    def db = context.dbName
    def owner = "streaming_alter_sql_owner"
    def editor = "streaming_alter_sql_editor"
    def password = "StreamAlter_123"
    def jobName = "test_streaming_insert_job_alter_priv"
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + db + "?"

    sql "DROP JOB IF EXISTS WHERE jobname = '${jobName}'"
    sql "DROP USER IF EXISTS '${owner}'"
    sql "DROP USER IF EXISTS '${editor}'"
    sql "DROP TABLE IF EXISTS streaming_alter_sql_target"
    sql "DROP TABLE IF EXISTS streaming_alter_sql_private"
    sql """CREATE TABLE streaming_alter_sql_target (c1 INT, c2 STRING, c3 INT)
           DISTRIBUTED BY HASH(c1) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    sql """CREATE TABLE streaming_alter_sql_private (id INT, secret STRING)
           DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO streaming_alter_sql_private VALUES (1, 'private value')"

    sql "CREATE USER '${owner}' IDENTIFIED BY '${password}'"
    sql "CREATE USER '${editor}' IDENTIFIED BY '${password}'"
    for (user in [owner, editor]) {
        sql "GRANT LOAD_PRIV, SELECT_PRIV ON ${db}.streaming_alter_sql_target TO '${user}'"
        if (isCloudMode()) {
            def clusters = sql_return_maparray "SHOW CLUSTERS"
            for (cluster in clusters) {
                if (cluster.is_current.equalsIgnoreCase("TRUE")) {
                    sql "GRANT USAGE_PRIV ON CLUSTER `${cluster.cluster}` TO '${user}'"
                }
            }
        }
    }
    sql "GRANT SELECT_PRIV ON ${db}.streaming_alter_sql_private TO '${owner}'"

    def source = """S3(
        "uri" = "s3://${s3BucketName}/regression/load/data/example_[0-1].csv",
        "format" = "csv",
        "provider" = "${getS3Provider()}",
        "column_separator" = ",",
        "s3.endpoint" = "${getS3Endpoint()}",
        "s3.region" = "${getS3Region()}",
        "s3.access_key" = "${getS3AK()}",
        "s3.secret_key" = "${getS3SK()}"
    )"""
    def original = """INSERT INTO ${db}.streaming_alter_sql_target
                      SELECT s.c1, s.c2, s.c3 FROM ${source} s"""
    def joined = """INSERT INTO ${db}.streaming_alter_sql_target
                    SELECT s.c1, p.secret, s.c3 FROM ${source} s
                    CROSS JOIN ${db}.streaming_alter_sql_private p"""
    def subquery = """INSERT INTO ${db}.streaming_alter_sql_target
                      SELECT s.c1, (SELECT max(secret) FROM ${db}.streaming_alter_sql_private), s.c3
                      FROM ${source} s"""
    def union = """INSERT INTO ${db}.streaming_alter_sql_target
                   SELECT s.c1, s.c2, s.c3 FROM ${source} s
                   UNION ALL SELECT id, secret, id FROM ${db}.streaming_alter_sql_private"""
    try {
        connectToDoris(owner, password, url) {
            sql """CREATE JOB ${jobName} PROPERTIES("s3.max_batch_files" = "1")
                   ON STREAMING DO ${original}"""
            sql "PAUSE JOB WHERE jobname = '${jobName}'"
        }
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until {
            sql("SELECT Status FROM jobs('type'='insert') WHERE Name='${jobName}'")[0][0] == "PAUSED"
        }
        def pendingBefore = (sql "SHOW LOAD FROM ${db} WHERE State = 'PENDING'").size()
        def before = sql """SELECT md5(ExecuteSql), md5(IFNULL(CurrentOffset, ''))
                           FROM jobs('type'='insert') WHERE Name='${jobName}'"""
        connectToDoris(editor, password, url) {
            test {
                sql "SELECT secret FROM ${db}.streaming_alter_sql_private"
                exception "denied"
            }
            // All three queries keep exactly one TVF and the original target and URI.
            for (query in [joined, subquery, union]) {
                test {
                    sql """ALTER JOB ${jobName} PROPERTIES("s3.max_batch_files" = "2") ${query}"""
                    exception "denied"
                }
            }
        }
        qt_rejected_unchanged """SELECT Status, Properties,
            md5(ExecuteSql) = '${before[0][0]}', md5(IFNULL(CurrentOffset, '')) = '${before[0][1]}'
            FROM jobs('type'='insert') WHERE Name='${jobName}'"""

        // The creator can still modify the query using a source it is allowed to read.
        connectToDoris(owner, password, url) {
            sql "ALTER JOB ${jobName} ${joined}"
        }
        qt_owner_altered """SELECT Status, ExecuteSql LIKE '%CROSS JOIN%'
            FROM jobs('type'='insert') WHERE Name='${jobName}'"""

        // A different user with all required privileges can modify the same job.
        sql "GRANT SELECT_PRIV ON ${db}.streaming_alter_sql_private TO '${editor}'"
        connectToDoris(editor, password, url) {
            sql "ALTER JOB ${jobName} ${subquery}"
            sql "ALTER JOB ${jobName} ${union}"
        }
        qt_editor_altered """SELECT Status, ExecuteSql LIKE '%UNION ALL%'
            FROM jobs('type'='insert') WHERE Name='${jobName}'"""

        sql "REVOKE SELECT_PRIV ON ${db}.streaming_alter_sql_private FROM '${editor}'"
        connectToDoris(editor, password, url) {
            test {
                sql "ALTER JOB ${jobName} ${joined}"
                exception "denied"
            }
            sql "ALTER JOB ${jobName} PROPERTIES('s3.max_batch_files'='3')"
            sql "ALTER JOB ${jobName} ${original}"
        }
        qt_restored """SELECT Status, Properties, ExecuteSql LIKE '%streaming_alter_sql_private%'
            FROM jobs('type'='insert') WHERE Name='${jobName}'"""
        // Planning an ALTER must not register an INSERT load that will never execute.
        def pendingAfter = (sql "SHOW LOAD FROM ${db} WHERE State = 'PENDING'").size()
        qt_no_pending_loads "SELECT ${pendingAfter - pendingBefore} AS new_pending_loads"
        connectToDoris(owner, password, url) {
            sql "RESUME JOB WHERE jobname = '${jobName}'"
        }
        Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            String.valueOf(sql("SELECT CurrentOffset FROM jobs('type'='insert') WHERE Name='${jobName}'")[0][0])
                    .contains("example_1.csv")
        }
        order_qt_loaded "SELECT * FROM streaming_alter_sql_target"
    } finally {
        sql "DROP JOB IF EXISTS WHERE jobname = '${jobName}'"
        sql "DROP USER IF EXISTS '${editor}'"
        sql "DROP USER IF EXISTS '${owner}'"
    }
}
