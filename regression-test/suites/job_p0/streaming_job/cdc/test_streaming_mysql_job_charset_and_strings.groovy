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

// Guard MySQL string column fidelity across cdc snapshot + binlog paths for:
//   - Per-column non-default charsets (gbk, latin1) in a single table.
//   - emoji / 4-byte UTF-8 on utf8mb4 columns.
//   - Special characters in string literals: \n, \r, \t, single/double quote,
//     backslash. Built via CONCAT()+CHAR() to dodge multi-layer SQL escape.
//   - Large TEXT field (~100KB) — sanity check that a single row above the
//     "small" size still streams correctly.
//
// Same themes run twice: ids 1-6 via JDBC/snapshot, ids 101-106 via binlog.
// Plus UPDATEs that switch a row's payload through binlog.
//
// The streaming job URL omits Unicode and character encoding options to verify
// that the MySQL defaults are added before metadata discovery and snapshot reads.
suite("test_streaming_mysql_job_charset_and_strings", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_charset_and_strings_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_charset_strings_pk"
    def mysqlDb = "test_cdc_charset_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // ===== Prepare MySQL side =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?useUnicode=true&characterEncoding=utf8") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb} DEFAULT CHARACTER SET utf8mb4"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """
            create table ${mysqlDb}.${table1} (
              `id` int primary key,
              `tag` varchar(64) character set utf8mb4,
              `gbk_col` varchar(255) character set gbk,
              `latin1_col` varchar(255) character set latin1,
              `utf8mb4_col` varchar(255) character set utf8mb4,
              `special_chars` varchar(500) character set utf8mb4,
              `long_text` mediumtext character set utf8mb4
            ) engine=innodb;
            """

            // ----- Snapshot rows: 6 charset/string themes via JDBC path -----
            // basic ASCII baseline
            sql """insert into ${mysqlDb}.${table1} values (1, 'basic_ascii',   'hello', 'hello', 'hello', 'normal', 'short')"""

            // gbk column with Chinese characters; MySQL converts utf8 input to gbk on insert.
            sql """insert into ${mysqlDb}.${table1} (id, tag, gbk_col) values (2, 'chinese_gbk', '中文GBK测试')"""

            // latin1 column with Western European characters.
            sql """insert into ${mysqlDb}.${table1} (id, tag, latin1_col) values (3, 'latin1_chars', 'café résumé naïve')"""

            // utf8mb4 with 4-byte sequences (emoji).
            sql """insert into ${mysqlDb}.${table1} (id, tag, utf8mb4_col) values (4, 'emoji_4byte', 'Hello 🚀 🎉 😀 你好')"""

            // Special characters constructed via CHAR() to bypass SQL escape stacking.
            //   CHAR(9)=tab, CHAR(10)=LF, CHAR(13)=CR, CHAR(34)=double-quote,
            //   CHAR(39)=single-quote, CHAR(92)=backslash
            sql """insert into ${mysqlDb}.${table1} (id, tag, special_chars) values (5, 'special_chars',
                CONCAT('line1', CHAR(10), 'line2',
                       CHAR(13), CHAR(10), 'crlf',
                       CHAR(9), 'tab',
                       CHAR(34), 'dquote', CHAR(34),
                       CHAR(39), 'squote', CHAR(39),
                       CHAR(92), 'back')
            )"""

            // 100KB single-cell long_text. Avoid 1MB+ to keep regression fast.
            sql """insert into ${mysqlDb}.${table1} (id, tag, long_text) values (6, 'large_text', REPEAT('x', 102400))"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Wait for snapshot to land all 6 rows in Doris.
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 6
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_desc_charset_strings """desc ${currentDb}.${table1};"""
        // Exclude long_text from select to keep .out diff readable; verify its length separately.
        qt_select_snapshot """select id, tag, gbk_col, latin1_col, utf8mb4_col, special_chars from ${currentDb}.${table1} order by id;"""
        qt_select_long_text_len """select id, length(long_text) from ${currentDb}.${table1} where id=6;"""

        // ===== Binlog phase: repeat the SAME 6 themes through binlog path =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?useUnicode=true&characterEncoding=utf8") {
            sql """insert into ${mysqlDb}.${table1} values (101, 'basic_ascii',   'world', 'world', 'world', 'plain', 'tiny')"""
            sql """insert into ${mysqlDb}.${table1} (id, tag, gbk_col) values (102, 'chinese_gbk', '增量中文测试')"""
            sql """insert into ${mysqlDb}.${table1} (id, tag, latin1_col) values (103, 'latin1_chars', 'naïveté Zürich')"""
            sql """insert into ${mysqlDb}.${table1} (id, tag, utf8mb4_col) values (104, 'emoji_4byte', 'Goodbye 👋 🌍')"""
            sql """insert into ${mysqlDb}.${table1} (id, tag, special_chars) values (105, 'special_chars',
                CONCAT('incr_line', CHAR(10),
                       CHAR(34), 'q', CHAR(34),
                       CHAR(92), 'b')
            )"""
            sql """insert into ${mysqlDb}.${table1} (id, tag, long_text) values (106, 'large_text', REPEAT('y', 102400))"""

            // UPDATEs: validate UPDATE binlog parsing on charset/special-char columns.
            //   id=2 (gbk) -> change Chinese content via UPDATE
            //   id=4 (emoji) -> swap emoji set
            //   id=5 (special) -> append more special chars
            sql """update ${mysqlDb}.${table1} set gbk_col='更新的中文' where id=2"""
            sql """update ${mysqlDb}.${table1} set utf8mb4_col=CONCAT('Updated ', '🎊') where id=4"""
            sql """update ${mysqlDb}.${table1} set special_chars=CONCAT('upd_', CHAR(10), CHAR(9), 'after') where id=5"""
        }

        // Wait until all 6 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd2 = sql """select gbk_col from ${currentDb}.${table1} where id=2"""
                        def upd4 = sql """select utf8mb4_col from ${currentDb}.${table1} where id=4"""
                        def upd5 = sql """select special_chars from ${currentDb}.${table1} where id=5"""
                        def g2 = upd2.get(0).get(0) == null ? '' : upd2.get(0).get(0).toString()
                        def u4 = upd4.get(0).get(0) == null ? '' : upd4.get(0).get(0).toString()
                        def s5 = upd5.get(0).get(0) == null ? '' : upd5.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " id2.gbk=" + g2 + " id4.utf=" + u4 + " id5.special=" + s5)
                        cnt.get(0).get(0) == 12 &&
                                g2.contains('更新') &&
                                u4.startsWith('Updated') &&
                                s5.startsWith('upd_')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, gbk_col, latin1_col, utf8mb4_col, special_chars from ${currentDb}.${table1} order by id;"""
        qt_select_long_text_len_after """select id, length(long_text) from ${currentDb}.${table1} where id in (6, 106) order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
