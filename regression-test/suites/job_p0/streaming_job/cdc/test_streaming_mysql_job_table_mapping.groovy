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

suite("test_streaming_mysql_job_table_mapping", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName         = "test_streaming_mysql_table_mapping"
    def jobNameMerge    = "test_streaming_mysql_table_mapping_merge"
    def currentDb       = (sql "select database()")[0][0]
    def mysqlSrcTable   = "mysql_src_table"       // upstream MySQL table name
    def dorisDstTable   = "doris_dst_table_mysql" // downstream Doris table name (mapped)
    def mysqlSrcTable2  = "mysql_src_table2"      // second upstream table (multi-table merge)
    def dorisMergeTable = "doris_merge_table_mysql"
    def mysqlDb         = "test_cdc_db_table_mapping"

    // Cleanup
    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP JOB IF EXISTS where jobname = '${jobNameMerge}'"""
    sql """drop table if exists ${currentDb}.${dorisDstTable} force"""
    sql """drop table if exists ${currentDb}.${dorisMergeTable} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port    = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint   = getS3Endpoint()
        String bucket        = getS3BucketName()
        String driver_url    = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // ── Case 1: basic table name mapping ─────────────────────────────────
        // MySQL table: mysql_src_table → Doris table: doris_dst_table_mysql
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${mysqlSrcTable}"""
            sql """CREATE TABLE ${mysqlDb}.${mysqlSrcTable} (
                      `id`   int NOT NULL,
                      `name` varchar(200),
                      PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${mysqlSrcTable} VALUES (1, 'Alice')"""
            sql """INSERT INTO ${mysqlDb}.${mysqlSrcTable} VALUES (2, 'Bob')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url"       = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                    "driver_url"     = "${driver_url}",
                    "driver_class"   = "com.mysql.cj.jdbc.Driver",
                    "user"           = "root",
                    "password"       = "123456",
                    "database"       = "${mysqlDb}",
                    "include_tables" = "${mysqlSrcTable}",
                    "offset"         = "initial",
                    "table.${mysqlSrcTable}.target_table" = "${dorisDstTable}"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        // Verify the Doris table was created with the mapped name, not the source name
        def tables = (sql """show tables from ${currentDb}""").collect { it[0] }
        assert tables.contains(dorisDstTable) : "Doris target table '${dorisDstTable}' should exist"
        assert !tables.contains(mysqlSrcTable) : "Source table name '${mysqlSrcTable}' must NOT exist in Doris"

        // Wait for snapshot
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING'"""
                cnt.size() == 1 && cnt.get(0).get(0).toLong() >= 2
            })
        } catch (Exception ex) {
            log.info("show job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("show task: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        qt_select_snapshot """ SELECT * FROM ${dorisDstTable} ORDER BY id ASC """

        // Incremental: INSERT / UPDATE / DELETE must all land in doris_dst_table_mysql
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${mysqlSrcTable} VALUES (3, 'Carol')"""
            sql """UPDATE ${mysqlDb}.${mysqlSrcTable} SET name = 'Bob_v2' WHERE id = 2"""
            sql """DELETE FROM ${mysqlDb}.${mysqlSrcTable} WHERE id = 1"""
        }
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def ids = (sql """ SELECT id FROM ${dorisDstTable} ORDER BY id ASC """).collect { it[0].toInteger() }
                ids.contains(3) && !ids.contains(1)
            })
        } catch (Exception ex) {
            log.info("show job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("show task: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        qt_select_incremental """ SELECT * FROM ${dorisDstTable} ORDER BY id ASC """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        // ── Case 2: multi-table merge (two MySQL tables → one Doris table) ──
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${mysqlSrcTable}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${mysqlSrcTable2}"""
            sql """CREATE TABLE ${mysqlDb}.${mysqlSrcTable} (
                      `id`   int NOT NULL,
                      `name` varchar(200),
                      PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB"""
            sql """CREATE TABLE ${mysqlDb}.${mysqlSrcTable2} (
                      `id`   int NOT NULL,
                      `name` varchar(200),
                      PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${mysqlSrcTable}  VALUES (100, 'Src1_A')"""
            sql """INSERT INTO ${mysqlDb}.${mysqlSrcTable2} VALUES (200, 'Src2_A')"""
        }

        sql """CREATE JOB ${jobNameMerge}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url"       = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                    "driver_url"     = "${driver_url}",
                    "driver_class"   = "com.mysql.cj.jdbc.Driver",
                    "user"           = "root",
                    "password"       = "123456",
                    "database"       = "${mysqlDb}",
                    "include_tables" = "${mysqlSrcTable},${mysqlSrcTable2}",
                    "offset"         = "initial",
                    "table.${mysqlSrcTable}.target_table"  = "${dorisMergeTable}",
                    "table.${mysqlSrcTable2}.target_table" = "${dorisMergeTable}"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        // Wait for snapshot rows from both source tables
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def ids = (sql """ SELECT id FROM ${dorisMergeTable} """).collect { it[0].toInteger() }
                ids.contains(100) && ids.contains(200)
            })
        } catch (Exception ex) {
            log.info("show job: " + (sql """select * from jobs("type"="insert") where Name='${jobNameMerge}'"""))
            log.info("show task: " + (sql """select * from tasks("type"="insert") where JobName='${jobNameMerge}'"""))
            throw ex
        }

        qt_select_merge_snapshot """ SELECT * FROM ${dorisMergeTable} ORDER BY id ASC """

        // Incremental from both source tables
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${mysqlSrcTable}  VALUES (101, 'Src1_B')"""
            sql """INSERT INTO ${mysqlDb}.${mysqlSrcTable2} VALUES (201, 'Src2_B')"""
        }
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def ids = (sql """ SELECT id FROM ${dorisMergeTable} """).collect { it[0].toInteger() }
                ids.contains(101) && ids.contains(201)
            })
        } catch (Exception ex) {
            log.info("show job: " + (sql """select * from jobs("type"="insert") where Name='${jobNameMerge}'"""))
            log.info("show task: " + (sql """select * from tasks("type"="insert") where JobName='${jobNameMerge}'"""))
            throw ex
        }

        qt_select_merge_incremental """ SELECT * FROM ${dorisMergeTable} ORDER BY id ASC """

        sql """DROP JOB IF EXISTS where jobname = '${jobNameMerge}'"""
        def mergeJobCnt = sql """select count(1) from jobs("type"="insert") where Name = '${jobNameMerge}'"""
        assert mergeJobCnt.get(0).get(0) == 0
    }
}
