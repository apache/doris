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

/**
 * Verify that a MySQL cdc_stream job uses numeric TINYINT(1) and YEAR semantics
 * consistently while fetching snapshot splits and scanning snapshot/binlog rows.
 */
suite("test_streaming_job_cdc_stream_mysql_jdbc_type_defaults",
        "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_cdc_stream_mysql_jdbc_defaults_job"
    def currentDb = (sql "select database()")[0][0]

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.test_cdc_stream_mysql_jdbc_defaults_tbl FORCE"""

    sql """
        CREATE TABLE ${currentDb}.test_cdc_stream_mysql_jdbc_defaults_tbl (
            `name` varchar(32) NULL,
            `tinyint_value` tinyint NULL,
            `year_value` smallint NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """CREATE DATABASE IF NOT EXISTS test_cdc_db"""
            sql """DROP TABLE IF EXISTS test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src"""
            sql """
                CREATE TABLE test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src (
                    `name` varchar(32) NOT NULL,
                    `tinyint_value` tinyint(1) NOT NULL,
                    `year_value` year NOT NULL
                ) ENGINE=InnoDB
            """
            sql """INSERT INTO test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src VALUES ('A1', -1, 0)"""
            sql """INSERT INTO test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src VALUES ('B1', 0, 2024)"""
            sql """INSERT INTO test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src VALUES ('C1', 2, 0)"""
            sql """INSERT INTO test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src VALUES ('D1', 4, 2025)"""
        }

        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.test_cdc_stream_mysql_jdbc_defaults_tbl
                (name, tinyint_value, year_value)
            SELECT name, tinyint_value, year_value FROM cdc_stream(
                "type"                 = "mysql",
                "jdbc_url"             = "jdbc:mysql://${externalEnvIp}:${mysqlPort}",
                "driver_url"           = "${driverUrl}",
                "driver_class"         = "com.mysql.cj.jdbc.Driver",
                "user"                 = "root",
                "password"             = "123456",
                "database"             = "test_cdc_db",
                "table"                = "test_cdc_stream_mysql_jdbc_defaults_src",
                "offset"               = "initial",
                "snapshot_split_key"   = "tinyint_value",
                "snapshot_split_size"  = "1"
            )
        """

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT count(1) FROM ${currentDb}.test_cdc_stream_mysql_jdbc_defaults_tbl"""
                log.info("snapshot rows: " + rows)
                (rows.get(0).get(0) as int) == 4
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        order_qt_snapshot_data """SELECT * FROM ${currentDb}.test_cdc_stream_mysql_jdbc_defaults_tbl"""

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """INSERT INTO test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src VALUES ('E1', 127, 0)"""
            sql """INSERT INTO test_cdc_db.test_cdc_stream_mysql_jdbc_defaults_src VALUES ('F1', -128, 2026)"""
        }

        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """
                    SELECT count(1) FROM ${currentDb}.test_cdc_stream_mysql_jdbc_defaults_tbl
                    WHERE name IN ('E1', 'F1')
                """
                log.info("incremental rows: " + rows)
                (rows.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        order_qt_final_data """SELECT * FROM ${currentDb}.test_cdc_stream_mysql_jdbc_defaults_tbl"""
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
