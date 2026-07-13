// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_oceanbase_job_all_type",
        "p0,external,oceanbase,external_docker,external_docker_oceanbase,nondatalake") {
    def jobName = "test_streaming_oceanbase_job_all_type"
    def currentDb = (sql "SELECT DATABASE()")[0][0]
    def sourceDb = "test_oceanbase_streaming_db"
    def table1 = "oceanbase_streaming_all_type"

    sql """DROP JOB IF EXISTS WHERE jobname='${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${table1} FORCE"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String oceanbaseCdcPort = context.config.otherConfigs.get("oceanbase_cdc_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl =
                "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
        String sourceUrl = "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}?serverTimezone=UTC"

        def dumpJobState = {
            log.info("jobs: " + sql("""SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks: " + sql("""SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
        }

        connect("root@test", "123456", sourceUrl) {
            sql """CREATE DATABASE IF NOT EXISTS ${sourceDb}"""
            sql """DROP TABLE IF EXISTS ${sourceDb}.${table1}"""
            sql """CREATE TABLE ${sourceDb}.${table1} (
                        id INT NOT NULL,
                        tiny_unsigned TINYINT UNSIGNED,
                        big_unsigned BIGINT UNSIGNED,
                        amount DECIMAL(18, 5),
                        enabled BOOLEAN,
                        event_date DATE,
                        event_datetime DATETIME(6),
                        event_timestamp TIMESTAMP(6) NULL,
                        fixed_text CHAR(5),
                        variable_text VARCHAR(32),
                        long_text TEXT,
                        binary_value BLOB,
                        json_value JSON,
                        set_value SET('A', 'B', 'C'),
                        enum_value ENUM('NEW', 'DONE'),
                        varbinary_value VARBINARY(16),
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${sourceDb}.${table1} VALUES (
                        1, 200, 18446744073709551610, 123456789.12345, TRUE,
                        '2026-07-10', '2026-07-10 10:11:12.123456',
                        '2026-07-10 10:11:12.123456', 'abc', 'snapshot',
                        'snapshot text', X'010203', '{"id":1,"name":"snapshot"}',
                        'A,C', 'NEW', X'0A0B0C'
                    )"""
            sql """INSERT INTO ${sourceDb}.${table1} VALUES (
                        2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL
                    )"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM OCEANBASE (
                    "jdbc_url" = "${sourceUrl}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root@test",
                    "password" = "123456",
                    "database" = "${sourceDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                rows.size() == 1 && rows[0][0] == 2
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        order_qt_oceanbase_all_type_snapshot """
            SELECT id, tiny_unsigned, big_unsigned, amount, enabled, event_date,
                   event_datetime, event_timestamp, fixed_text, variable_text,
                   long_text, HEX(binary_value), CAST(json_value AS STRING),
                   set_value, enum_value, HEX(varbinary_value)
            FROM ${currentDb}.${table1}
            ORDER BY id
        """

        connect("root@test", "123456", sourceUrl) {
            sql """INSERT INTO ${sourceDb}.${table1} VALUES (
                        3, 100, 9000000000, -98765.43210, FALSE,
                        '2026-07-11', '2026-07-11 11:12:13.654321',
                        '2026-07-11 11:12:13.654321', 'xyz', 'incremental',
                        'incremental text', X'FFEEDD', '{"id":3,"name":"incremental"}',
                        'B', 'DONE', X'ABCDEF'
                    )"""
            sql """UPDATE ${sourceDb}.${table1}
                    SET variable_text='updated', amount=1.25000 WHERE id=1"""
            sql """DELETE FROM ${sourceDb}.${table1} WHERE id=2"""
        }

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def count = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                def updated = sql """SELECT variable_text FROM ${currentDb}.${table1} WHERE id=1"""
                def inserted = sql """SELECT COUNT(*) FROM ${currentDb}.${table1} WHERE id=3"""
                count[0][0] == 2 && updated.size() == 1 && updated[0][0] == 'updated' &&
                        inserted[0][0] == 1
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        order_qt_oceanbase_all_type_incremental """
            SELECT id, tiny_unsigned, big_unsigned, amount, enabled, event_date,
                   event_datetime, event_timestamp, fixed_text, variable_text,
                   long_text, HEX(binary_value), CAST(json_value AS STRING),
                   set_value, enum_value, HEX(varbinary_value)
            FROM ${currentDb}.${table1}
            ORDER BY id
        """

        sql """DROP JOB IF EXISTS WHERE jobname='${jobName}'"""
    }
}
