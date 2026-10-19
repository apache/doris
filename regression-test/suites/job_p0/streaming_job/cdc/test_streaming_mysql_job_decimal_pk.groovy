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

suite("test_streaming_mysql_job_decimal_pk", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_decimal_pk_name"
    def currentDb = (sql "select database()")[0][0]
    def tableDecimal = "streaming_decimal_pk_mysql"
    def tableBigintUnsigned = "streaming_bigint_unsigned_pk_mysql"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableDecimal} force"""
    sql """drop table if exists ${currentDb}.${tableBigintUnsigned} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableDecimal}"""
            sql """CREATE TABLE ${mysqlDb}.${tableDecimal} (
                  `amount` decimal(20,4) NOT NULL,
                  `payload` varchar(64),
                  PRIMARY KEY (`amount`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${tableDecimal} VALUES (1.0000, 'A1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDecimal} VALUES (2.5000, 'B1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDecimal} VALUES (3.7500, 'C1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDecimal} VALUES (4.1234, 'D1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDecimal} VALUES (9999999999999.9999, 'E1')"""

            sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableBigintUnsigned}"""
            sql """CREATE TABLE ${mysqlDb}.${tableBigintUnsigned} (
                  `id` bigint unsigned NOT NULL,
                  `payload` varchar(64),
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${tableBigintUnsigned} VALUES (0, 'zero')"""
            sql """INSERT INTO ${mysqlDb}.${tableBigintUnsigned} VALUES (1, 'one')"""
            sql """INSERT INTO ${mysqlDb}.${tableBigintUnsigned} VALUES (9223372036854775807, 'signed_max')"""
            sql """INSERT INTO ${mysqlDb}.${tableBigintUnsigned} VALUES (9223372036854775808, 'signed_max_plus1')"""
            sql """INSERT INTO ${mysqlDb}.${tableBigintUnsigned} VALUES (18446744073709551615, 'unsigned_max')"""
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
                    "include_tables" = "${tableDecimal},${tableBigintUnsigned}",
                    "offset" = "initial",
                    "snapshot_split_size" = "2"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def c1 = sql """select count(1) from ${currentDb}.${tableDecimal}"""
                        def c2 = sql """select count(1) from ${currentDb}.${tableBigintUnsigned}"""
                        log.info("snapshot row count decimal=${c1} bigint_unsigned=${c2}")
                        c1.get(0).get(0) == 5 && c2.get(0).get(0) == 5
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_select_snapshot_decimal """select amount, payload from ${currentDb}.${tableDecimal} order by amount asc"""
        qt_select_snapshot_bigint_unsigned """select id, payload from ${currentDb}.${tableBigintUnsigned} order by id asc"""

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${tableDecimal} VALUES (5.5555, 'F2')"""
            sql """UPDATE ${mysqlDb}.${tableDecimal} SET payload='B2_upd' WHERE amount=2.5000"""
            sql """DELETE FROM ${mysqlDb}.${tableDecimal} WHERE amount=4.1234"""

            sql """INSERT INTO ${mysqlDb}.${tableBigintUnsigned} VALUES (10000000000000000000, 'incr_huge')"""
            sql """UPDATE ${mysqlDb}.${tableBigintUnsigned} SET payload='signed_max_upd' WHERE id=9223372036854775807"""
            sql """DELETE FROM ${mysqlDb}.${tableBigintUnsigned} WHERE id=9223372036854775808"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def c1 = sql """select count(1) from ${currentDb}.${tableDecimal}"""
                        def c2 = sql """select count(1) from ${currentDb}.${tableBigintUnsigned}"""
                        def upd1 = sql """select payload from ${currentDb}.${tableDecimal} where amount=2.5000"""
                        def upd2 = sql """select payload from ${currentDb}.${tableBigintUnsigned} where id=9223372036854775807"""
                        def del1 = sql """select count(1) from ${currentDb}.${tableDecimal} where amount=4.1234"""
                        def del2 = sql """select count(1) from ${currentDb}.${tableBigintUnsigned} where id=9223372036854775808"""
                        def p1 = upd1.size() == 0 ? null : upd1.get(0).get(0)
                        def p2 = upd2.size() == 0 ? null : upd2.get(0).get(0)
                        log.info("incr decimal=${c1} bigint_unsigned=${c2} dec_upd=${p1} bui_upd=${p2} dec_del=${del1} bui_del=${del2}")
                        c1.get(0).get(0) == 5 && c2.get(0).get(0) == 5 &&
                                p1 == 'B2_upd' && p2 == 'signed_max_upd' &&
                                del1.get(0).get(0) == 0 && del2.get(0).get(0) == 0
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr_decimal """select amount, payload from ${currentDb}.${tableDecimal} order by amount asc"""
        qt_select_after_incr_bigint_unsigned """select id, payload from ${currentDb}.${tableBigintUnsigned} order by id asc"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
