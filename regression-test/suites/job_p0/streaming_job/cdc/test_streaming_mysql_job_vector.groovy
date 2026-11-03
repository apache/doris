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

suite("test_streaming_mysql_job_vector", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    // Enable for MySQL 9.x or PolarDB instances with VECTOR support.
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMysqlVectorTest"))) {
        log.info("Skip MySQL VECTOR test: enableMysqlVectorTest is not true")
        return
    }
    def currentDb = (sql "select database()")[0][0]
    def mysqlDb = "test_cdc_db"
    String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String jdbcUrl = "jdbc:mysql://${externalEnvIp}:${mysqlPort}?serverTimezone=UTC"
    String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

    sql "DROP JOB IF EXISTS where jobname='mysql_vector_columns_job'"
    sql "DROP JOB IF EXISTS where jobname='mysql_vector_add_columns_job'"
    sql "DROP TABLE IF EXISTS mysql_vector_columns FORCE"
    sql "DROP TABLE IF EXISTS mysql_vector_add_columns FORCE"

    try {
        connect("root", "123456", jdbcUrl) {
            sql "CREATE DATABASE IF NOT EXISTS ${mysqlDb}"
            sql "DROP TABLE IF EXISTS ${mysqlDb}.mysql_vector_columns"
            sql """CREATE TABLE ${mysqlDb}.mysql_vector_columns (
                id INT PRIMARY KEY,
                info JSON,
                virtual_vector VECTOR(4) GENERATED ALWAYS AS (TO_VECTOR(info ->> '\$.a1')) VIRTUAL,
                stored_vector VECTOR(4) GENERATED ALWAYS AS (TO_VECTOR(info ->> '\$.a1')) STORED,
                marker INT,
                plain_vector VECTOR(2),
                tail VARCHAR(20)
            )"""
            sql """INSERT INTO ${mysqlDb}.mysql_vector_columns(id,info,marker,plain_vector,tail)
                VALUES (1,'{"a1":[1.25,-2.5,0,4]}',42,TO_VECTOR('[0.5,-0.5]'),'tail'),
                       (2,NULL,42,NULL,'tail')"""
            sql "DROP TABLE IF EXISTS ${mysqlDb}.mysql_vector_add_columns"
            sql """CREATE TABLE ${mysqlDb}.mysql_vector_add_columns (
                id INT PRIMARY KEY, info JSON, marker INT, tail VARCHAR(20)
            )"""
            sql """INSERT INTO ${mysqlDb}.mysql_vector_add_columns VALUES
                (1,'{"a1":[1.25,-2.5,0,4]}',42,'tail'),(2,NULL,42,'tail')"""
        }

        def createJob = { String job, String table ->
            sql """CREATE JOB ${job}
                PROPERTIES("max_interval"="1") ON STREAMING
                FROM MYSQL (
                    "jdbc_url"="${jdbcUrl}",
                    "driver_url"="${driverUrl}",
                    "driver_class"="com.mysql.cj.jdbc.Driver",
                    "user"="root", "password"="123456",
                    "database"="${mysqlDb}", "include_tables"="${table}", "offset"="initial"
                ) TO DATABASE ${currentDb} ("table.create.properties.replication_num"="1")"""
        }
        def waitFor = { Closure condition ->
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until(condition)
        }

        createJob("mysql_vector_columns_job", "mysql_vector_columns")
        waitFor {
            def rows = sql "SELECT COUNT(*) FROM mysql_vector_columns"
            rows[0][0] == 2
        }
        qt_existing_column_types """SELECT column_name, column_type FROM information_schema.columns
            WHERE table_schema='${currentDb}' AND table_name='mysql_vector_columns'
              AND column_name IN ('virtual_vector','plain_vector','stored_vector') ORDER BY column_name"""
        qt_existing_columns_snapshot "SELECT id,virtual_vector,stored_vector,plain_vector,marker,tail FROM mysql_vector_columns ORDER BY id"

        connect("root", "123456", jdbcUrl) {
            sql """INSERT INTO ${mysqlDb}.mysql_vector_columns(id,info,marker,plain_vector,tail)
                VALUES (3,'{"a1":[1.25,-2.5,0,4]}',42,TO_VECTOR('[0.5,-0.5]'),'tail')"""
            sql """UPDATE ${mysqlDb}.mysql_vector_columns
                SET info='{"a1":[5,-6.25,7.5,8]}',plain_vector=TO_VECTOR('[-0.75,0.25]') WHERE id=1"""
            sql "DELETE FROM ${mysqlDb}.mysql_vector_columns WHERE id=2"
            sql "INSERT INTO ${mysqlDb}.mysql_vector_columns(id,marker,tail) VALUES(4,42,'tail')"
        }
        waitFor {
            def rows = sql """SELECT COUNT(*), SUM(IF(id=1 AND virtual_vector[2]=-6.25 AND stored_vector[2]=-6.25 AND plain_vector[1]=-0.75,1,0)),
                SUM(IF(id=2,1,0)),SUM(IF(id=4 AND virtual_vector IS NULL AND stored_vector IS NULL AND plain_vector IS NULL,1,0))
                FROM mysql_vector_columns"""
            rows[0][0] == 3 && rows[0][1] == 1 && rows[0][2] == 0 && rows[0][3] == 1
        }
        qt_existing_columns_incremental "SELECT id,virtual_vector,stored_vector,plain_vector,marker,tail FROM mysql_vector_columns ORDER BY id"
        sql "DROP JOB IF EXISTS where jobname='mysql_vector_columns_job'"

        createJob("mysql_vector_add_columns_job", "mysql_vector_add_columns")
        waitFor { (sql "SELECT COUNT(*) FROM mysql_vector_add_columns")[0][0] == 2 }
        qt_before_add_columns_snapshot "SELECT id,marker,tail FROM mysql_vector_add_columns ORDER BY id"
        connect("root", "123456", jdbcUrl) {
            sql "UPDATE ${mysqlDb}.mysql_vector_add_columns SET tail='binlog' WHERE id=1"
        }
        waitFor { (sql "SELECT COUNT(*) FROM mysql_vector_add_columns WHERE id=1 AND tail='binlog'")[0][0] == 1 }
        qt_before_add_columns_incremental "SELECT id,marker,tail FROM mysql_vector_add_columns ORDER BY id"

        connect("root", "123456", jdbcUrl) {
            sql """ALTER TABLE ${mysqlDb}.mysql_vector_add_columns ADD COLUMN virtual_vector VECTOR(4)
                GENERATED ALWAYS AS (TO_VECTOR(info ->> '\$.a1')) VIRTUAL AFTER info"""
            sql """ALTER TABLE ${mysqlDb}.mysql_vector_add_columns ADD COLUMN stored_vector VECTOR(4)
                GENERATED ALWAYS AS (TO_VECTOR(info ->> '\$.a1')) STORED AFTER virtual_vector"""
            sql """ALTER TABLE ${mysqlDb}.mysql_vector_add_columns ADD COLUMN plain_vector VECTOR(4)
                AFTER stored_vector"""
        }
        waitFor {
            def names = (sql "DESC mysql_vector_add_columns").collect { it[0] }
            names.containsAll(['virtual_vector', 'stored_vector', 'plain_vector'])
        }
        qt_added_column_types """SELECT column_name, column_type FROM information_schema.columns
            WHERE table_schema='${currentDb}' AND table_name='mysql_vector_add_columns'
              AND column_name IN ('virtual_vector','stored_vector','plain_vector') ORDER BY column_name"""
        connect("root", "123456", jdbcUrl) {
            sql """UPDATE ${mysqlDb}.mysql_vector_add_columns
                SET plain_vector=TO_VECTOR('[0.5,-0.5,2,-2]') WHERE id=1"""
        }
        waitFor { (sql "SELECT COUNT(*) FROM mysql_vector_add_columns WHERE id=1 AND plain_vector[1]=0.5")[0][0] == 1 }
        qt_added_plain_vector_before_update "SELECT id,plain_vector FROM mysql_vector_add_columns WHERE id=1 ORDER BY id"
        connect("root", "123456", jdbcUrl) {
            sql """INSERT INTO ${mysqlDb}.mysql_vector_add_columns(id,info,marker,tail,plain_vector)
                VALUES(3,'{"a1":[1.25,-2.5,0,4]}',42,'tail',TO_VECTOR('[0.5,-0.5,2,-2]'))"""
            sql """UPDATE ${mysqlDb}.mysql_vector_add_columns
                SET info='{"a1":[5,-6.25,7.5,8]}',plain_vector=TO_VECTOR('[9,8,7,6]'),tail='tail' WHERE id=1"""
            sql "DELETE FROM ${mysqlDb}.mysql_vector_add_columns WHERE id=2"
            sql "INSERT INTO ${mysqlDb}.mysql_vector_add_columns(id,marker,tail) VALUES(4,42,'tail')"
        }
        waitFor {
            def rows = sql """SELECT COUNT(*),SUM(IF(id=1 AND virtual_vector[2]=-6.25 AND stored_vector[2]=-6.25 AND plain_vector[1]=9,1,0)),
                SUM(IF(id=2,1,0)),SUM(IF(id=4 AND virtual_vector IS NULL AND stored_vector IS NULL AND plain_vector IS NULL,1,0)) FROM mysql_vector_add_columns"""
            rows[0][0] == 3 && rows[0][1] == 1 && rows[0][2] == 0 && rows[0][3] == 1
        }
        qt_added_columns_incremental "SELECT id,virtual_vector,stored_vector,plain_vector,marker,tail FROM mysql_vector_add_columns ORDER BY id"
    } finally {
        sql "DROP JOB IF EXISTS where jobname='mysql_vector_columns_job'"
        sql "DROP JOB IF EXISTS where jobname='mysql_vector_add_columns_job'"
    }

}
