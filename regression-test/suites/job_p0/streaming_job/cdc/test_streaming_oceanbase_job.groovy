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

suite("test_streaming_oceanbase_job",
        "p0,external,oceanbase,external_docker,external_docker_oceanbase,nondatalake") {
    def jobName = "test_streaming_oceanbase_job"
    def currentDb = (sql "SELECT DATABASE()")[0][0]
    def sourceDb = "test_oceanbase_streaming_db"
    def table1 = "oceanbase_streaming_users"
    def table2 = "oceanbase_streaming_orders"
    def emptyTable = "oceanbase_streaming_empty"

    sql """DROP JOB IF EXISTS WHERE jobname='${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${table1} FORCE"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${table2} FORCE"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${emptyTable} FORCE"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String oceanbaseCdcPort = context.config.otherConfigs.get("oceanbase_cdc_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl =
                "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        def dumpJobState = {
            log.info("jobs: " + sql("""SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks: " + sql("""SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
        }

        connect("root@test", "123456", "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}") {
            sql """CREATE DATABASE IF NOT EXISTS ${sourceDb}"""
            sql """DROP TABLE IF EXISTS ${sourceDb}.${table1}"""
            sql """DROP TABLE IF EXISTS ${sourceDb}.${table2}"""
            sql """DROP TABLE IF EXISTS ${sourceDb}.${emptyTable}"""
            sql """CREATE TABLE ${sourceDb}.${table1} (
                        id INT NOT NULL,
                        name VARCHAR(100),
                        age INT,
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            sql """CREATE TABLE ${sourceDb}.${table2} (
                        id INT NOT NULL,
                        description VARCHAR(100),
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            sql """CREATE TABLE ${sourceDb}.${emptyTable} (
                        id INT NOT NULL,
                        value VARCHAR(100),
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${sourceDb}.${table1} VALUES
                        (1, 'Alice', 18),
                        (2, 'Bob', 20)"""
            sql """INSERT INTO ${sourceDb}.${table2} VALUES (10, 'snapshot_order')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM OCEANBASE (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root@test",
                    "password" = "123456",
                    "database" = "${sourceDb}",
                    "include_tables" = "${table1},${table2},${emptyTable}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def users = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                def orders = sql """SELECT COUNT(*) FROM ${currentDb}.${table2}"""
                def emptyTables = sql """SHOW TABLES FROM ${currentDb} LIKE '${emptyTable}'"""
                users[0][0] == 2 && orders[0][0] == 1 && emptyTables.size() == 1
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        order_qt_oceanbase_snapshot_users """
            SELECT id, name, age FROM ${currentDb}.${table1} ORDER BY id
        """
        order_qt_oceanbase_snapshot_orders """
            SELECT id, description FROM ${currentDb}.${table2} ORDER BY id
        """
        order_qt_oceanbase_snapshot_empty """
            SELECT id, value FROM ${currentDb}.${emptyTable} ORDER BY id
        """

        connect("root@test", "123456", "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}") {
            sql """INSERT INTO ${sourceDb}.${table1} VALUES (3, 'Carol', 30)"""
            sql """UPDATE ${sourceDb}.${table1} SET age=21 WHERE id=2"""
            sql """DELETE FROM ${sourceDb}.${table1} WHERE id=1"""
            sql """INSERT INTO ${sourceDb}.${table2} VALUES (11, 'incremental_order')"""
        }

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def users = sql """SELECT id, name, age FROM ${currentDb}.${table1} ORDER BY id"""
                def orders = sql """SELECT id, description FROM ${currentDb}.${table2} ORDER BY id"""
                users == [[2, 'Bob', 21], [3, 'Carol', 30]] &&
                        orders == [[10, 'snapshot_order'], [11, 'incremental_order']]
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        order_qt_oceanbase_incremental_users """
            SELECT id, name, age FROM ${currentDb}.${table1} ORDER BY id
        """
        order_qt_oceanbase_incremental_orders """
            SELECT id, description FROM ${currentDb}.${table2} ORDER BY id
        """

        def status = sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
        assert status.size() == 1 && status[0][0] == "RUNNING"
        sql """DROP JOB IF EXISTS WHERE jobname='${jobName}'"""
    }
}
