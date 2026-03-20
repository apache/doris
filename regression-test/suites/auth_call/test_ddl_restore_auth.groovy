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

import java.util.UUID

suite("test_ddl_restore_auth", "p0,auth_call") {
    String label = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 8)
    def syncer = getSyncer()

    def waitingBackupTaskFinished = { String curDbName ->
        String showTasks = "SHOW BACKUP FROM ${curDbName};"
        String status = "NULL"
        long timeoutTimestamp = System.currentTimeMillis() + 5 * 60 * 1000 // 5 min
        while (timeoutTimestamp > System.currentTimeMillis()) {
            List<List<Object>> result = sql(showTasks)
            assertTrue(result.size() == 1)
            status = result.last().get(3)
            if (status == "FINISHED") {
                return
            }
            Thread.sleep(1000)
        }
        assertTrue(status == "FINISHED")
    }

    String user = 'test_ddl_restore_auth_user'
    String pwd = 'C123_567p'
    String dbName = "test_ddl_restore_auth_db_${label}"
    String tableName = 'test_ddl_restore_auth_tb'
    String repositoryName = "test_ddl_restore_auth_rps_${label}"
    String restoreLabelName = "test_ddl_restore_auth_restore_label_${label}"

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    sql """create table ${dbName}.${tableName} (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""
    sql """
        insert into ${dbName}.`${tableName}` values 
        (1, "111"),
        (2, "222"),
        (3, "333");
        """

    syncer.createS3Repository(repositoryName)
    sql """BACKUP SNAPSHOT ${dbName}.${restoreLabelName}
                    TO ${repositoryName}
                    ON (${tableName})
                    PROPERTIES ("type" = "full");"""
    waitingBackupTaskFinished(dbName)
    def real_timestamp = syncer.getSnapshotTimestamp(repositoryName, restoreLabelName)
    assertTrue(real_timestamp != null)

    sql """truncate table ${dbName}.`${tableName}`"""

    sql """grant admin_PRIV on *.*.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        def show_snapshot_res = sql """SHOW SNAPSHOT ON ${repositoryName};"""
        logger.info("show_snapshot_res: " + show_snapshot_res)
    }
    sql """revoke admin_PRIV on *.*.* from ${user}"""

    // ddl create,show,drop
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql """SHOW SNAPSHOT ON ${repositoryName};"""
            exception "denied"
        }
        test {
            sql """RESTORE SNAPSHOT ${dbName}.`${restoreLabelName}`
                    FROM `${repositoryName}`
                    ON ( `${tableName}` )
                    PROPERTIES
                    (
                        "backup_timestamp"="${real_timestamp}",
                        "replication_num" = "1"
                    );"""
            exception "denied"
        }
        test {
            sql """CANCEL RESTORE FROM ${dbName};"""
            exception "denied"
        }
        test {
            sql """SHOW RESTORE FROM ${dbName};"""
            exception "denied"
        }
    }
    sql """grant LOAD_PRIV on ${dbName}.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """RESTORE SNAPSHOT ${dbName}.`${restoreLabelName}`
                FROM `${repositoryName}`
                ON ( `${tableName}` )
                PROPERTIES
                (
                    "backup_timestamp"="${real_timestamp}",
                    "replication_num" = "1"
                );"""
        def res = sql """SHOW RESTORE FROM ${dbName};"""
        logger.info("res: " + res)
        assertTrue(res.size() == 1)

        sql """CANCEL RESTORE FROM ${dbName};"""
        res = sql """SHOW RESTORE FROM ${dbName};"""
        logger.info("res: " + res)
        assertTrue(res[0][4] == "CANCELLED")
    }

    try_sql("""DROP REPOSITORY `${repositoryName}`;""")
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
