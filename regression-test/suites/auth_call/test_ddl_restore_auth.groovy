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

import org.junit.Assert;
import java.util.UUID

suite("test_ddl_restore_auth","p0,auth_call") {
    UUID uuid = UUID.randomUUID()
    String randomValue = uuid.toString()
    int hashCode = randomValue.hashCode()
    hashCode = hashCode > 0 ? hashCode : hashCode * (-1)

    def waitingBackupTaskFinished = { def curDbName ->
        Thread.sleep(2000)
        String showTasks = "SHOW BACKUP FROM ${curDbName};"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            assertTrue(result.size() == 1)
            if (!result.isEmpty()) {
                status = result.last().get(3)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status != 'FINISHED'))
        if (status != "FINISHED") {
            logger.info("status is not success")
        }
        assertTrue(status == "FINISHED")
        return result[0][1]
    }

    String user = 'test_ddl_restore_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_restore_auth_db'
    String tableName = 'test_ddl_restore_auth_tb'
    String repositoryName = 'test_ddl_restore_auth_rps'
    String restoreLabelName = 'test_ddl_restore_auth_restore_label' + hashCode.toString()

    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql("""DROP REPOSITORY `${repositoryName}`;""")
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

    sql """CREATE REPOSITORY `${repositoryName}`
            WITH S3
            ON LOCATION "s3://${bucket}/${repositoryName}"
            PROPERTIES
            (
                "s3.endpoint" = "http://${endpoint}",
                "s3.region" = "${region}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "delete_if_exists" = "true"
            )"""
    sql """BACKUP SNAPSHOT ${dbName}.${restoreLabelName}
                    TO ${repositoryName}
                    ON (${tableName})
                    PROPERTIES ("type" = "full");"""
    def real_label = waitingBackupTaskFinished(dbName)
    def backup_timestamp = sql """SHOW SNAPSHOT ON ${repositoryName};"""
    logger.info("backup_timestamp: " + backup_timestamp)
    def real_timestamp
    for (int i = 0; i < backup_timestamp.size(); i++) {
        if (backup_timestamp[i][0] == real_label) {
            real_timestamp = backup_timestamp[i][1]
            break
        }
    }

    sql """truncate table ${dbName}.`${tableName}`"""

    sql """grant admin_PRIV on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        def show_snapshot_res = sql """SHOW SNAPSHOT ON ${repositoryName};"""
        logger.info("show_snapshot_res: " + show_snapshot_res)
    }
    sql """revoke admin_PRIV on *.*.* from ${user}"""

    // ddl create,show,drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
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
        test {
            sql """SHOW SYNC JOB FROM `${dbName}`;"""
            exception "denied"
        }
    }
    sql """grant LOAD_PRIV on ${dbName}.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
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

        sql """SHOW SYNC JOB FROM `${dbName}`;"""
    }

    try_sql("""DROP REPOSITORY `${repositoryName}`;""")
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
