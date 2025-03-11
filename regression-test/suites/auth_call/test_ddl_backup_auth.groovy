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

suite("test_ddl_backup_auth","p0,auth_call") {
    UUID uuid = UUID.randomUUID()
    String randomValue = uuid.toString()
    int hashCode = randomValue.hashCode()
    hashCode = hashCode > 0 ? hashCode : hashCode * (-1)

    String user = 'test_ddl_backup_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_backup_auth_db'
    String tableName = 'test_ddl_backup_auth_tb'
    String repositoryName = 'test_ddl_backup_auth_rps'
    String backupLabelName = 'test_ddl_backup_auth_backup_label' + hashCode.toString()

    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")

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
                "s3.secret_key" = "${sk}"
            )"""

    // ddl create,show,drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """BACKUP SNAPSHOT ${dbName}.${backupLabelName}
                    TO ${repositoryName}
                    ON (${tableName})
                    PROPERTIES ("type" = "full");"""
            exception "denied"
        }
        test {
            sql """CANCEL BACKUP FROM ${dbName};"""
            exception "denied"
        }

        test {
            sql """SHOW BACKUP FROM ${dbName};"""
            exception "denied"
        }

        test {
            sql """SHOW SNAPSHOT ON ${repositoryName};"""
            exception "denied"
        }
    }
    sql """grant LOAD_PRIV on ${dbName}.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """BACKUP SNAPSHOT ${dbName}.${backupLabelName}
                TO ${repositoryName}
                ON (${tableName})
                PROPERTIES ("type" = "full");"""
        def res = sql """SHOW BACKUP FROM ${dbName};"""
        logger.info("res: " + res)
        assertTrue(res.size() == 1)

        sql """CANCEL BACKUP FROM ${dbName};"""
        res = sql """SHOW BACKUP FROM ${dbName};"""
        logger.info("res: " + res)
        assertTrue(res[0][3] == "CANCELLED")

        test {
            sql """SHOW SNAPSHOT ON ${repositoryName};"""
            exception "denied"
        }
    }

    try_sql("""DROP REPOSITORY `${repositoryName}`;""")
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
