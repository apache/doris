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

import java.util.stream.Collectors;

suite("test_vault_privilege_with_multi_roles", "nonConcurrent") {
    def suiteName = name;
    if (!isCloudMode()) {
        logger.info("skip ${suiteName} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${suiteName} case, because storage vault not enabled")
        return
    }

    def dbName = context.config.getDbNameByFile(context.file)
    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def hdfsVaultName = "hdfs_" + randomStr

    def userName = "user_${randomStr}"
    def userPassword = "Cloud12345"
    def roleName1 = "role1_${randomStr}"
    def roleName2 = "role2_${randomStr}"
    def tableName1 = "tbl1_${randomStr}"
    def tableName2 = "tbl2_${randomStr}"
    def tableName3 = "tbl3_${randomStr}"

    sql """DROP TABLE IF EXISTS ${dbName}.${tableName1}"""
    sql """DROP TABLE IF EXISTS ${dbName}.${tableName2}"""
    sql """DROP USER IF EXISTS ${userName}"""
    sql """DROP ROLE IF EXISTS ${roleName1}"""
    sql """DROP ROLE IF EXISTS ${roleName2}"""

    sql """CREATE ROLE ${roleName1}"""
    sql """CREATE ROLE ${roleName2}"""

    sql """CREATE USER ${userName} identified by '${userPassword}'"""
    sql """GRANT create_priv ON *.*.* TO '${userName}'; """

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
        """

    connect(userName, userPassword, context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS ${dbName}.${tableName1} (
                    C_CUSTKEY     INTEGER NOT NULL,
                    C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "storage_vault_name" = ${hdfsVaultName}
                )
            """
        }, "denied")
    }

    sql """ GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName}' TO ROLE '${roleName1}';"""
    sql """ GRANT '${roleName1}' TO '${userName}';"""

    connect(userName, userPassword, context.config.jdbcUrl) {
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName1} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = ${hdfsVaultName}
            )
        """
    }

    sql """ GRANT load_priv,select_priv ON  *.*.* TO '${userName}';"""
    sql """ GRANT USAGE_PRIV ON COMPUTE GROUP '%' TO '${userName}';"""
    connect(userName, userPassword, context.config.jdbcUrl) {
        sql """
            insert into ${dbName}.${tableName1} values(1, 1);
            select * from ${dbName}.${tableName1};
        """
    }

    sql """REVOKE usage_priv ON STORAGE VAULT '${hdfsVaultName}' FROM ROLE '${roleName1}';"""
    connect(userName, userPassword, context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE ${dbName}.${tableName2} (
                    C_CUSTKEY     INTEGER NOT NULL,
                    C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "storage_vault_name" = ${hdfsVaultName}
                )
            """
        }, "denied")
    }

    sql """ GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName}' TO ROLE '${roleName2}';"""
    sql """ GRANT '${roleName2}' TO '${userName}';"""

    connect(userName, userPassword, context.config.jdbcUrl) {
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName2} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = ${hdfsVaultName}
            )
        """
    }

    connect(userName, userPassword, context.config.jdbcUrl) {
        sql """
            insert into ${dbName}.${tableName1} values(1, 1);
            select * from ${dbName}.${tableName1};
        """
    }

    connect(userName, userPassword, context.config.jdbcUrl) {
        sql """
            insert into ${dbName}.${tableName2} values(1, 1);
            select * from ${dbName}.${tableName2};
        """
    }

    sql """REVOKE usage_priv ON STORAGE VAULT '${hdfsVaultName}' FROM ROLE '${roleName2}';"""

    connect(userName, userPassword, context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE ${dbName}.${tableName3} (
                    C_CUSTKEY     INTEGER NOT NULL,
                    C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "storage_vault_name" = ${hdfsVaultName}
                )
            """
        }, "denied")
    }
}