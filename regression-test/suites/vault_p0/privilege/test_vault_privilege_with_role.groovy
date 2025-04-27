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

suite("test_vault_privilege_with_role", "nonConcurrent") {
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
    def roleName = "role_${randomStr}"
    def tableName = "tbl_${randomStr}"

    sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""
    sql """DROP USER IF EXISTS ${userName}"""
    sql """DROP ROLE IF EXISTS ${roleName}"""

    sql """CREATE ROLE ${roleName}"""
    sql """CREATE USER ${userName} identified by '${userPassword}' DEFAULT ROLE '${roleName}'"""
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
                CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
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

    sql """ GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName}' TO ROLE '${roleName}';"""

    connect(userName, userPassword, context.config.jdbcUrl) {
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
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
            insert into ${dbName}.${tableName} values(1, 1);
        """
        def result = sql """ select * from ${dbName}.${tableName}; """
        assertEquals(result.size(), 1)
    }

    sql """REVOKE usage_priv ON STORAGE VAULT '${hdfsVaultName}' FROM ROLE '${roleName}';"""
    connect(userName, userPassword, context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE ${dbName}.${tableName}_2 (
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

    def hdfsVaultName2 = "hdfs2_" + randomStr
    def userName2 = "user2_${randomStr}"
    def userPassword2 = "Cloud789654"
    def roleName2 = "role2_${randomStr}"
    def tableName2 = "tbl2_${randomStr}"

    sql """DROP TABLE IF EXISTS ${dbName}.${tableName2}"""
    sql """DROP USER IF EXISTS ${userName2}"""
    sql """DROP ROLE IF EXISTS ${roleName2}"""

    sql """CREATE ROLE ${roleName2}"""
    sql """CREATE USER ${userName2} identified by '${userPassword2}'"""
    sql """GRANT create_priv ON *.*.* TO '${userName2}'; """
    sql """GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName2}' TO '${userName2}';"""

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName2}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName2}",
            "hadoop.username" = "${getHmsUser()}"
        );
        """

    sql """ GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName2}' TO ROLE '${roleName2}';"""

    connect(userName2, userPassword2, context.config.jdbcUrl) {
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName2} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = ${hdfsVaultName2}
            )
        """
    }

    sql """ GRANT load_priv,select_priv ON  *.*.* TO '${userName2}';"""
    sql """ GRANT USAGE_PRIV ON COMPUTE GROUP '%' TO '${userName2}';"""
    connect(userName2, userPassword2, context.config.jdbcUrl) {
        sql """
            insert into ${dbName}.${tableName2} values(1, 1);
        """
        def result = sql """ select * from ${dbName}.${tableName2}; """
        assertEquals(result.size(), 1)
    }

    sql """REVOKE usage_priv ON STORAGE VAULT '${hdfsVaultName2}' FROM ROLE '${roleName2}';"""
    connect(userName2, userPassword2, context.config.jdbcUrl) {
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName2}_2 (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = ${hdfsVaultName2}
            )
        """
    }

    connect(userName2, userPassword2, context.config.jdbcUrl) {
        sql """
            insert into ${dbName}.${tableName2} values(2, 2);
        """
        def result = sql """ select * from ${dbName}.${tableName2}; """
        assertEquals(result.size(), 2)
    }
}