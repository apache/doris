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
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${name} case, because storage vault not enabled")
        return
    }

    def vaultName = "test_vault_privilege_with_role_vault";

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${vaultName}
        PROPERTIES (
            "type"="hdfs",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${vaultName}"
        );
    """

    def tableName = "test_vault_privilege_with_role_table"
    def userName = "test_vault_privilege_with_role_user"
    def userPassword = "Cloud12345"
    def roleName = "test_vault_privilege_with_role_role"
    def dbName = context.config.getDbNameByFile(context.file)

    sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""
    sql """DROP TABLE IF EXISTS ${dbName}.${tableName}_2"""

    sql """DROP USER IF EXISTS ${userName}"""
    sql """DROP ROLE IF EXISTS ${roleName}"""

    sql """CREATE ROLE ${roleName}"""
    sql """CREATE USER ${userName} identified by '${userPassword}' DEFAULT ROLE '${roleName}'"""
    sql """GRANT create_priv ON *.*.* TO '${userName}'; """

    connect(user = userName, password = userPassword, url = context.config.jdbcUrl) {
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
                        "storage_vault_name" = ${vaultName}
                        )
            """
        }, "denied")
    }

    sql """ GRANT usage_priv ON STORAGE VAULT '${vaultName}' TO ROLE '${roleName}';"""

    connect(user = userName, password = userPassword, url = context.config.jdbcUrl) {
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
                    C_CUSTKEY     INTEGER NOT NULL,
                    C_NAME        INTEGER NOT NULL
                    )
                    DUPLICATE KEY(C_CUSTKEY, C_NAME)
                    DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                    PROPERTIES (
                    "replication_num" = "1",
                    "storage_vault_name" = ${vaultName}
                    )
        """
    }

    sql """
        REVOKE usage_priv ON STORAGE VAULT '${vaultName}' FROM ROLE '${roleName}';
    """

    connect(user = userName, password = userPassword, url = context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS ${dbName}.${tableName}_2 (
                        C_CUSTKEY     INTEGER NOT NULL,
                        C_NAME        INTEGER NOT NULL
                        )
                        DUPLICATE KEY(C_CUSTKEY, C_NAME)
                        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_vault_name" = ${vaultName}
                        )
            """
        }, "denied")
    }

}