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

suite("test_vault_privilege_with_user", "nonConcurrent") {
    def suiteName = name;
    if (!isCloudMode()) {
        logger.info("skip ${suiteName} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${suiteName} case, because storage vault not enabled")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def hdfsVaultName = "hdfs_" + randomStr
    def hdfsUser = "user_" + randomStr
    def hdfsUser2 = "user2_" + randomStr

    sql """DROP USER IF EXISTS ${hdfsUser}"""
    sql """DROP USER IF EXISTS ${hdfsUser2}"""

    sql """create user ${hdfsUser} identified by 'Cloud12345'"""
    sql """create user ${hdfsUser2} identified by 'Cloud12345'"""
    sql """ GRANT create_priv ON *.*.* TO '${hdfsUser}'; """
    sql """ GRANT create_priv ON *.*.* TO '${hdfsUser2}'; """


    // Only users with admin role can create storage vault
    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE STORAGE VAULT ${hdfsVaultName}
                PROPERTIES (
                    "type"="hdfs",
                    "fs.defaultFS"="${getHmsHdfsFs()}",
                    "path_prefix" = "${hdfsVaultName}",
                    "hadoop.username" = "${getHmsUser()}"
                );
            """
        }, "denied")
    }

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
        """

    // Only users with admin role can set/unset default storage vault
    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                SET ${hdfsVaultName} AS DEFAULT STORAGE VAULT
            """
        }, "denied")
    }

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                UNSET DEFAULT STORAGE VAULT;
            """
        }, "denied")
    }

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                SET not_exist_vault AS DEFAULT STORAGE VAULT
            """
        }, "denied")
    }

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                UNSET DEFAULT STORAGE VAULT
            """
        }, "denied")
    }

    def result = connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
            sql " SHOW STORAGE VAULT; "
    }
    assertTrue(result.isEmpty())

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS tbl_${hdfsVaultName} (
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
        }, "USAGE denied")
    }

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS tbl_${hdfsVaultName} (
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
        }, "USAGE denied")
    }

    sql """
        GRANT usage_priv ON STORAGE VAULT 'not_exist_vault' TO '${hdfsUser}';
    """

    sql """
        GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName}' TO 'not_exit_user';
    """

    sql """
        GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName}' TO '${hdfsUser}';
        """

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        sql """
            CREATE TABLE tbl_${hdfsVaultName} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${hdfsVaultName}
            );
        """
    }

    result = connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
            sql " SHOW STORAGE VAULT; "
    }

    def storageVaults = result.stream().map(row -> row[0]).collect(Collectors.toSet())
    assertTrue(storageVaults.contains(hdfsVaultName))

    sql """ GRANT load_priv,select_priv ON  *.*.* TO '${hdfsUser}';"""
    sql """ GRANT USAGE_PRIV ON COMPUTE GROUP '%' TO '${hdfsUser}';"""

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        sql """
            insert into tbl_${hdfsVaultName} values(1, 1);
            select * from tbl_${hdfsVaultName};
        """
    }

    // Grant `usage_prive` to hdfsUser2
    sql """
        GRANT usage_priv ON STORAGE VAULT '${hdfsVaultName}' TO '${hdfsUser2}';
        """

    connect(hdfsUser2, 'Cloud12345', context.config.jdbcUrl) {
        sql """
            CREATE TABLE tbl2_${hdfsVaultName} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${hdfsVaultName}
            );
        """
    }

    result = connect(hdfsUser2, 'Cloud12345', context.config.jdbcUrl) {
            sql " SHOW STORAGE VAULT; "
    }

    storageVaults = result.stream().map(row -> row[0]).collect(Collectors.toSet())
    assertTrue(storageVaults.contains(hdfsVaultName))

    sql """
        GRANT load_priv,select_priv ON  *.*.* TO '${hdfsUser2}';
        """
    sql """
        GRANT USAGE_PRIV ON COMPUTE GROUP '%' TO '${hdfsUser2}';
        """

    connect(hdfsUser2, 'Cloud12345', context.config.jdbcUrl) {
        sql """
            insert into tbl2_${hdfsVaultName} values(1, 1);
            select * from tbl2_${hdfsVaultName};
        """
    }

    // hdfsUser1 still has privlege
    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        sql """
            insert into tbl_${hdfsVaultName} values(1, 1);
            select * from tbl_${hdfsVaultName};
        """
    }

    sql """
        REVOKE usage_priv ON STORAGE VAULT '${hdfsVaultName}' FROM '${hdfsUser}';
        """

    result = connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
            sql " SHOW STORAGE VAULT; "
    }
    assertTrue(result.isEmpty())

    connect(hdfsUser, 'Cloud12345', context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE tbl3_${hdfsVaultName} (
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
        }, "USAGE denied")
    }
}