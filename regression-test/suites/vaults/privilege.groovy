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

suite("test_privilege_vault", "nonConcurrent") {
    if (!enableStoragevault()) {
        logger.info("skip test_privilege_vault case")
        return
    }

    def vault1 = "test_privilege_vault1"
    def table1 = "test_privilege_vault_t1"
    def table2 = "test_privilege_vault_t2"
    def table3 = "test_privilege_vault_t3"

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${vault1}
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${getHmsHdfsFs()}",
        "path_prefix" = "test_vault_privilege"
        );
    """

    def storageVaults = (sql " SHOW STORAGE VAULT; ").stream().map(row -> row[0]).collect(Collectors.toSet())
    assertTrue(storageVaults.contains(vault1))

    sql """
        SET ${vault1} AS DEFAULT STORAGE VAULT
    """
    sql """
        UNSET DEFAULT STORAGE VAULT
    """

    sql """
        DROP TABLE IF EXISTS ${table1};
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${table1} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = ${vault1}
                )
    """

    def user1 = "test_privilege_vault_user1"
    sql """drop user if exists ${user1}"""
    sql """create user ${user1} identified by 'Cloud12345'"""
    sql """
        GRANT create_priv ON *.*.* TO '${user1}';
    """

    def vault2 = "test_privilege_vault2"
    // Only users with admin role can create storage vault
    connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE STORAGE VAULT IF NOT EXISTS ${vault2}
                PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "test_vault_privilege"
                );
            """
        }, "denied")
    }

    // Only users with admin role can set/unset default storage vault
    connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                SET ${vault1} AS DEFAULT STORAGE VAULT
            """
        }, "denied")
    }
    connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                UNSET DEFAULT STORAGE VAULT
            """
        }, "denied")
    }

    def result = connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql " SHOW STORAGE VAULT; "
    }
    assertTrue(result.isEmpty())

    sql """
        DROP TABLE IF EXISTS ${table2};
    """
    connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS ${table2} (
                        C_CUSTKEY     INTEGER NOT NULL,
                        C_NAME        INTEGER NOT NULL
                        )
                        DUPLICATE KEY(C_CUSTKEY, C_NAME)
                        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_vault_name" = ${vault1}
                        )
            """
        }, "USAGE denied")
    }

    sql """
        GRANT usage_priv ON STORAGE VAULT '${vault1}' TO '${user1}';
    """

    result = connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql " SHOW STORAGE VAULT; "
    }
    storageVaults = result.stream().map(row -> row[0]).collect(Collectors.toSet())
    assertTrue(storageVaults.contains(vault1))

    connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
        sql """
            CREATE TABLE IF NOT EXISTS ${table2} (
                    C_CUSTKEY     INTEGER NOT NULL,
                    C_NAME        INTEGER NOT NULL
                    )
                    DUPLICATE KEY(C_CUSTKEY, C_NAME)
                    DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                    PROPERTIES (
                    "replication_num" = "1",
                    "storage_vault_name" = ${vault1}
                    )
        """
    }

    sql """
        REVOKE usage_priv ON STORAGE VAULT '${vault1}' FROM '${user1}';
    """

    result = connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql " SHOW STORAGE VAULT; "
    }
    assertTrue(result.isEmpty())

    sql """
        DROP TABLE IF EXISTS ${table3};
    """
    connect(user = user1, password = 'Cloud12345', url = context.config.jdbcUrl) {
        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS ${table3} (
                        C_CUSTKEY     INTEGER NOT NULL,
                        C_NAME        INTEGER NOT NULL
                        )
                        DUPLICATE KEY(C_CUSTKEY, C_NAME)
                        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_vault_name" = ${vault1}
                        )
            """
        }, "USAGE denied")
    }
}