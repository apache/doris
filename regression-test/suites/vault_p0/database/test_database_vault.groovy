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

suite("test_database_vault", "nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("skip test_database_vault case because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip test_database_vault case")
        return
    }

    def db1 = "test_db_vault_db1"
    def db2 = "test_db_vault_db2"
    def vault1 = "test_db_vault_vault1"
    def vault2 = "test_db_vault_vault2"
    def vault3 = "test_db_vault_vault3"
    def table1 = "test_db_vault_table1"
    def table2 = "test_db_vault_table2"
    def table3 = "test_db_vault_table3"

    try {

        sql """
            UNSET DEFAULT STORAGE VAULT
        """

        sql """
            CREATE STORAGE VAULT IF NOT EXISTS ${vault1}
            PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "test_db_vault_vault1_prefix",
                "hadoop.username" = "${getHmsUser()}"
            );
        """

        def storageVaults = (sql " SHOW STORAGE VAULT; ").stream().map(row -> row[0]).collect(Collectors.toSet())
        assertTrue(storageVaults.contains(vault1))

        sql """
            CREATE STORAGE VAULT IF NOT EXISTS ${vault2}
            PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "test_db_vault_vault2_prefix",
                "hadoop.username" = "${getHmsUser()}"
            );
        """

        storageVaults = (sql " SHOW STORAGE VAULT; ").stream().map(row -> row[0]).collect(Collectors.toSet())
        assertTrue(storageVaults.contains(vault2))

        sql "DROP DATABASE IF EXISTS ${db1}"
        sql "DROP DATABASE IF EXISTS ${db2}"
        // Create database with vault, vault should exist
        expectExceptionLike({
            sql """
                CREATE DATABASE IF NOT EXISTS ${db1}
                PROPERTIES (
                    "storage_vault_name" = "non_exist_vault"
                )
            """
        }, "not exist")

        sql """
            CREATE DATABASE IF NOT EXISTS ${db1}
            PROPERTIES (
                "storage_vault_name" = "${vault2}"
            )
        """

        sql """
            CREATE DATABASE IF NOT EXISTS ${db2}
        """

        def createDbSql = (sql " SHOW CREATE DATABASE ${db1}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createDbSql.first().contains(vault2))

        // Alter database property with storage vault
        expectExceptionLike({
            sql """
                ALTER DATABASE ${db1} SET
                PROPERTIES (
                    "storage_vault_name" = "non_exist_vault"
                )
            """
        }, "not exist")

        sql """
            ALTER DATABASE ${db1} SET
            PROPERTIES (
                "storage_vault_name" = "${vault1}"
            )
        """

        createDbSql = (sql " SHOW CREATE DATABASE ${db1}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createDbSql.first().contains(vault1))

        sql """
            ALTER DATABASE ${db2} SET
            PROPERTIES (
                "storage_vault_name" = "${vault2}"
            )
        """

        createDbSql = (sql " SHOW CREATE DATABASE ${db2}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createDbSql.first().contains(vault2))

        // Create table with/without specifying vault
        sql """
            CREATE TABLE IF NOT EXISTS ${db1}.${table1} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "storage_vault_name" = ${vault2}
            )
        """
        def createTableSql = (sql " SHOW CREATE TABLE ${db1}.${table1}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createTableSql.first().contains(vault2))

        sql """
            CREATE TABLE IF NOT EXISTS ${db1}.${table2} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        """
        createTableSql = (sql " SHOW CREATE TABLE ${db1}.${table2}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createTableSql.first().contains(vault1))

        // Unset Database storage vault
        sql """
            ALTER DATABASE ${db2} SET
            PROPERTIES (
                "storage_vault_name" = ""
            )
        """
        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS ${db2}.${table1} (
                    C_CUSTKEY     INTEGER NOT NULL,
                    C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            """
        }, "No default storage vault")

        sql """
            CREATE TABLE IF NOT EXISTS ${db2}.${table2} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "storage_vault_name" = ${vault1}
            )
        """
        createTableSql = (sql " SHOW CREATE TABLE ${db2}.${table2}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createTableSql.first().contains(vault1))

        sql """
            ALTER DATABASE ${db2} SET
            PROPERTIES (
                "storage_vault_name" = "${vault2}"
            )
        """
        sql """
            CREATE TABLE IF NOT EXISTS ${db2}.${table1} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        """
        createTableSql = (sql " SHOW CREATE TABLE ${db2}.${table1}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createTableSql.first().contains(vault2))

        // Rename storage vault
        sql """
            ALTER STORAGE VAULT ${vault2}
            PROPERTIES (
                "type" = "hdfs",
                "VAULT_NAME" = "${vault3}"
            );
        """

        expectExceptionLike({
            sql """
                CREATE TABLE IF NOT EXISTS ${db2}.${table3} (
                    C_CUSTKEY     INTEGER NOT NULL,
                    C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            """
        }, "does not exist")

        sql """
            ALTER DATABASE ${db2} SET
            PROPERTIES (
                "storage_vault_name" = "${vault3}"
            );
        """

        sql """
            CREATE TABLE IF NOT EXISTS ${db2}.${table3} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        """

        createTableSql = (sql " SHOW CREATE TABLE ${db2}.${table3}; ").stream().map(row -> row[1]).collect(Collectors.toSet())
        assertTrue(createTableSql.first().contains(vault3))
    } finally {
        sql "DROP DATABASE IF EXISTS ${db1}"
        sql "DROP DATABASE IF EXISTS ${db2}"
    }

}
