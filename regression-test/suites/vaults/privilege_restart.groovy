
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

// This test suite is intent to test the granted privilege for specific user will
// not disappear
suite("test_privilege_vault_restart", "nonConcurrent") {
    if (!enableStoragevault()) {
        logger.info("skip test_privilege_vault_restart case")
        return
    }

    // user1 will be kept before and after running this test in order to check
    // the granted vault privilege is persisted well eventhough FE restarts many times
    def user1 = "test_privilege_vault_restart_user1"
    def passwd = "Cloud12345"

    def vault1 = "test_privilege_vault_restart_vault1"
    // this vaule is derived from current file location: regression-test/vaults
    def db = "regression_test_vaults"
    def table1 = "test_privilege_vault_restart_t1"
    def table2 = "test_privilege_vault_restart_t2"
    def hdfsLinkWeDontReallyCare = "127.0.0.1:10086" // a dummy link, it doesn't need to work

    //==========================================================================
    // prepare the basic vault and tables for further check
    //==========================================================================
    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${vault1}
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${hdfsLinkWeDontReallyCare}",
        "path_prefix" = "test_vault_privilege_restart"
        );
    """

    def storageVaults = (sql " SHOW STORAGE VAULT; ").stream().map(row -> row[0]).collect(Collectors.toSet())
    logger.info("all vaults: ${storageVaults}")
    org.junit.Assert.assertTrue("${vault1} is not present after creating, all vaults: ${storageVaults}", storageVaults.contains(vault1))

    def allTables = (sql " SHOW tables").stream().map(row -> row[0]).collect(Collectors.toSet())
    logger.info("all tables ${allTables}")

    // table1 is the sign to check if the user1 has been created and granted well
    def targetTableExist = allTables.contains(table1) 

    if (targetTableExist) { 
        // the grant procedure at least run once before, user1 has been granted vault1
        logger.info("${user1} has been granted with usage_priv to ${vault1} before")
    } else {
        logger.info("this is the frist run, or there was a crash during the very first run, ${user1} has not been granted with usage_priv to ${vault1} before")
        // create user and grant storage vault and create a table with that vault
        sql """drop user if exists ${user1}"""
        sql """create user ${user1} identified by '${passwd}'"""
        sql """
            GRANT usage_priv ON storage vault ${vault1} TO '${user1}';
        """
        sql """
            GRANT create_priv ON *.*.* TO '${user1}';
        """

        // ATTN: create table1, if successful, the sign has been set
        //       there wont be any execuse that user1 misses the privilege to vault1 from now on
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
    }

    //==========================================================================
    // check the prepared users and tables
    //==========================================================================
    def allUsers = (sql " SHOW all grants ").stream().map(row -> row[0]).collect(Collectors.toSet())
    logger.info("all users: ${allUsers}")
    def userPresent = !(allUsers.stream().filter(i -> i.contains(user1)).collect(Collectors.toSet()).isEmpty())
    org.junit.Assert.assertTrue("${user1} is not in the priv table ${allUsers}", userPresent)

    allTables = (sql " SHOW tables").stream().map(row -> row[0]).collect(Collectors.toSet())
    logger.info("all tables: ${allTables}")
    org.junit.Assert.assertTrue("${table1} is not present, all tables: ${allUsers}", allTables.contains(table1))

    // Test user privilege, the newly created user cannot create or set default vault
    // Only users with admin role can create storage vault
    connect(user = user1, password = passwd, url = context.config.jdbcUrl) {
        sql """use ${db}"""
        expectExceptionLike({
            sql """
                CREATE STORAGE VAULT IF NOT EXISTS ${vault1}
                PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS"="${hdfsLinkWeDontReallyCare}",
                "path_prefix" = "test_vault_privilege"
                );
            """
        }, "denied")
    }
    // Only users with admin role can set/unset default storage vault
    connect(user = user1, password = passwd, url = context.config.jdbcUrl) {
        sql """use ${db}"""
        expectExceptionLike({
            sql """
                SET ${vault1} AS DEFAULT STORAGE VAULT
            """
        }, "denied")
    }
    connect(user = user1, password = passwd, url = context.config.jdbcUrl) {
        sql """use ${db}"""
        expectExceptionLike({
            sql """
                UNSET DEFAULT STORAGE VAULT
            """
        }, "denied")
    }

    // user1 should see vault1
    def result = connect(user = user1, password = passwd, url = context.config.jdbcUrl) {
        sql """use ${db}"""
        sql " SHOW STORAGE VAULT; "
    }
    storageVaults = result.stream().map(row -> row[0]).collect(Collectors.toSet())
    org.junit.Assert.assertTrue("${user1} cannot see granted vault ${vault1} in result ${result}", storageVaults.contains(vault1))


    //==========================================================================
    // to test that user1 has the privilege of vault1 to create new tables
    // this is the main test for granted vault privilege after restarting FE
    //==========================================================================
    sql """
        DROP TABLE IF EXISTS ${table2} force;
    """
    connect(user = user1, password = passwd, url = context.config.jdbcUrl) {
        sql """use ${db}"""
        sql """
            CREATE TABLE ${table2} (
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

    result = connect(user = user1, password = passwd, url = context.config.jdbcUrl) {
        sql """use ${db}"""
        sql " SHOW create table ${table2}; "
    }
    logger.info("show create table ${table2}, result ${result}")
    org.junit.Assert.assertTrue("missing storage vault properties ${vault1} in table ${table2}", result.toString().contains(vault1))

}
