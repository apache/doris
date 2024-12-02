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

suite("test_backup_restore_priv", "backup_restore") {
    String suiteName = "test_backup_restore_priv"
    String repoName = "${suiteName}_repo"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1"
        )
        """
    def insert_num = 5
    for (int i = 0; i < insert_num; ++i) {
        sql """
               INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
            """
    }

    res = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(res.size(), insert_num)

    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"


    sql "CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
    sql "CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
    sql "CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

    sql "create role role_select;"
    sql "GRANT Select_priv ON *.* TO ROLE 'role_select';"

    sql "create role role_load;"
    sql "GRANT Load_priv ON *.* TO ROLE 'role_load';"

    sql "grant 'role_select', 'role_load' to 'user1'@'%';"
    sql "grant 'role_select' to 'user2'@'%';"
    sql "grant 'role_load' to 'user3'@'%';"

    sql "CREATE ROW POLICY test_row_policy_1 ON ${dbName}.${tableName} AS RESTRICTIVE TO user1 USING (id = 1);"

    sql """
        CREATE SQL_BLOCK_RULE test_block_rule
        PROPERTIES(
        "sql"="select \\* from order_analysis",
        "global"="false",
        "enable"="true"
        );
     """

    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    //String driver_url = "mysql-connector-j-8.0.31.jar"

    sql """
        CREATE CATALOG mysql PROPERTIES (
        "type"="jdbc",
        "user" = "${jdbcUser}",
        "password"="${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        );
    """

    sql """ create workload group wg1 properties('tag'='cn1', "memory_limit"="45%"); """
    sql """ create workload group wg2 properties ("max_concurrency"="5","max_queue_size" = "50"); """

    def commonAuth = { result, UserIdentity, Password, Roles, CatalogPrivs ->
        assertEquals(UserIdentity as String, result.UserIdentity[0] as String)
        assertEquals(Password as String, result.Password[0] as String)
        assertEquals(Roles as String, result.Roles[0] as String)
        assertEquals(CatalogPrivs as String, result.CatalogPrivs[0] as String)
    }

    def showRoles = { name ->
        def ret = sql_return_maparray """show roles"""
        ret.find {
            def matcher = it.Name =~ /.*${name}$/
            matcher.matches()
        }
    }

    def showRowPolicy = { name ->
        def ret = sql_return_maparray """show row policy"""
        ret.find {
            def matcher = it.PolicyName =~ /.*${name}$/
            matcher.matches()
        }
    }

    def showSqlBlockRule = { name ->
        def ret = sql_return_maparray """show SQL_BLOCK_RULE"""
        ret.find {
            def matcher = it.Name =~ /.*${name}$/
            matcher.matches()
        }
    }

    def showCatalogs = { name ->
        def ret = sql_return_maparray """show catalogs"""
        ret.find {
            def matcher = it.CatalogName =~ /.*${name}$/
            matcher.matches()
        }
    }

    def showWorkloadGroups = { name ->
        def ret = sql_return_maparray """show workload groups"""
        ret.find {
            def matcher = it.Name =~ /.*${name}$/
            matcher.matches()
        }
    }

    def checkPrivileges = () -> {
        def result = sql_return_maparray """show grants for user1"""
        log.info(result as String)
        commonAuth result, "'user1'@'%'", "Yes", "role_select,role_load", "internal: Select_priv,Load_priv"

        result = sql_return_maparray """show grants for user2"""
        log.info(result as String)
        commonAuth result, "'user2'@'%'", "Yes", "role_select", "internal: Select_priv"

        result = sql_return_maparray """show grants for user3"""
        log.info(result as String)
        commonAuth result, "'user3'@'%'", "Yes", "role_load", "internal: Load_priv"

        result = showRoles.call("role_select")
        log.info(result as String)
        //assertNull(result.CloudClusterPrivs)
        assertEquals(result.Users, "'user1'@'%', 'user2'@'%'")
        assertEquals(result.CatalogPrivs, "internal.*.*: Select_priv")
    
        result = showRoles.call("role_load")
        log.info(result as String)
        assertEquals(result.Users, "'user1'@'%', 'user3'@'%'")
        assertEquals(result.CatalogPrivs, "internal.*.*: Load_priv")

        result = showRowPolicy.call("test_row_policy_1")
        log.info(result as String)
        assertNotNull(result)


        // check row policy valid
        connect(user="user1", password="12345", url=url) {
            try {
                res = sql "SELECT * FROM ${dbName}.${tableName}"
                assertEquals(res.size(), 1)
            } catch (Exception e) {
                log.info(e.getMessage())
            }
        }

        result = showSqlBlockRule.call("test_block_rule")
        log.info(result as String)
        assertEquals(result.Name, "test_block_rule")
    }

    def checkNonPrivileges = () -> {
        //except 'password_policy.password_creation_time'
        result = sql_return_maparray "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
        assertEquals(result.size(), 0)

        result = showRoles.call("role_select")
        log.info(result as String)
        assertNull(result)

        result = showRoles.call("role_load")
        log.info(result as String)
        assertNull(result)

        result = showRowPolicy.call("test_row_policy_1")
        log.info(result as String)
        assertNull(result)

        result = showSqlBlockRule.call("test_block_rule")
        log.info(result as String)
        assertNull(result)
    }



    def checkCatalogs = () -> {
        result = showCatalogs.call("mysql")
        log.info(result as String)
        assertEquals(result.CatalogName, "mysql")
        assertEquals(result.Type, "jdbc")
    }

    def checkSqlBlockRules = () -> {
        result = showSqlBlockRule.call("test_block_rule")
        log.info(result as String)
        assertEquals(result.Name, "test_block_rule")
    }

    def checkWorkloadGroups = () -> {
        result = showWorkloadGroups.call("wg1")
        log.info(result as String)
        assertNotNull(result)
        result = showWorkloadGroups.call("wg2")
        log.info(result as String)
        assertNotNull(result)
    }

    def checkNonCatalogs = () -> {
        result = showCatalogs.call("mysql")
        log.info(result as String)
        assertNull(result)
    }

    def checkNonWorkloadGroups = () -> {
        result = showWorkloadGroups.call("wg1")
        log.info(result as String)
        assertNull(result)
        result = showWorkloadGroups.call("wg2")
        log.info(result as String)
        assertNull(result)
    }

    checkPrivileges();
    qt_order_before_restore "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
    checkCatalogs();
    checkWorkloadGroups();


    sql """
        BACKUP GLOBAL SNAPSHOT ${snapshotName}
        TO `${repoName}`
        PROPERTIES (
            "backup_privilege"="true",
            "backup_catalog"="true",
            "backup_workload_group"="true"
        )
    """

    syncer.waitSnapshotFinish("__internal_schema")

    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    checkNonPrivileges();
    checkNonCatalogs();
    checkNonWorkloadGroups();

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    logger.info(""" ======================================  1 "reserve_privilege"="true", "reserve_catalog"="true","reserve_workload_group"="true" ==================================== """)

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_privilege"="true",
            "reserve_catalog"="true",
            "reserve_workload_group"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    checkPrivileges();
    qt_order_after_restore1 "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
    checkCatalogs();
    checkWorkloadGroups();

    logger.info(" ====================================== 2 without reserve ==================================== ")
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    checkPrivileges();
    qt_order_after_restore2 "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
    checkCatalogs();
    checkWorkloadGroups();


    logger.info(""" ======================================  3 "reserve_privilege"="true" ==================================== """)
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_privilege"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    checkPrivileges();
    qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
    checkNonCatalogs();
    checkNonWorkloadGroups();


    logger.info(""" ======================================  4 "reserve_catalog"="true" ==================================== """)
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_catalog"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    checkNonPrivileges();
    qt_order_after_restore4 "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
    checkCatalogs();
    checkNonWorkloadGroups();


    logger.info(""" ======================================  5 "reserve_workload_group"="true" ==================================== """)
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_workload_group"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    checkNonPrivileges();
    qt_order_after_restore5 "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
    checkNonCatalogs();
    checkWorkloadGroups();


    logger.info(""" ======================================  6 "reserve_privilege"="true","reserve_workload_group"="true" ==================================== """)

    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_privilege"="true",
            "reserve_workload_group"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    checkPrivileges();
    qt_order_after_restore6 "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"
    checkNonCatalogs();
    checkWorkloadGroups();

    logger.info(""" ======================================  7 restore fail check cancel ==================================== """)

    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql "create role role_select;"
    sql "GRANT Select_priv, Load_priv ON *.* TO ROLE 'role_select';"
    sql "create user user1;"
    sql "grant select_priv on *.* to user1;"
    sql """ create workload group wg2 properties ("max_concurrency"="5","max_queue_size" = "50"); """


    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_privilege"="true",
            "reserve_workload_group"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    // restore failed
    records = sql_return_maparray "SHOW global restore"
    row = records[records.size() - 1]
    assertTrue(row.Status.contains("workload group wg2 already exist"))

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }

    qt_order_after_restore7 "select * except(`password_policy.password_creation_time`) from mysql.user where User in ('user1', 'user2', 'user3') order by host, user;"

    result = sql_return_maparray """show grants for user1"""
    log.info(result as String)
    commonAuth result, "'user1'@'%'", "No", "", "internal: Select_priv"

    result = showRoles.call("role_select")
    log.info(result as String)
    assertEquals(result.Users, "")
    assertEquals(result.CatalogPrivs, "internal.*.*: Select_priv,Load_priv")


    //cleanup
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop sql_block_rule if exists test_block_rule;"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}