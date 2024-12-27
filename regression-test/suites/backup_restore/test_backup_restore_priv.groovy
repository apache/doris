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

import groovy.json.JsonSlurper

suite("test_backup_restore_priv", "backup_restore") {
    String suiteName = "test_backup_restore_priv"
    String repoName = "${suiteName}_repo"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String tableName1 = "${suiteName}_table_1"
    String tableName2 = "${suiteName}_table_2"
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
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName1}
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

    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName2}
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

    multi_sql """
        drop user if exists user1;
        drop user if exists user2;
        drop user if exists user3;

        drop role if exists role_select;
        drop role if exists role_load;
        drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};
        drop sql_block_rule if exists test_block_rule;
        drop catalog if exists mysql;
        drop workload group if exists wg1;
        drop workload group if exists wg2;
    """

    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    //String driver_url = "mysql-connector-j-8.0.31.jar"


    def commonAuth = { result, UserIdentity, Password, Roles, CatalogPrivs, DatabasePrivs, TablePrivs ->
        assertEquals(UserIdentity as String, result.UserIdentity[0] as String)
        assertEquals(Password as String, result.Password[0] as String)
        assertEquals(Roles as String, result.Roles[0] as String)
        assertEquals(CatalogPrivs as String, result.CatalogPrivs[0] as String)
        assertEquals(DatabasePrivs as String, result.DatabasePrivs[0] as String)
        assertEquals(TablePrivs as String, result.TablePrivs[0] as String)
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
        commonAuth result, "'user1'@'%'", "Yes", "role_select,role_load", "internal: Select_priv,Load_priv", "internal.information_schema: Select_priv; internal.mysql: Select_priv", null
        assertEquals("internal.${dbName}.${tableName2}: Select_priv[test]" as String, result.ColPrivs[0] as String)

        result = sql_return_maparray """show grants for user2"""
        log.info(result as String)
        commonAuth result, "'user2'@'%'", "Yes", "role_select", "internal: Select_priv","internal.information_schema: Select_priv; internal.mysql: Select_priv", null
        assertEquals("internal.${dbName}.${tableName2}: Select_priv[test]" as String, result.ColPrivs[0] as String)

        result = sql_return_maparray """show grants for user3"""
        log.info(result as String)
        commonAuth result, "'user3'@'%'", "Yes", "role_load", "internal: Select_priv,Load_priv,Create_priv","internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.${dbName}: Create_priv","internal.${dbName}.${tableName1}: Load_priv"
        assertEquals("internal.${dbName}.${tableName1}: Select_priv[id1]" as String, result.ColPrivs[0] as String)

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
                res = sql "select * from ${dbName}.${tableName}"
                assertEquals(res.size(), 1)
            } catch (Exception e) {
                log.info(e.getMessage())
            }
        }

        result = showSqlBlockRule.call("test_block_rule")
        log.info(result as String)
        assertEquals(result.Name, "test_block_rule")
    }

    def checkCatalogs = () -> {
        result = showCatalogs.call("mysql")
        log.info(result as String)
        assertEquals(result.CatalogName, "mysql")
        assertEquals(result.Type, "jdbc")
    }

    def checkWorkloadGroups = () -> {
        result = showWorkloadGroups.call("wg1")
        log.info(result as String)
        assertNotNull(result)
        result = showWorkloadGroups.call("wg2")
        log.info(result as String)
        assertNotNull(result)
    }

    multi_sql """
        CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;
        CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;
        CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;

        create role role_select;
        GRANT Select_priv ON *.* TO ROLE 'role_select';

        create role role_load;
        GRANT Load_priv ON *.* TO ROLE 'role_load';

        grant 'role_select', 'role_load' to 'user1'@'%';
        grant 'role_select' to 'user2'@'%';
        grant 'role_load' to 'user3'@'%';
        grant Create_priv on *.* to user3;
        GRANT Select_priv(id1) ON ${dbName}.${tableName1} TO user3;
        GRANT Select_priv(test) ON ${dbName}.${tableName2} TO role 'role_select';
        CREATE ROW POLICY test_row_policy_1 ON ${dbName}.${tableName} AS RESTRICTIVE TO user1 USING (id = 1);

        GRANT Select_priv, Load_priv ON *.* TO ROLE 'role_1';
        GRANT USAGE_PRIV ON WORKLOAD GROUP 'wg1' to role 'role_1';
        GRANT USAGE_PRIV ON RESOURCE * TO 'user2'@'%';
        GRANT USAGE_PRIV ON STAGE * TO 'user2'@'%';
        GRANT USAGE_PRIV ON STORAGE VAULT * TO 'user2'@'%';
        GRANT USAGE_PRIV ON COMPUTE GROUP * TO 'user3'@'%';

        grant node_priv on *.*.* to 'user2'@'%';
        grant select_priv on internal.*.* to 'user3'@'%';
        grant create_priv on internal.${dbName}.* to 'user3'@'%';
        grant load_priv on internal.${dbName}.${tableName1} to 'user3'@'%';


        CREATE SQL_BLOCK_RULE test_block_rule
            PROPERTIES(
            "sql"="select \\\\* from ${dbName}.${tableName}",
            "global"="true",
            "enable"="true"
            );

        CREATE CATALOG mysql PROPERTIES (
            "type"="jdbc",
            "user" = "${jdbcUser}",
            "password"="${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
            );
        create workload group wg1 properties('tag'='cn1', "memory_limit"="45%");
        create workload group wg2 properties ("max_concurrency"="5","max_queue_size" = "50");

        set property for user2 'default_workload_group' = 'wg2';
    """

    checkPrivileges()
    checkCatalogs()
    checkWorkloadGroups()
    
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

    def snapshotTime = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshotTime != null)

    //cleanup
    multi_sql """
        drop user if exists user1;
        drop user if exists user2;
        drop user if exists user3;

        drop role if exists role_select;
        drop role if exists role_load;
        drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};
        drop sql_block_rule if exists test_block_rule;
        drop catalog if exists mysql;
        drop workload group if exists wg1;
        drop workload group if exists wg2;
    """

    def result = sql_return_maparray "show snapshot on ${repoName} where Snapshot='${snapshotName}' and Timestamp='${snapshotTime}'"
    assertTrue(result.size() == 1)
    log.info(result as String)
    def details = result.Details[0]

    def jsonSlurper = new JsonSlurper()
    def data = jsonSlurper.parseText(details)
    def sqls = data.sqls

    log.info("=============================restore from sqls: ${sqls} ============================")

    sql "${sqls}"

    checkPrivileges()
    checkCatalogs()
    checkWorkloadGroups()

    //cleanup
    multi_sql """
        drop user if exists user1;
        drop user if exists user2;
        drop user if exists user3;

        drop role if exists role_select;
        drop role if exists role_load;
        drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};
        drop sql_block_rule if exists test_block_rule;
        drop catalog if exists mysql;
        drop workload group if exists wg1;
        drop workload group if exists wg2;
    """

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}