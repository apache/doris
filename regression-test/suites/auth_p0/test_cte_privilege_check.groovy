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

suite("test_cte_privilege_check","p0,auth") {
    String suiteName = "test_cte_privilege_check"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    String dbName = "${suiteName}_db"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String viewName = "${suiteName}_view"

    try_sql("drop user ${user}")
    try_sql """drop view if exists ${dbName}.${viewName}"""
    try_sql """drop table if exists ${dbName}.${tableName1}"""
    try_sql """drop table if exists ${dbName}.${tableName2}"""
    sql """drop database if exists ${dbName}"""

    sql """create user '${user}' IDENTIFIED by '${pwd}'"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql """create database ${dbName}"""
    sql("""use ${dbName}""")

    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName1}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName2}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """insert into ${dbName}.${tableName1} values (1, 'alice'), (2, 'bob')"""
    sql """insert into ${dbName}.${tableName2} values (3, 'charlie'), (4, 'dave')"""

    // Grant select only on tableName1, NOT on tableName2
    sql """grant select_priv on ${dbName}.${tableName1} to ${user}"""

    // Case 1: CTE references unauthorized table -> should fail
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql """
                with cte as (select * from ${dbName}.${tableName2})
                select * from cte
            """
            assertTrue(false, "Should have thrown privilege denied exception")
        } catch (Exception e) {
            log.info("Case 1 expected error: " + e.getMessage())
            assertTrue(e.getMessage().contains("denied"), "Expected denied but got: " + e.getMessage())
        }
    }

    // Case 2: CTE references authorized table -> should succeed
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """
            with cte as (select * from ${dbName}.${tableName1})
            select * from cte
        """
    }

    // Case 3: Direct query unauthorized table -> should fail (baseline)
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "select * from ${dbName}.${tableName2}"
            assertTrue(false, "Should have thrown privilege denied exception")
        } catch (Exception e) {
            log.info("Case 3 expected error: " + e.getMessage())
            assertTrue(e.getMessage().contains("denied"), "Expected denied but got: " + e.getMessage())
        }
    }

    // Case 4: CTE referenced twice (no inline) + unauthorized table -> should fail
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql """
                with cte as (select * from ${dbName}.${tableName2})
                select * from cte
                union all
                select * from cte
            """
            assertTrue(false, "Should have thrown privilege denied exception")
        } catch (Exception e) {
            log.info("Case 4 expected error: " + e.getMessage())
            assertTrue(e.getMessage().contains("denied"), "Expected denied but got: " + e.getMessage())
        }
    }

    // Case 5: CTE authorized + direct unauthorized table -> should fail
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql """
                with cte as (select * from ${dbName}.${tableName1})
                select * from cte
                union all
                select * from ${dbName}.${tableName2}
            """
            assertTrue(false, "Should have thrown privilege denied exception")
        } catch (Exception e) {
            log.info("Case 5 expected error: " + e.getMessage())
            assertTrue(e.getMessage().contains("denied"), "Expected denied but got: " + e.getMessage())
        }
    }

    // Case 6: View + UNION auth non-regression (PR #44621)
    sql """create view ${dbName}.${viewName} as select * from ${dbName}.${tableName1} union select * from ${dbName}.${tableName2};"""
    sql """grant select_priv on ${dbName}.${viewName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "select * from ${dbName}.${viewName}"
    }

    try_sql("drop user ${user}")
    try_sql """drop view if exists ${dbName}.${viewName}"""
    try_sql """drop table if exists ${dbName}.${tableName1}"""
    try_sql """drop table if exists ${dbName}.${tableName2}"""
    sql """drop database if exists ${dbName}"""
}
