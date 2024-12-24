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

suite("test_select_view_auth","p0,auth") {
    String suiteName = "test_select_view_auth"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    String dbName = "${suiteName}_db"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String viewName = "${suiteName}_view"

    try_sql("drop user ${user}")
    try_sql """drop table if exists ${dbName}.${tableName1}"""
    try_sql """drop table if exists ${dbName}.${tableName2}"""
    try_sql """drop view if exists ${dbName}.${viewName}"""
    sql """drop database if exists ${dbName}"""

    sql """create user '${user}' IDENTIFIED by '${pwd}'"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
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

    sql """create view ${dbName}.${viewName} as select * from ${dbName}.${tableName1} union select * from ${dbName}.${tableName2};"""

    sql """grant select_priv on regression_test to ${user}"""

    // table column
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "select * from ${dbName}.${viewName}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    sql """grant select_priv on ${dbName}.${viewName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "select * from ${dbName}.${viewName}"
    }

    try_sql("drop user ${user}")
    try_sql """drop table if exists ${dbName}.${tableName1}"""
    try_sql """drop table if exists ${dbName}.${tableName2}"""
    try_sql """drop view if exists ${dbName}.${viewName}"""
    sql """drop database if exists ${dbName}"""
}
