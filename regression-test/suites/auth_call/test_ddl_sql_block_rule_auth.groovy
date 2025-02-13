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

import org.junit.Assert;

suite("test_ddl_sql_block_rule_auth","p0,auth_call") {
    String user = 'test_ddl_sbr_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_sbr_auth_db'
    String tableName = 'test_ddl_sbr_auth_tb'
    String sqlBlockRuleName = 'test_ddl_sbr_auth_sbr'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql("""DROP SQL_BLOCK_RULE ${sqlBlockRuleName};""")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""
    sql """create table ${dbName}.${tableName} (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""

    // ddl create,show,drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE SQL_BLOCK_RULE ${sqlBlockRuleName} 
                    PROPERTIES(
                      "sql"="select \\* from ${dbName}.${tableName}",
                      "partition_num" = "30",
                      "global"="false",
                      "enable"="true"
                    );"""
            exception "denied"
        }
        test {
            sql """ALTER SQL_BLOCK_RULE ${sqlBlockRuleName} PROPERTIES("partition_num" = "10")"""
            exception "denied"
        }

        test {
            sql """SHOW SQL_BLOCK_RULE FOR ${sqlBlockRuleName};"""
            exception "denied"
        }

        test {
            sql """DROP SQL_BLOCK_RULE ${sqlBlockRuleName};"""
            exception "denied"
        }
    }
    sql """grant admin_priv on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE SQL_BLOCK_RULE ${sqlBlockRuleName} 
                    PROPERTIES(
                      "sql"="select \\\\* from ${dbName}\\\\.${tableName}",
                      "global"="false",
                      "enable"="true"
                    );"""
        def res = sql """SHOW SQL_BLOCK_RULE FOR ${sqlBlockRuleName};"""
        assertTrue(res.size() > 0)
        sql """SET PROPERTY FOR '${user}' 'sql_block_rules' = '${sqlBlockRuleName}';"""
        test {
            sql """select * from ${dbName}.${tableName}"""
            exception "sql block rule"
        }
        sql """ALTER SQL_BLOCK_RULE ${sqlBlockRuleName} PROPERTIES("enable"="true")"""
        sql """DROP SQL_BLOCK_RULE ${sqlBlockRuleName};"""
        res = sql """SHOW SQL_BLOCK_RULE FOR ${sqlBlockRuleName};"""
        assertTrue(res.size() == 0)
        sql """select * from ${dbName}.${tableName}"""
    }

    try_sql("""DROP SQL_BLOCK_RULE ${sqlBlockRuleName};""")
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
