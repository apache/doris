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

suite("test_auth_show", "account") {

     def create_table = { tableName ->
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    `key` INT,
                    value INT
                ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
                PROPERTIES ('replication_num' = '1')
            """
        }

    def user = 'acount_auth_show_user'
    def pwd = 'C123_567p'
    def dbName = 'account_auth_show_db'
    def tableName = 'account_auth_show_table'

    try_sql("DROP USER ${user}")
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE ${dbName}"""
    sql """USE ${dbName}"""
    create_table.call(tableName);
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    // With select priv for table, should be able to see db
    sql """GRANT SELECT_PRIV ON ${dbName}.${tableName} TO ${user}"""
    def result1 = connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "show databases like '${dbName}'"
    }
    assertEquals(result1.size(), 1)
    sql """REVOKE SELECT_PRIV ON ${dbName}.${tableName} FROM ${user}"""

    // With show_view priv for table, should be able to see db
    sql """GRANT SHOW_VIEW_PRIV ON ${dbName}.${tableName} TO ${user}"""
    def result2 = connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "show databases like '${dbName}'"
    }
    assertEquals(result2.size(), 1)
    sql """REVOKE SHOW_VIEW_PRIV ON ${dbName}.${tableName} FROM ${user}"""
}

