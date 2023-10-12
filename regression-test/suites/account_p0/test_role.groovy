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

suite("test_role", "account") {
    def role= 'account_role_test'
    def user = 'acount_role_user_test'
    def dbName = 'account_role_test_db'
    def pwd = 'C123_567p'

    try_sql("DROP ROLE ${role}")
    try_sql("DROP USER ${user}")
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE ${dbName}"""

    sql """CREATE ROLE ${role}"""
    sql """GRANT SELECT_PRIV ON ${context.config.defaultDb} TO ROLE '${role}'"""
    sql """GRANT SELECT_PRIV ON ${dbName} TO ROLE '${role}'"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}' DEFAULT ROLE '${role}'"""
    def result1 = connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "show databases like '${dbName}'"
    }
    assertEquals(result1.size(), 1)

    sql """REVOKE SELECT_PRIV ON ${dbName} FROM ROLE '${role}'"""
    def result2 = connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "show databases like '${dbName}'"
    }
    assertEquals(result2.size(), 0)

    sql """DROP USER ${user}"""
    sql """DROP ROLE ${role}"""
    sql """DROP DATABASE ${dbName}"""

    def normal_user = 'normal_user'
    sql """DROP USER ${normal_user}"""
    sql """CREATE USER ${normal_user}"""
    def result3 = connect(user=normal_user, password="", url=context.config.jdbcUrl) {
        sql "select * from numbers(\"numbers\"=\"1\")"
    }
    assertEquals(result3, "0")

    def result4 = connect(user=normal_user, password="", url=context.config.jdbcUrl) {
        sql "select /*+ SET_VAR(enable_nereids_planner=false) */ * from numbers(\"numbers\"=\"1\")"
    }
    assertEquals(result4, "0")
    sql """DROP USER ${normal_user}"""
}

