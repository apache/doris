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
    def result1 = connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "show databases like '${dbName}'"
    }
    assertEquals(result1.size(), 1)

    sql """REVOKE SELECT_PRIV ON ${dbName} FROM ROLE '${role}'"""
    def result2 = connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "show databases like '${dbName}'"
    }
    assertEquals(result2.size(), 0)

    sql """DROP USER ${user}"""
    sql """DROP ROLE ${role}"""
    sql """DROP DATABASE ${dbName}"""

    // test comment
    // create role with comment
    sql """CREATE ROLE ${role} comment 'account_p0_account_role_test_comment_create'"""
    def roles_create = sql """show roles"""
    logger.info("roles_create: " + roles_create.toString())
    assertTrue(roles_create.toString().contains("account_p0_account_role_test_comment_create"))
    // alter role with comment
    sql """ALTER ROLE ${role} comment 'account_p0_account_role_test_comment_alter'"""
    def roles_alter = sql """show roles"""
    logger.info("roles_alter: " + roles_alter.toString())
    assertTrue(roles_alter.toString().contains("account_p0_account_role_test_comment_alter"))
    // drop role
    sql """DROP ROLE ${role}"""
    def roles_drop = sql """show roles"""
    logger.info("roles_drop: " + roles_drop.toString())
    assertFalse(roles_drop.toString().contains("account_p0_account_role_test_comment_alter"))
}

