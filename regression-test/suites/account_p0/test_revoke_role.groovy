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

suite("test_revoke_role", "account") {
    def role= 'revoke_test_role'
    def user = 'revoke_test_user'
    def dbName = 'revoke_test_db'
    def pwd = 'revoke_test_pwd'

    try_sql("DROP ROLE ${role}")
    try_sql("DROP USER ${user}")
    try_sql("DROP ROLE ${role}")
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE ${dbName}"""

    sql """CREATE ROLE ${role}"""
    sql """CREATE USER ${user} IDENTIFIED BY '${pwd}'"""
   
    sql """GRANT SELECT_PRIV ON ${dbName}.* TO ROLE '${role}'"""
    sql """GRANT '${role}' TO ${user}"""

    def result = sql """ SHOW GRANTS FOR ${user} """
    assertEquals(result.size(), 1)
    assertTrue(result[0][5].contains("internal.${dbName}: Select_priv"))

    sql """REVOKE '${role}' from ${user}"""
    result = sql """ SHOW GRANTS FOR ${user} """
    assertEquals(result.size(), 1)
    assertFalse(result[0][5].contains("internal.${dbName}: Select_priv"))

    sql """DROP USER ${user}"""
    sql """DROP ROLE ${role}"""
    sql """DROP DATABASE ${dbName}"""
}

