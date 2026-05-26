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

suite("test_drop_row_policy_by_roles") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "drop_row_policy_by_roles_tbl"
    def user1 = "drop_rp_role_user1"
    def user2 = "drop_rp_role_user2"
    def role1 = "drop_rp_role1"
    def role2 = "drop_rp_role2"
    def tokens = context.config.getDbNameByFile(context.file)
    def url = context.config.jdbcUrl

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `k` INT,
            `v` INT
        ) DUPLICATE KEY (`k`) DISTRIBUTED BY HASH (`k`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql "INSERT INTO ${tableName} VALUES (1,1), (2,2), (3,3)"

    sql "DROP USER IF EXISTS ${user1}"
    sql "DROP USER IF EXISTS ${user2}"
    sql "CREATE USER ${user1} IDENTIFIED BY '123abc!@#'"
    sql "CREATE USER ${user2} IDENTIFIED BY '123abc!@#'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ${user1}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ${user2}"

    sql "DROP ROLE IF EXISTS ${role1}"
    sql "DROP ROLE IF EXISTS ${role2}"
    sql "CREATE ROLE ${role1}"
    sql "CREATE ROLE ${role2}"
    sql "GRANT ${role1} TO ${user1}"
    sql "GRANT ${role2} TO ${user2}"

    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ROLE ${role1}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName} TO ROLE ${role2}"

    // clean up existing policies
    sql "DROP ROW POLICY IF EXISTS rp1 ON ${dbName}.${tableName} FOR ROLE ${role1}"
    sql "DROP ROW POLICY IF EXISTS rp2 ON ${dbName}.${tableName} FOR ROLE ${role1}"
    sql "DROP ROW POLICY IF EXISTS rp3 ON ${dbName}.${tableName} FOR ROLE ${role2}"
    sql "DROP ROW POLICY IF EXISTS rp4 ON ${dbName}.${tableName} FOR ROLE ${role2}"

    // create row policies for role1 and role2
    sql """
        CREATE ROW POLICY IF NOT EXISTS rp1 ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ROLE ${role1} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY IF NOT EXISTS rp2 ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ROLE ${role1} USING (v = 1)
    """
    sql """
        CREATE ROW POLICY IF NOT EXISTS rp3 ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ROLE ${role2} USING (k = 2)
    """
    sql """
        CREATE ROW POLICY IF NOT EXISTS rp4 ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ROLE ${role2} USING (v = 2)
    """

    sql 'sync'

    // verify policies exist
    def showResult1 = sql "SHOW ROW POLICY FOR ROLE ${role1}"
    assertEquals(2, showResult1.size())
    def showResult2 = sql "SHOW ROW POLICY FOR ROLE ${role2}"
    assertEquals(2, showResult2.size())

    // drop all row policies for role1 and role2 in one statement
    sql "DROP ROW POLICY FOR ROLE ${role1}, ${role2}"

    sql 'sync'

    // verify all policies are gone
    def showResultAfter1 = sql "SHOW ROW POLICY FOR ROLE ${role1}"
    assertEquals(0, showResultAfter1.size())
    def showResultAfter2 = sql "SHOW ROW POLICY FOR ROLE ${role2}"
    assertEquals(0, showResultAfter2.size())

    // test: drop with non-existent role should not throw error
    sql "DROP ROW POLICY FOR ROLE non_existent_role"

    // recreate policies for single role drop test
    sql """
        CREATE ROW POLICY IF NOT EXISTS rp5 ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ROLE ${role1} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY IF NOT EXISTS rp6 ON ${dbName}.${tableName}
        AS RESTRICTIVE TO ROLE ${role2} USING (k = 2)
    """

    sql 'sync'

    // drop only role1's policies
    sql "DROP ROW POLICY FOR ROLE ${role1}"

    sql 'sync'

    // verify role1's policies are gone but role2's remain
    def showResultRole1 = sql "SHOW ROW POLICY FOR ROLE ${role1}"
    assertEquals(0, showResultRole1.size())
    def showResultRole2 = sql "SHOW ROW POLICY FOR ROLE ${role2}"
    assertEquals(1, showResultRole2.size())

    // cleanup
    sql "DROP ROW POLICY FOR ROLE ${role2}"
    sql "DROP USER IF EXISTS ${user1}"
    sql "DROP USER IF EXISTS ${user2}"
    sql "DROP ROLE IF EXISTS ${role1}"
    sql "DROP ROLE IF EXISTS ${role2}"
}
