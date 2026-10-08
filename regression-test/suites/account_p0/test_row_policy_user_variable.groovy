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

// A row policy may compare a column against a user variable, so one policy serves every session and each
// session says which rows it is allowed to see: USING (region = @authorized_region). The predicate keeps the
// variable unbound until a query binds it to the session's value, and SHOW ROW POLICY used to fail on it
// with an unbound-expression error - for every policy it listed, not only this one.
suite("test_row_policy_user_variable") {
    def dbName = context.config.getDbNameByFile(context.file)
    def user = 'row_policy_user_var_user'
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    sql "DROP TABLE IF EXISTS row_policy_user_var_tbl"
    sql """
        CREATE TABLE row_policy_user_var_tbl (region varchar(8), v int)
        DISTRIBUTED BY HASH(region) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO row_policy_user_var_tbl VALUES ('cn', 1), ('us', 2), ('de', 3)"""

    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123abc!@#'"
    sql "GRANT SELECT_PRIV ON ${dbName}.row_policy_user_var_tbl TO ${user}"

    def cloudMode = isCloudMode()
    if (cloudMode) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${user}"""
    }

    sql "DROP ROW POLICY IF EXISTS p_user_var ON ${dbName}.row_policy_user_var_tbl FOR ${user}"
    sql """
        CREATE ROW POLICY p_user_var ON ${dbName}.row_policy_user_var_tbl
        AS RESTRICTIVE TO ${user} USING (region = @authorized_region)
    """

    // Both the listing that names the policy's grantee and the unfiltered one include this policy, and each
    // has to render it rather than fail.
    def shown = sql "SHOW ROW POLICY FOR ${user}"
    def predicate = shown.find { it[0] == 'p_user_var' }[6].toString()
    assertTrue(predicate.contains("@authorized_region"),
            "SHOW ROW POLICY does not render the user variable the policy compares against: ${predicate}")
    assertTrue(sql("SHOW ROW POLICY").any { it[0] == 'p_user_var' },
            "SHOW ROW POLICY without a filter does not list the policy")

    // The variable is bound per session, so the same policy admits different rows to different sessions.
    def cnRows = connect(user, '123abc!@#', url) {
        sql "SET @authorized_region = 'cn'"
        sql "SELECT region, v FROM row_policy_user_var_tbl ORDER BY region"
    }
    assertEquals([['cn', '1']], cnRows.collect { row -> [row[0].toString(), row[1].toString()] },
            "the session bound to cn did not read exactly the cn row")

    def usRows = connect(user, '123abc!@#', url) {
        sql "SET @authorized_region = 'us'"
        sql "SELECT region, v FROM row_policy_user_var_tbl ORDER BY region"
    }
    assertEquals([['us', '2']], usRows.collect { row -> [row[0].toString(), row[1].toString()] },
            "the session bound to us did not read exactly the us row")
}
