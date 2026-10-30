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
    def pwd = '123abc!@#'
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    sql "DROP TABLE IF EXISTS row_policy_user_var_tbl"
    sql """
        CREATE TABLE row_policy_user_var_tbl (region varchar(8), v int)
        DISTRIBUTED BY HASH(region) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO row_policy_user_var_tbl VALUES ('cn', 1), ('us', 2), ('de', 3)"""

    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '${pwd}'"
    sql "GRANT SELECT_PRIV ON ${dbName}.row_policy_user_var_tbl TO ${user}"

    def cloudMode = isCloudMode()
    if (cloudMode) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${user}"""
    }

    sql "DROP ROW POLICY IF EXISTS p_user_var ON ${dbName}.row_policy_user_var_tbl FOR ${user}"
    sql "DROP ROW POLICY IF EXISTS p_quoted_var ON ${dbName}.row_policy_user_var_tbl FOR ${user}"
    sql "CREATE ROW POLICY p_user_var ON ${dbName}.row_policy_user_var_tbl " +
            "AS RESTRICTIVE TO ${user} USING (region = @authorized_region)"

    // Both the listing that names the policy's grantee and the unfiltered one include this policy, and each
    // has to render it rather than fail. The name is rendered backquoted, which is how any name reads back.
    order_qt_show_user_var "SHOW ROW POLICY FOR ${user}"
    assertTrue(sql("SHOW ROW POLICY").any { it[0] == 'p_user_var' },
            "SHOW ROW POLICY without a filter does not list the policy")

    // The variable is bound per session, so the same policy admits different rows to different sessions,
    // and a session that never set it reads nothing: an unset user variable is NULL.
    connectToDoris(user, pwd, url) {
        sql "SET @authorized_region = 'cn'"
        order_qt_cn_session "SELECT region, v FROM row_policy_user_var_tbl ORDER BY region"
    }
    connectToDoris(user, pwd, url) {
        sql "SET @authorized_region = 'us'"
        order_qt_us_session "SELECT region, v FROM row_policy_user_var_tbl ORDER BY region"
    }
    connectToDoris(user, pwd, url) {
        order_qt_unset_session "SELECT region, v FROM row_policy_user_var_tbl ORDER BY region"
    }

    // A quoted variable name is stored without its backticks, so SHOW has to put them back: rendered bare,
    // @authorized-region reads as a subtraction, not the variable the policy enforces. p_user_var is dropped
    // first: restrictive policies combine with AND, and this session never sets @authorized_region.
    sql "DROP ROW POLICY IF EXISTS p_user_var ON ${dbName}.row_policy_user_var_tbl FOR ${user}"
    sql "CREATE ROW POLICY p_quoted_var ON ${dbName}.row_policy_user_var_tbl " +
            "AS RESTRICTIVE TO ${user} USING (region = @`authorized-region`)"
    order_qt_show_quoted_var "SHOW ROW POLICY FOR ${user}"
    connectToDoris(user, pwd, url) {
        sql "SET @`authorized-region` = 'de'"
        order_qt_quoted_session "SELECT region, v FROM row_policy_user_var_tbl ORDER BY region"
    }
}
