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

suite("test_show_charset_command", "query,charset") {
    checkNereidsExecute("SHOW CHARSET")
    order_qt_cmd "SHOW CHARSET"
    order_qt_character_set "SHOW CHARACTER SET"

    order_qt_like_exact "SHOW CHARACTER SET LIKE 'utf8mb4'"
    order_qt_like_percent "SHOW CHARSET LIKE 'utf8%'"
    order_qt_like_underscore "SHOW CHARSET LIKE 'utf8mb_'"
    order_qt_like_case "SHOW CHARSET LIKE 'UTF8%'"
    order_qt_like_no_match "SHOW CHARACTER SET LIKE 'latin%'"
    order_qt_like_empty "SHOW CHARSET LIKE ''"

    order_qt_where_charset "SHOW CHARACTER SET WHERE charset = 'utf8mb4'"
    order_qt_where_no_match "SHOW CHARSET WHERE Charset = 'latin1'"
    order_qt_where_like "SHOW CHARSET WHERE Charset LIKE 'utf8%'"
    order_qt_where_not_like "SHOW CHARSET WHERE Charset NOT LIKE 'utf8%'"
    order_qt_where_and "SHOW CHARSET WHERE CHARSET = 'utf8mb4' AND Maxlen >= 4"
    order_qt_where_or "SHOW CHARSET WHERE Charset = 'latin1' OR Description = 'UTF-8 Unicode'"
    order_qt_where_collation "SHOW CHARSET WHERE `Default collation` = 'utf8mb4_0900_bin'"
    order_qt_where_numeric "SHOW CHARSET WHERE Maxlen > 4"
    order_qt_where_expression "SHOW CHARSET WHERE upper(Charset) = 'UTF8MB4' AND Maxlen + 1 = 5"
    order_qt_where_true "SHOW CHARSET WHERE TRUE"
    order_qt_where_false "SHOW CHARSET WHERE FALSE"
    order_qt_where_null "SHOW CHARSET WHERE NULL"
    order_qt_where_equals_null "SHOW CHARSET WHERE Charset = NULL"
    order_qt_where_not_null "SHOW CHARSET WHERE Charset IS NOT NULL"

    sql "SET sql_mode = ''"
    order_qt_like_backspace "SHOW CHARSET LIKE 'utf8m\\b4'"
    order_qt_like_backslash "SHOW CHARSET LIKE 'utf8m\\\\b4'"
    sql "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"
    order_qt_like_no_backslash_escapes "SHOW CHARSET LIKE 'utf8m\\\\b4'"
    sql "SET sql_mode = DEFAULT"

    sql "SET enable_sql_cache = true"
    order_qt_where_before_collation_change "SHOW CHARSET WHERE TRUE"
    sql "SET collation_connection = 'utf8mb4_general_ci'"
    order_qt_where_after_collation_change "SHOW CHARSET WHERE TRUE"
    sql "SET collation_connection = DEFAULT"
    sql "SET enable_sql_cache = DEFAULT"

    sql "SET sql_select_limit = 0"
    order_qt_where_with_select_limit "SHOW CHARSET WHERE @@sql_select_limit = 0"
    order_qt_select_limit_still_applies "SELECT 1"
    sql "SET sql_select_limit = DEFAULT"

    order_qt_where_database "SHOW CHARSET WHERE DATABASE() = '${context.dbName}' AND CURRENT_CATALOG() = 'internal'"

    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName, false) +
            "&emulateUnsupportedPstmts=false"
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        context.getConnection().prepareStatement("SHOW CHARSET WHERE Maxlen = ?").withCloseable { stmt ->
            assertTrue(stmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement)
            stmt.setInt(1, 4)
            qe_prepared_match stmt
            stmt.setInt(1, 5)
            qe_prepared_no_match stmt
        }
    }

    test {
        sql "SHOW CHARSET WHERE unknown_column = 'utf8mb4'"
        exception "unknown_column"
    }
}
