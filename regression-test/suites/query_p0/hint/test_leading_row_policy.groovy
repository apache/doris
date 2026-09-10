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

suite("test_leading_row_policy") {
    String dbName = context.config.getDbNameByFile(context.file)
    String user = "leading_row_policy_user"
    String pwd = 'C123_567p'
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    sql "DROP ROW POLICY IF EXISTS leading_row_policy ON ${dbName}.leading_row_policy_t1 FOR ${user}"
    sql "DROP ROW POLICY IF EXISTS leading_row_policy_agg ON ${dbName}.leading_row_policy_agg FOR ${user}"
    sql "DROP TABLE IF EXISTS leading_row_policy_t1"
    sql "DROP TABLE IF EXISTS leading_row_policy_t2"
    sql "DROP TABLE IF EXISTS leading_row_policy_agg"
    sql """
        CREATE TABLE leading_row_policy_t1 (
            `k` INT,
            `v` INT
        ) DUPLICATE KEY (`k`) DISTRIBUTED BY HASH (`k`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        CREATE TABLE leading_row_policy_t2 (
            `k` INT,
            `v` INT
        ) DUPLICATE KEY (`k`) DISTRIBUTED BY HASH (`k`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    // the aggregate table with random distribution needs an aggregation above the scan to merge the rows
    sql """
        CREATE TABLE leading_row_policy_agg (
            `k` INT,
            `v` INT SUM
        ) AGGREGATE KEY (`k`) DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql "INSERT INTO leading_row_policy_t1 VALUES (1, 10), (2, 20)"
    sql "INSERT INTO leading_row_policy_t2 VALUES (1, 100), (2, 200)"
    sql "INSERT INTO leading_row_policy_agg VALUES (1, 10), (1, 30), (2, 20)"

    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '${pwd}'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.leading_row_policy_t1 TO ${user}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.leading_row_policy_t2 TO ${user}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.leading_row_policy_agg TO ${user}"
    sql """
        CREATE ROW POLICY leading_row_policy ON ${dbName}.leading_row_policy_t1
        AS RESTRICTIVE TO ${user} USING (k = 1)
    """
    sql """
        CREATE ROW POLICY leading_row_policy_agg ON ${dbName}.leading_row_policy_agg
        AS RESTRICTIVE TO ${user} USING (k = 1)
    """
    sql "SYNC"

    connect(user, "${pwd}", url) {
        sql "SET enable_sql_cache = false"
        // the row policy only allows to read the rows of leading_row_policy_t1 with k = 1
        order_qt_read_table "SELECT k, v FROM leading_row_policy_t1 ORDER BY k"
        // the leading hint only changes the join order, it must not change the rows allowed by the row policy
        order_qt_join_without_hint """
            SELECT leading_row_policy_t1.k, leading_row_policy_t1.v, leading_row_policy_t2.v
            FROM leading_row_policy_t1 JOIN leading_row_policy_t2
            ON leading_row_policy_t1.k = leading_row_policy_t2.k
            ORDER BY leading_row_policy_t1.k
        """
        order_qt_join_with_leading """
            SELECT /*+ leading(t1 t2) */ t1.k, t1.v, t2.v
            FROM leading_row_policy_t1 t1 JOIN leading_row_policy_t2 t2 ON t1.k = t2.k
            ORDER BY t1.k
        """
        order_qt_join_with_leading_swapped """
            SELECT /*+ leading(t2 t1) */ t1.k, t1.v, t2.v
            FROM leading_row_policy_t1 t1 JOIN leading_row_policy_t2 t2 ON t1.k = t2.k
            ORDER BY t1.k
        """
        order_qt_left_join_with_leading """
            SELECT /*+ leading(t1 t2) */ t1.k, t1.v, t2.v
            FROM leading_row_policy_t1 t1 LEFT JOIN leading_row_policy_t2 t2 ON t1.k = t2.k
            ORDER BY t1.k
        """
        // the aggregate table with random distribution also has to keep the aggregation and the row policy
        order_qt_agg_table_without_hint """
            SELECT leading_row_policy_agg.k, leading_row_policy_agg.v, leading_row_policy_t2.v
            FROM leading_row_policy_agg JOIN leading_row_policy_t2
            ON leading_row_policy_agg.k = leading_row_policy_t2.k
            ORDER BY leading_row_policy_agg.k
        """
        order_qt_agg_table_with_leading """
            SELECT /*+ leading(t1 t2) */ t1.k, t1.v, t2.v
            FROM leading_row_policy_agg t1 JOIN leading_row_policy_t2 t2 ON t1.k = t2.k
            ORDER BY t1.k
        """
    }
}
