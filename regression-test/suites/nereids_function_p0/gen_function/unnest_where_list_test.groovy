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

suite("unnest_where_list_test", "unnest") {

    String prefix_str = "unnest_where_list_"
    def tb_name1 = prefix_str + "table1"
    def tb_name2 = prefix_str + "table2"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            user_id INT,
            user_name VARCHAR(50),
            user_type VARCHAR(20),
            tags ARRAY<STRING>,
            login_days BITMAP,
            ext_props MAP<STRING, STRING>
        ) 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name1} VALUES
        (1, 'Alice', 'VIP', ['tech', 'music', 'sport'], bitmap_from_string("1, 2, 10"), {'level': 'gold'}),
        (2, 'Bob', 'Normal', ['teach', 'math'], bitmap_from_string("15, 20"), {'level': 'silver'}),
        (3, 'Charlie', 'Normal', [], bitmap_empty(), {}),
        (4, 'David', 'VIP', NULL, bitmap_empty(), NULL);"""

    sql """drop table if exists ${tb_name2}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name2} (
            ref_id INT,
            ref_tag STRING
        ) DISTRIBUTED BY HASH(ref_id) BUCKETS 1 PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name2} VALUES (1, 'tech'), (2, 'math');"""

    // subquery nested
    test {
        // Test a correlated EXISTS subquery with UNNEST in the WHERE clause, which is expected to fail.
        sql """SELECT user_name FROM ${tb_name1} u WHERE EXISTS (SELECT 1 FROM UNNEST(u.tags) AS t(tag) WHERE t.tag = 'tech');"""
        exception "unnest can't access outer query table's column"
    }

    test {
        // Test a correlated IN subquery with UNNEST in the WHERE clause, which is expected to fail.
        sql """SELECT user_name FROM ${tb_name1} u WHERE 'math' IN (SELECT tag FROM UNNEST(u.tags) AS t(tag));"""
        exception "unnest can't access outer query table's column"
    }

    // subquery filter
    // Test filtering the result of an UNNEST from a subquery's SELECT list in the outer WHERE clause.
    qt_where_filter_on_unnest_in_subquery """
        SELECT DISTINCT user_name FROM (
            SELECT user_name, UNNEST(login_days) AS day FROM ${tb_name1}
        ) t WHERE t.day BETWEEN 10 AND 20
        ORDER BY user_name;"""

    // Test filtering unnested results using a REGEXP predicate in the WHERE clause.
    qt_where_regexp_filter_on_unnest """
        SELECT user_name, t.tag FROM (
            SELECT user_name, UNNEST(tags) AS tag FROM ${tb_name1}
        ) t WHERE t.tag REGEXP '^t.*h\$'
        ORDER BY user_name, t.tag;"""

    // mix logical judge
    // Test a complex WHERE clause with AND/OR logic on both regular and unnested columns.
    qt_where_complex_logic_on_unnest """
        SELECT * FROM (
            SELECT user_name, user_type, UNNEST(tags) AS tag FROM ${tb_name1}
        ) t 
        WHERE (t.user_type = 'VIP' AND t.tag LIKE 'm%') 
           OR (t.tag IS NULL)
        ORDER BY t.user_name, t.user_type, t.tag NULLS LAST;"""

    // Test a simple WHERE clause filter on a value from an unnested bitmap.
    qt_where_simple_filter_on_unnested_bitmap """
        SELECT user_name, day FROM (
            SELECT user_name, UNNEST(login_days) AS day FROM ${tb_name1}
        ) t WHERE t.day > 10
        ORDER BY user_name, day;"""

    // boundary fail
    test {
        // Test that using UNNEST directly in the WHERE clause is not allowed and fails as expected.
        sql """SELECT * FROM ${tb_name1} WHERE UNNEST(tags) = 'tech';"""
        exception "table generating function is not allowed in LOGICAL_FILTER"
    }

    // join filter
    // Test using the WHERE clause as a join condition between an unnested column and another table.
    qt_where_as_join_condition_for_unnest """
        SELECT u.user_name, t.tag 
        FROM (${tb_name1} u, UNNEST(u.tags) AS t(tag))
        JOIN ${tb_name2} d ON 1=1
        WHERE t.tag = d.ref_tag
        ORDER BY u.user_name, t.tag;"""

    // filter before aqg
    // Test filtering unnested results in the WHERE clause before applying an aggregation.
    qt_where_filter_before_aggregation_on_unnest """
        SELECT COUNT(DISTINCT user_id) 
        FROM ${tb_name1} u, UNNEST(u.tags) AS t(tag)
        WHERE t.tag LIKE 'm%';"""

    // table column same name
    // Test filtering on an unnested column that has been aliased to the same name as another column.
    qt_where_filter_on_conflicting_alias """
        SELECT * FROM (
            SELECT user_id, UNNEST(tags) AS user_name FROM ${tb_name1}
        ) tmp
        WHERE tmp.user_name = 'tech'
        ORDER BY tmp.user_id, tmp.user_name;"""
}
