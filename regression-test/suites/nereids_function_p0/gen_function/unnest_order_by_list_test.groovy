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

suite("unnest_order_by_list_test", "unnest") {
    
    String prefix_str = "unnest_order_by_list_"
    def tb_name1 = prefix_str + "table1"
    def tb_name2 = prefix_str + "table2"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            user_id INT,
            user_name VARCHAR(20),
            scores ARRAY<INT>,
            login_dates BITMAP
        ) 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name1} VALUES
        (1, 'Alice', [90, 70, 85], bitmap_from_string("10, 20")),
        (2, 'Bob', [60, 80], bitmap_from_string("5, 15")),
        (3, 'Charlie', [95], bitmap_from_string("30"));"""

    // Test ordering the result set by the unnested value.
    qt_order_by_unnested_value """SELECT user_name, s.val FROM ${tb_name1}, UNNEST(scores) AS s(val) ORDER BY s.val DESC;"""

    // Test ordering by both the position (ordinality) and the value from UNNEST.
    qt_order_by_ordinality_and_value """SELECT user_name, s.val, s.pos+1 FROM ${tb_name1}, UNNEST(scores) WITH ORDINALITY AS s(pos, val) ORDER BY s.pos ASC, s.val DESC;"""

    // Test ordering by the value unnested from a bitmap.
    qt_order_by_unnested_bitmap """SELECT user_name, b.day FROM ${tb_name1}, UNNEST(login_dates) AS b(day) ORDER BY b.day ASC;"""

    // Test ordering by a regular column and the position from UNNEST.
    qt_order_by_column_and_ordinality """
        SELECT user_name, s.val, s.pos 
        FROM ${tb_name1}, UNNEST(scores) WITH ORDINALITY AS s(pos, val)
        ORDER BY user_name ASC, s.pos ASC;"""

    // Test ordering by an expression that involves the unnested value.
    qt_order_by_expression_on_unnested_value """
        SELECT user_name, s.val 
        FROM ${tb_name1}, UNNEST(scores) AS s(val)
        ORDER BY ABS(s.val - 80) ASC;"""

    // Test ordering by an aggregate column and the unnested column after a GROUP BY operation.
    qt_order_by_aggregate_and_unnested_value """SELECT s.val, COUNT(*) as freq FROM ${tb_name1}, UNNEST(scores) AS s(val) GROUP BY s.val ORDER BY freq DESC, s.val ASC;"""

    // Test using UNNEST in the SELECT list and ordering by its resulting alias.
    qt_order_by_unnest_alias_in_select """SELECT user_name, UNNEST(scores) AS score_val FROM ${tb_name1} ORDER BY score_val DESC;"""

    // Test ordering by the UNNEST function call directly in the ORDER BY clause.
    qt_order_by_unnest_function_call """SELECT user_name FROM ${tb_name1} ORDER BY UNNEST(scores);"""

    sql """drop table if exists ${tb_name2}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name2} (
            user_id INT,
            subject VARCHAR(20),
            history_scores ARRAY<INT>
        ) 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name2} VALUES
        (1, 'Math', [80, 95, 70]),
        (1, 'English', [90, 85]),
        (2, 'Math', [60, 75]);"""

    // Test using the unnested value within the ORDER BY clause of a window function.
    qt_window_function_order_by_unnested_value """
        SELECT 
            user_id, 
            subject, 
            s.val, 
            RANK() OVER (PARTITION BY user_id, subject ORDER BY s.val DESC) as score_rank
        FROM ${tb_name2}, UNNEST(history_scores) AS s(val);"""

    // Test an ORDER BY clause on a query that involves WHERE and UNNEST.
    qt_order_by_after_where_and_unnest """
        SELECT 
            user_id, 
            s.pos,             
            s.val            
        FROM ${tb_name2}, UNNEST(history_scores) WITH ORDINALITY AS s(pos, val)
        WHERE subject = 'Math'
        ORDER BY user_id, s.pos ASC;"""

    // Test using ORDER BY on an unnested value in combination with a LIMIT clause.
    qt_order_by_unnested_value_with_limit """
        SELECT 
            user_id, 
            s.val 
        FROM ${tb_name2}, UNNEST(history_scores) AS s(val)
        ORDER BY s.val DESC 
        LIMIT 3;"""

    // Test ordering on an unnested column from a LEFT JOIN, including NULLS LAST handling.
    qt_order_by_unnested_from_left_join """
        SELECT 
            u.user_name, 
            t.tag 
        FROM ${tb_name1} u 
        LEFT JOIN UNNEST(u.scores) AS t(tag) ON true
        ORDER BY t.tag DESC NULLS LAST;"""



}
