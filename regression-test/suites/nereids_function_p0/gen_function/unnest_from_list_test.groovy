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

suite("unnest_from_list_test", "unnest") {

    String prefix_str = "unnest_from_list_"
    def tb_name1 = prefix_str + "table1"
    def tb_name2 = prefix_str + "table2"
    def tb_name3 = prefix_str + "table3"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            user_id INT,
            user_name VARCHAR(50),
            tags ARRAY<STRING>,           
            tags_2 ARRAY<array<STRING>>,  
            login_days BITMAP,           
            ext_info MAP<STRING, STRING>,
            u_id INT,
            u_name VARCHAR(50),
            user_tags ARRAY<STRING>,
            user_tags_2 ARRAY<STRING>
        ) 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name1} VALUES
        (1, 'Alice', ['tech', 'music'], [['tech', 'music'], ['tech', 'music']], to_bitmap(1), {'source': 'web', 'level': 'gold'}, 1, 'Alice', ['Java', 'SQL', 'str3'], ['Java', 'SQL']),
        (2, 'Bob', ['sport'], [['sport'], ['sport']], bitmap_from_string('1,2'), {'source': 'app'}, 1, 'Alice', ['Java', 'SQL', 'str3'], ['Java', 'SQL']),
        (3, 'Charlie', [], [[]], bitmap_empty(), {}, 1, 'Alice', ['Java', 'SQL', 'str3'], ['Java', 'SQL']);"""

    sql """drop table if exists ${tb_name2}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name2} (
            tag_name VARCHAR(50),
            tag_group VARCHAR(50),
            d_id INT,
            dept_name VARCHAR(50),
            dept_tags ARRAY<STRING>,
            dept_tags_2 ARRAY<STRING>
        ) 
        DISTRIBUTED BY HASH(tag_name) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name2} VALUES 
            ('tech', 'Work', 1, 'R&D', ['Microservice', 'Docker'], ['Microservice', 'Docker']), 
            ('music', 'Life', 1, 'R&D', ['Microservice', 'Docker'], ['Microservice', 'Docker']), 
            ('sport', 'Life', 1, 'R&D', ['Microservice', 'Docker'], ['Microservice', 'Docker']);"""

    // Test an implicit CROSS JOIN with UNNEST on an array column.
    qt_implicit_cross_join_unnest_array """SELECT u.user_name, t.tag, t.pos FROM ${tb_name1} u, UNNEST(u.tags) WITH ORDINALITY AS t(tag, pos) ORDER BY u.user_name, t.pos;"""

    // Test an implicit CROSS JOIN with UNNEST on a bitmap column.
    qt_implicit_cross_join_unnest_bitmap """SELECT u.user_name, b.day FROM ${tb_name1} u, UNNEST(u.login_days) AS b(day) ORDER BY u.user_name, b.day;"""

    // Cross Join
    // Test an explicit CROSS JOIN with UNNEST, followed by a subsequent JOIN.
    qt_explicit_cross_join_unnest_then_join """
        SELECT 
            u.user_name, 
            expl_tags.tag_name, 
            tc.tag_group
        FROM ${tb_name1} u
        CROSS JOIN UNNEST(u.tags) AS expl_tags(tag_name)
        JOIN ${tb_name2} tc ON expl_tags.tag_name = tc.tag_name
        ORDER BY u.user_name, expl_tags.tag_name, tc.tag_group;"""

    // const virtual table
    // Test UNNEST on a constant array to create a virtual table.
    qt_unnest_constant_array """SELECT * FROM UNNEST(['a', 'b', 'c']) WITH ORDINALITY AS t(val, pos) ORDER BY pos;"""

    // more unnest
    // Test multiple UNNEST clauses in the same FROM clause for an array and a map.
    qt_multiple_unnest_in_from """
        SELECT 
            u.user_name, 
            t.tag, 
            m.info_item
        FROM ${tb_name1} u, 
             UNNEST(u.tags) AS t(tag), 
             UNNEST(u.ext_info) AS m(info_item)
        ORDER BY u.user_name, t.tag, m.info_item;"""

    // subquery
    // Test using UNNEST within a subquery.
    qt_unnest_in_subquery """
        SELECT tmp.user_name, tmp.tag 
        FROM (
            SELECT u.user_name, t.tag 
            FROM ${tb_name1} u, UNNEST(u.tags) AS t(tag)
        ) tmp
        ORDER BY tmp.user_name, tmp.tag;"""


    // join
    // Test a LEFT JOIN with UNNEST.
    qt_left_join_unnest """SELECT u.user_name, t.tag FROM ${tb_name1} u LEFT JOIN UNNEST(u.tags) WITH ORDINALITY AS t(tag, pos) ON true ORDER BY u.user_name, t.pos;"""

    // Test an explicit CROSS JOIN with UNNEST.
    qt_explicit_cross_join_unnest """SELECT u.user_name, t.tag FROM ${tb_name1} u CROSS JOIN UNNEST(u.tags) AS t(tag) ORDER BY u.user_name, t.tag;"""

    test {
        // Test that FULL JOIN with UNNEST is not supported and fails as expected.
        sql """SELECT u.user_name, t.tag FROM ${tb_name1} u FULL JOIN UNNEST(u.tags) AS t(tag) ON false;""" // fail, don't support
        exception "The combining JOIN type must be INNER, LEFT or CROSS for UNNEST"
    }

    // Test chained UNNEST on a nested array, where the second UNNEST depends on the first.
    qt_chained_unnest_on_nested_array """SELECT u.user_name, t1.sub_arr, t2.item FROM ${tb_name1} u, UNNEST(u.tags_2) AS t1(sub_arr), UNNEST(t1.sub_arr) AS t2(item) ORDER BY u.user_name, t2.item;"""

    // more table dependency
    // Test unnesting multiple arrays from two implicitly joined tables.
    qt_multi_table_multi_array_unnest_implicit """SELECT 
            a.u_name, 
            b.dept_name, 
            t.combined_tag,
            t.combined_tag_2,
            t.combined_tag_3,
            t.combined_tag_4
        FROM (${tb_name1} a, ${tb_name2} b)
        CROSS JOIN UNNEST(a.user_tags, b.dept_tags, a.user_tags_2, b.dept_tags_2) AS t(combined_tag, combined_tag_2, combined_tag_3, combined_tag_4)
        WHERE a.u_id = b.d_id
        ORDER BY a.u_name, b.dept_name, t.combined_tag, t.combined_tag_2, t.combined_tag_3, t.combined_tag_4;
        """

    // Test unnesting multiple arrays from two tables listed in the FROM clause.
    qt_multi_table_multi_array_unnest_from """SELECT 
            a.u_name, 
            b.dept_name, 
            t.combined_tag,
            t.combined_tag_2,
            t.combined_tag_3,
            t.combined_tag_4
        FROM ${tb_name1} a, ${tb_name2} b, UNNEST(a.user_tags, b.dept_tags, a.user_tags_2, b.dept_tags_2) AS t(combined_tag, combined_tag_2, combined_tag_3, combined_tag_4)
        WHERE a.u_id = b.d_id
        ORDER BY a.u_name, b.dept_name, t.combined_tag, t.combined_tag_2, t.combined_tag_3, t.combined_tag_4;
        """

    // Test unnesting multiple arrays from two tables using explicit JOIN and CROSS JOIN syntax.
    qt_multi_table_multi_array_unnest_explicit """SELECT 
            a.u_name, 
            b.dept_name, 
            t.combined_tag,
            t.combined_tag_2,
            t.combined_tag_3,
            t.combined_tag_4
        FROM ${tb_name1} a
        JOIN ${tb_name2} b ON a.u_id = b.d_id
        CROSS JOIN UNNEST(a.user_tags, b.dept_tags, a.user_tags_2, b.dept_tags_2) AS t(combined_tag, combined_tag_2, combined_tag_3, combined_tag_4)
        ORDER BY a.u_name, b.dept_name, t.combined_tag, t.combined_tag_2, t.combined_tag_3, t.combined_tag_4;
        """

    sql """drop table if exists ${tb_name3}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name3} (
            case_id INT,
            case_desc VARCHAR(100),
            big_bitmap BITMAP,
            long_array ARRAY<STRING>
        ) 
        DUPLICATE KEY(case_id)
        DISTRIBUTED BY HASH(case_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name3}
        SELECT 
            1, 
            '100K_ID_Bitmap', 
            BITMAP_UNION(to_bitmap(number)),
            []
        FROM numbers("number" = "100000");"""

    sql """INSERT INTO ${tb_name3} SELECT 2, 'Long_Array_Pushdown', to_bitmap(1), collect_list(CONCAT('tag_', number)) FROM numbers("number" = "1000");"""

    sql """
        INSERT INTO ${tb_name3}
        SELECT 
            3, 
            'Search_Target', 
            to_bitmap(1), 
            array_union(collect_list(CONCAT('val_', number)), ['tech'])
        FROM numbers("number" = "500");"""

    // Stress test UNNEST on a large bitmap and perform a COUNT aggregation.
    qt_stress_test_unnest_large_bitmap """
        SELECT COUNT(*) 
        FROM ${tb_name3} s, UNNEST(s.big_bitmap) AS t(uid)
        WHERE s.case_id = 1;"""

    // Stress test UNNEST on a long array with a predicate on the unnested column.
    qt_stress_test_unnest_long_array_with_predicate """
        SELECT s.case_id, t.tag
        FROM ${tb_name3} s, UNNEST(s.long_array) AS t(tag)
        WHERE s.case_id = 3 AND t.tag = 'tech'
        ORDER BY s.case_id, t.tag;"""

}
