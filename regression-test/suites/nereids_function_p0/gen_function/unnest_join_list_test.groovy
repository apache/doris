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

suite("unnest_join_list_test", "unnest") {

    String prefix_str = "unnest_join_list_"
    def tb_name1 = prefix_str + "table1"
    def tb_name2 = prefix_str + "table2"
    def tb_name3 = prefix_str + "table3"
    def tb_name4 = prefix_str + "table4"
    def tb_name5 = prefix_str + "table5"
    def tb_name6 = prefix_str + "table6"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            u_id INT,
            u_name VARCHAR(50),
            u_tags ARRAY<STRING>,        
            u_logins BITMAP,             
            u_ext MAP<STRING, STRING>   
        ) 
        DUPLICATE KEY(u_id)
        DISTRIBUTED BY HASH(u_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name1} VALUES
        (1, 'Alice', ['work', 'gym'], bitmap_from_string("101, 102"), {'city': 'shanghai'}),
        (2, 'Bob', ['game'], bitmap_empty(), {'city': 'beijing'}),
        (3, 'Charlie', [], to_bitmap(103), {}),
        (101, 'Alice', ['work', 'gym'], bitmap_from_string("101, 102"), {'city': 'shanghai'}),
        (102, 'Bob', ['game'], bitmap_empty(), {'city': 'beijing'}),
        (103, 'Charlie', [], to_bitmap(103), {});"""

    sql """drop table if exists ${tb_name2}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name2} (
            tag_name VARCHAR(50),
            tag_group VARCHAR(20)
        ) 
        DISTRIBUTED BY HASH(tag_name) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name2} VALUES ('work', 'Business'), ('gym', 'Health'), ('game', 'Entertain');"""

    sql """drop table if exists ${tb_name3}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name3} (
            category_name VARCHAR(20),
            manager_name VARCHAR(50),
            office_location VARCHAR(50)
        ) 
        DISTRIBUTED BY HASH(category_name) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name3} VALUES 
        ('Business', 'David', 'Tower A'), 
        ('Health', 'Eve', 'Tower B'), 
        ('Entertain', 'Frank', 'Tower C');"""

    // lateral join
    // Test a lateral join using `JOIN UNNEST` with an ON true condition.
    qt_lateral_join_unnest """SELECT u.u_name, t.tag, t.pos FROM ${tb_name1} u JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(pos, tag) ON true ORDER BY u.u_name, t.pos;"""

    // on filter
    // Test a lateral join with a filter on the unnested table in the ON clause.
    qt_lateral_join_unnest_with_on_filter """SELECT u.u_name, t.tag FROM ${tb_name1} u JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(pos, tag) ON t.pos = 1 ORDER BY u.u_name, t.tag;"""

    // more table join
    // Test a chain of joins following an UNNEST operation.
    qt_chain_join_after_unnest """SELECT u.u_name, t.tag, d.tag_group FROM ${tb_name1} u JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(pos, tag) ON true JOIN ${tb_name2} d ON t.tag = d.tag_name ORDER BY u.u_name, t.tag, d.tag_group;"""

    // bitmap
    // Test unnesting a bitmap and then joining the results back to the same table.
    qt_unnest_bitmap_and_join_back """
        SELECT u.u_name AS owner, b.login_id, u2.u_name AS visitor
        FROM ${tb_name1} u 
        JOIN UNNEST(u.u_logins) AS b(login_id) ON true
        JOIN ${tb_name1} u2 ON b.login_id = u2.u_id
        ORDER BY owner, login_id, visitor;"""

    test {
        // Test that using UNNEST directly within a JOIN condition is not allowed and fails as expected.
        sql """SELECT u.u_name AS owner, b.login_id, u2.u_name AS visitor
                FROM ${tb_name1} u 
                JOIN UNNEST(u.u_logins) AS b(login_id) ON true
                JOIN ${tb_name1} u2 ON b.login_id = unnest(u2.u_logins);
            """
        exception "table generating function is not allowed in LOGICAL_PROJECT"
    }

    // map
    // Test joining with an unnested map and applying a filter in the ON clause.
    qt_join_unnest_map_with_filter """
        SELECT u.u_name, m.entry
        FROM ${tb_name1} u 
        JOIN UNNEST(u.u_ext) AS m(entry) ON m.entry LIKE '%city%'
        ORDER BY u.u_name, m.entry;"""

    // left join
    // Test a LEFT JOIN with UNNEST, preserving left-side rows for empty arrays.
    qt_left_join_unnest_array """
        SELECT u.u_name, t.tag, t.pos
        FROM ${tb_name1} u 
        LEFT JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(pos, tag) ON true
        ORDER BY u.u_name, t.pos;"""

    // cross join
    // Test an explicit CROSS JOIN with UNNEST.
    qt_explicit_cross_join_unnest_array """
        SELECT u.u_name, t.tag
        FROM ${tb_name1} u 
        CROSS JOIN UNNEST(u.u_tags) AS t(tag)
        ORDER BY u.u_name, t.tag;"""

    // complex compute
    // Test joining with UNNEST using a more complex condition in the ON clause.
    qt_join_unnest_with_complex_on_condition """
        SELECT u.u_name, t.tag, t.pos
        FROM ${tb_name1} u 
        JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(pos, tag) ON t.pos > 0
        ORDER BY u.u_name, t.pos;"""

    // Standard chain type Join (Inner Join)
    // Test a multi-level INNER JOIN chain that starts with an UNNEST.
    qt_multi_level_inner_join_chain_with_unnest """
        SELECT 
            u.u_name, 
            t.tag, 
            d.tag_group, 
            m.manager_name
        FROM ${tb_name1} u
        JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(pos, tag) ON t.pos <= 2
        JOIN ${tb_name2} d ON t.tag = d.tag_name
        JOIN ${tb_name3} m ON d.tag_group = m.category_name
        ORDER BY u.u_name, t.tag, d.tag_group, m.manager_name;"""

    // Mixed Left Join Link (Null Value Propagation Test)
    // Test a multi-level LEFT JOIN chain starting with an UNNEST to check null propagation.
    qt_multi_level_left_join_chain_with_unnest """
        SELECT 
            u.u_name, 
            t.tag, 
            d.tag_group, 
            m.manager_name
        FROM ${tb_name1} u
        LEFT JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(tag, pos) ON true
        LEFT JOIN ${tb_name2} d ON t.tag = d.tag_name
        LEFT JOIN ${tb_name3} m ON d.tag_group = m.category_name
        ORDER BY u.u_name, t.tag NULLS LAST, d.tag_group NULLS LAST, m.manager_name NULLS LAST;"""

    // Three-table cross-filtering in ON conditions
    // Test a JOIN condition that references columns from multiple preceding tables in the chain, including the unnested table.
    qt_join_condition_with_multi_table_references """
        SELECT 
            u.u_name, 
            t.tag, 
            m.manager_name
        FROM ${tb_name1} u
        JOIN UNNEST(u.u_tags) WITH ORDINALITY AS t(pos, tag) ON true
        JOIN ${tb_name2} d ON t.tag = d.tag_name
        JOIN ${tb_name3} m ON d.tag_group = m.category_name AND t.pos = 1
        ORDER BY u.u_name, t.tag, m.manager_name;"""

    // Bitmap Multi-level Perspective Test
    // Test a complex join chain involving UNNEST on a bitmap, a self-join, and a LEFT JOIN with a complex condition.
    qt_complex_join_chain_with_bitmap_unnest """
        SELECT 
            u.u_name AS owner, 
            b.login_id, 
            u2.u_name AS visitor,
            m.manager_name
        FROM ${tb_name1} u
        CROSS JOIN UNNEST(u.u_logins) AS b(login_id)
        JOIN ${tb_name1} u2 ON b.login_id = u2.u_id
        LEFT JOIN ${tb_name3} m ON m.category_name = 'Business' AND u2.u_name = 'Alice'
        ORDER BY owner, login_id, visitor, m.manager_name NULLS LAST;"""

    sql """drop table if exists ${tb_name4}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name4} (
            u_id INT,
            u_name VARCHAR(50),
            base_tags ARRAY<STRING>,
            ext_tags ARRAY<STRING>
        ) DUPLICATE KEY(u_id) DISTRIBUTED BY HASH(u_id) BUCKETS 1 PROPERTIES("replication_num" = "1");"""

    sql """drop table if exists ${tb_name5}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name5} (
            d_id INT,
            d_name VARCHAR(50),
            dept_tags ARRAY<STRING>
        ) DUPLICATE KEY(d_id) DISTRIBUTED BY HASH(d_id) BUCKETS 1 PROPERTIES("replication_num" = "1");"""

    sql """drop table if exists ${tb_name6}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name6} (
            t_name VARCHAR(50),
            t_level INT
        ) DISTRIBUTED BY HASH(t_name) BUCKETS 1 PROPERTIES("replication_num" = "1");"""

    sql """INSERT INTO ${tb_name4} VALUES (1, 'Alice', ['tech', 'db'], ['p1', 'p2']);"""
    sql """INSERT INTO ${tb_name5} VALUES (1, 'R&D', ['infra', 'cloud']);"""
    sql """INSERT INTO ${tb_name6} VALUES ('tech', 1), ('db', 2), ('p1', 1);"""

    // Join type restrictions (lateral association dependency)
    // Test a simple LEFT JOIN with UNNEST.
    qt_simple_left_join_unnest """
        SELECT u.u_name, t.tag 
        FROM ${tb_name4} u 
        LEFT JOIN UNNEST(u.base_tags) WITH ORDINALITY AS t(tag, pos) ON true
        ORDER BY u.u_name, t.tag NULLS LAST;"""

    // Test a simple CROSS JOIN with UNNEST.
    qt_simple_cross_join_unnest """
        SELECT u.u_name, t.tag 
        FROM ${tb_name4} u 
        CROSS JOIN UNNEST(u.base_tags) AS t(tag)
        ORDER BY u.u_name, t.tag;"""

    test {
        // Test that RIGHT JOIN with a lateral UNNEST is not supported and fails as expected.
        sql """
            SELECT u.u_name, t.tag 
            FROM ${tb_name4} u 
            RIGHT JOIN UNNEST(u.base_tags) AS t(tag) ON true;"""
        exception "The combining JOIN type must be"
    }

    // Chain expansion and multi-table sequential connection
    // Test a chain join after unnesting and filtering the unnested results.
    qt_chain_join_after_filtered_unnest """
        SELECT u.u_name, t.tag, d.d_name, td.t_level
        FROM ${tb_name4} u
        JOIN UNNEST(u.base_tags) WITH ORDINALITY AS t(pos, tag) ON t.pos = 1
        JOIN ${tb_name5} d ON u.u_id = d.d_id
        JOIN ${tb_name6} td ON t.tag = td.t_name
        ORDER BY u.u_name, t.tag, d.d_name, td.t_level;"""

    // Full join type support for constant UNNEST
    // Test RIGHT JOIN with UNNEST on a constant array, which does not have a lateral dependency.
    qt_right_join_with_constant_unnest """
        SELECT u.u_name, c.val
        FROM ${tb_name4} u 
        RIGHT JOIN UNNEST(['tech', 'math']) AS c(val) ON u.u_name = c.val
        ORDER BY u.u_name NULLS LAST, c.val;"""

    // Test FULL JOIN with UNNEST on a constant array.
    qt_full_join_with_constant_unnest """
        SELECT u.u_name, c.val
        FROM ${tb_name4} u 
        FULL JOIN UNNEST(['tech', 'math']) AS c(val) ON u.u_name = c.val
        ORDER BY u.u_name NULLS LAST, c.val NULLS LAST;"""

    // Multi-column/multi-table field reference (CROSS JOIN Lateral)
    // Test unnesting multiple arrays from the same table at once.
    qt_unnest_multiple_arrays_same_table """SELECT u.u_name, t.combined_tag FROM ${tb_name4} u, UNNEST(u.base_tags, u.ext_tags) AS t(combined_tag) ORDER BY u.u_name, t.combined_tag;"""

}
