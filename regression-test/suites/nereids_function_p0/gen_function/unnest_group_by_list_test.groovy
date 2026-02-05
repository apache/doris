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

suite("unnest_group_by_list_test", "unnest") {

    String prefix_str = "unnest_group_by_list_"
    def tb_name1 = prefix_str + "table1"
    def tb_name2 = prefix_str + "table2"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            user_id INT,
            dept_name VARCHAR(20),
            tags ARRAY<STRING>,        
            scores BITMAP,             
            kv_data MAP<STRING, STRING>
        ) 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name1} VALUES
        (1, 'Sales', ['high', 'potential'], bitmap_from_string("80, 90"), {'level': 'A'}),
        (2, 'Sales', ['potential', 'new'], bitmap_from_string("70, 80"), {'level': 'B'}),
        (3, 'Tech', ['high', 'expert'], bitmap_from_string("90, 95"), {'level': 'A'}),
        (4, 'Tech', ['expert'], bitmap_from_string("95"), {'level': 'A'});"""

    // array
    // Test grouping by an unnested array column and counting the occurrences of each element.
    qt_group_by_unnested_array """SELECT t.tag, COUNT(*) FROM ${tb_name1}, UNNEST(tags) AS t(tag) GROUP BY t.tag ORDER BY t.tag;"""

    // bitmap
    // Test grouping by an unnested bitmap column and counting distinct users for each value.
    qt_group_by_unnested_bitmap """SELECT b.score, COUNT(DISTINCT user_id) FROM ${tb_name1}, UNNEST(scores) AS b(score) GROUP BY b.score ORDER BY b.score;"""

    // map
    // Test grouping by the values of an unnested map and counting their occurrences.
    qt_group_by_unnested_map """SELECT m.item, COUNT(*) FROM ${tb_name1}, UNNEST(kv_data) AS m(item) GROUP BY m.item ORDER BY m.item;"""

    // Test grouping by both a regular column and an unnested column.
    qt_group_by_column_and_unnested_column """
        SELECT 
            dept_name col1, 
            t.tag col2, 
            COUNT(*) as tag_count
        FROM ${tb_name1}, UNNEST(tags) AS t(tag)
        GROUP BY dept_name, t.tag
        ORDER BY col1, col2, tag_count DESC;"""

    // with ORDINALITY
    // Test grouping on an unnested column after filtering by the element's position from ORDINALITY.
    qt_group_by_unnested_array_with_ordinality_filter """
        SELECT 
            t.tag, 
            COUNT(*) 
        FROM ${tb_name1}, UNNEST(tags) WITH ORDINALITY AS t(pos, tag)
        WHERE t.pos = 1
        GROUP BY t.tag
        ORDER BY t.tag;"""

    // Test GROUP BY ROLLUP with a regular column and an unnested column.
    qt_rollup_with_unnested_column """
        SELECT 
            dept_name, 
            t.tag, 
            COUNT(*) 
        FROM ${tb_name1}, UNNEST(tags) AS t(tag)
        GROUP BY ROLLUP(dept_name, t.tag)
        ORDER BY dept_name, t.tag;"""

    // Test grouping by a regular column and an UNNEST function call directly.
    qt_group_by_unnest_function """SELECT dept_name FROM ${tb_name1} GROUP BY dept_name, UNNEST(tags) ORDER BY dept_name;"""

    test {
        // Test that using an aggregate function on an UNNEST result in the SELECT list and grouping by it is invalid.
        sql """SELECT SUM(UNNEST(scores)) as col1 FROM ${tb_name1} group by col1;"""
        exception "GROUP BY expression must not contain aggregate functions"
    }

    sql """drop table if exists ${tb_name2}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name2} (
            user_id INT,
            dept_name VARCHAR(20),
            position VARCHAR(20),
            skills ARRAY<STRING> 
        ) 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name2} VALUES
        (1, 'R&D', 'Manager', ['Java', 'Go']),
        (2, 'R&D', 'Dev', ['Java', 'SQL']),
        (3, 'Sales', 'Manager', ['SQL']),
        (4, 'Sales', 'Trainee', ['Excel', 'SQL']);"""

    // rollup
    // Test GROUP BY ROLLUP with a column and an unnested column from a CROSS JOIN.
    qt_rollup_with_cross_join_unnest """
        SELECT 
            dept_name, 
            s.skill, 
            COUNT(DISTINCT user_id) as user_count
        FROM ${tb_name2}
        CROSS JOIN UNNEST(skills) WITH ORDINALITY AS s(pos, skill)
        GROUP BY ROLLUP(dept_name, s.skill)
        ORDER BY dept_name, s.skill;"""

    // GROUPING SETS
    // Test GROUP BY GROUPING SETS involving columns from both the original and the unnested table.
    qt_grouping_sets_with_unnested_column """
        SELECT 
            dept_name, 
            position, 
            s.skill, 
            GROUPING(dept_name) as g_dept,
            GROUPING(s.skill) as g_skill,
            COUNT(*) 
        FROM ${tb_name2}
        CROSS JOIN UNNEST(skills) WITH ORDINALITY AS s(skill, pos)
        GROUP BY GROUPING SETS (
            (dept_name, s.skill),
            (position, s.skill),
            (s.skill)
        )
        ORDER BY dept_name, position, s.skill;"""

    // cube
    // Test GROUP BY CUBE involving columns from both the original and the unnested table.
    qt_cube_with_unnested_column """
        SELECT 
            dept_name, 
            position, 
            s.skill, 
            COUNT(*) 
        FROM ${tb_name2}
        CROSS JOIN UNNEST(skills) WITH ORDINALITY AS s(skill, pos)
        GROUP BY CUBE(dept_name, position, s.skill)
        ORDER BY dept_name, position, s.skill;"""

    // Test GROUP BY GROUPING SETS with the ordinality column from UNNEST.
    qt_grouping_sets_with_unnest_ordinality """
        SELECT 
            dept_name, 
            s.pos, 
            COUNT(*) 
        FROM ${tb_name2}
        CROSS JOIN UNNEST(skills) WITH ORDINALITY AS s(skill, pos)
        GROUP BY GROUPING SETS (
            (dept_name, s.pos),
            (s.pos)
        )
        ORDER BY dept_name, s.pos;"""

    // Test a full query pipeline with UNNEST, including WHERE, GROUP BY, and HAVING clauses.
    qt_group_by_with_where_and_having """
        SELECT 
            s.skill, 
            COUNT(*) as freq
        FROM ${tb_name2}
        CROSS JOIN UNNEST(skills) AS s(skill)
        WHERE dept_name = 'R&D'
        GROUP BY s.skill
        HAVING freq > 1
        ORDER BY s.skill;"""
}
