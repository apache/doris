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

suite("unnest_having_list_test", "unnest") {

    String prefix_str = "unnest_having_list_"
    def tb_name1 = prefix_str + "table1"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            user_id INT,
            dept_name VARCHAR(20),
            skills ARRAY<STRING>
        ) 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name1} VALUES
        (1, 'R&D', ['Java', 'Go', 'SQL']),
        (2, 'R&D', ['Java', 'SQL']),
        (3, 'Sales', ['SQL', 'Marketing']),
        (4, 'Sales', ['Marketing']);"""

    test {
        // Test that using UNNEST directly in a HAVING clause without it being in an aggregate function or the GROUP BY clause is invalid.
        sql """
            SELECT dept_name, COUNT(*) 
            FROM ${tb_name1} 
            GROUP BY dept_name 
            HAVING UNNEST(skills) = 'Java';"""
        exception "HAVING expression 'skills' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    // Test a HAVING clause on an aggregate function (COUNT) of an unnested column.
    qt_having_on_aggregated_unnested_column """SELECT s.skill, COUNT(*) as freq FROM ${tb_name1}, UNNEST(skills) AS s(skill) GROUP BY s.skill HAVING freq > 1 ORDER BY s.skill;"""

    // Test a HAVING clause on a COUNT(DISTINCT) of an unnested column, grouped by a regular column.
    qt_having_on_distinct_count_of_unnested """SELECT dept_name, COUNT(DISTINCT s.skill) as skill_cnt FROM ${tb_name1}, UNNEST(skills) AS s(skill) GROUP BY dept_name HAVING skill_cnt >= 3 ORDER BY dept_name;"""

    // Test a HAVING clause after filtering the unnested set by its position (ordinality).
    qt_having_after_filtering_by_ordinality """
        SELECT s.skill, COUNT(*) as first_skill_count
        FROM ${tb_name1}
        CROSS JOIN UNNEST(skills) WITH ORDINALITY AS s(pos, skill)
        WHERE s.pos = 0
        GROUP BY s.skill
        HAVING first_skill_count > 1
        ORDER BY s.skill;"""

    // Test a HAVING clause in combination with a GROUP BY ROLLUP on both a regular and an unnested column.
    qt_having_with_rollup_on_unnested """
        SELECT 
            dept_name, 
            s.skill, 
            COUNT(*) as total
        FROM ${tb_name1}
        CROSS JOIN UNNEST(skills) AS s(skill)
        GROUP BY ROLLUP(dept_name, s.skill)
        HAVING total > 2
        ORDER BY dept_name, s.skill;"""


}
