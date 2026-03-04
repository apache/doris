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

suite("complex_rec_cte_test", "rec_cte") {
    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "complex_rec_cte_"
    def tb_name1 = prefix_str + "tb1"
    def tb_name2 = prefix_str + "tb2"
    def tb_name3 = prefix_str + "tb3"
    def tb_name4 = prefix_str + "tb4"

    sql """drop table if exists ${tb_name1}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name1} (
            id INT,
            parent_id INT,
            dept_name VARCHAR(50),
            budget DECIMAL(18, 2)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");"""

    sql """drop table if exists ${tb_name2}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name2} (
            source_id INT,
            target_id INT,
            status VARCHAR(20)
        )
        DUPLICATE KEY(source_id)
        DISTRIBUTED BY HASH(source_id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");"""

    sql """drop table if exists ${tb_name3}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name3} (
            id INT,
            type VARCHAR(20)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """drop table if exists ${tb_name4}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tb_name4} (
            part_id INT,
            sub_part_id INT,
            qty DOUBLE
        )
        DUPLICATE KEY(part_id)
        DISTRIBUTED BY HASH(part_id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO ${tb_name1} VALUES 
        (1, NULL, '总部', 10000.00),
        (10, 1, '研发部', 5000.00),
        (11, 1, '市场部', 4000.00),
        (101, 10, '后端开发', 2000.00),
        (102, 10, '前端开发', 1500.00),
        (111, 11, '线上推广', 2000.00);"""

    sql """
        INSERT INTO ${tb_name3} VALUES (1, 'seed'), (2, 'normal'), (3, 'normal'), (4, 'normal');"""

    sql """
        INSERT INTO ${tb_name2} VALUES 
        (1, 2, 'active'), (2, 3, 'active'), (3, 4, 'active'), 
        (4, 1, 'active'), (2, 4, 'active'), (1, 3, 'inactive');"""

    sql """
        INSERT INTO ${tb_name4} VALUES 
        (1001, 2001, 2.0), (1001, 2002, 5.0),
        (2001, 3001, 1.5), (2001, 3002, 3.0),
        (2002, 4001, 1.0);"""

    sql """
        WITH RECURSIVE 
        dept_tree(id, parent_id, dept_name, path, budget) AS (
            SELECT 
                id, 
                parent_id, 
                dept_name, 
                CAST(dept_name AS CHAR(200)) AS path,
                budget
            FROM ${tb_name1} 
            WHERE parent_id IS NULL
            UNION ALL
            SELECT 
                t.id, 
                t.parent_id, 
                t.dept_name, 
                CAST(CONCAT(c.path, '->', t.dept_name) AS CHAR(200)),
                t.budget
            FROM ${tb_name1} t 
            JOIN dept_tree c ON t.parent_id = c.id
            WHERE c.id < 1000 
        ),
        budget_summary(dept_id, total_budget, lvl) AS (
            SELECT 
                id, 
                CAST(budget AS DECIMAL(18,2)), 
                CAST(1 AS INT)
            FROM dept_tree 
            WHERE parent_id IS NULL
            UNION ALL
            SELECT 
                curr.id, 
                CAST(curr.budget + prev.total_budget AS DECIMAL(18,2)), 
                CAST(prev.lvl + 1 AS INT)
            FROM ${tb_name1} curr
            JOIN budget_summary prev ON curr.parent_id = prev.dept_id
            WHERE prev.lvl < 10
        )
        SELECT 
            dept_id, 
            MAX(total_budget) as max_b,
            RANK() OVER(ORDER BY MAX(total_budget) DESC) as rk
        FROM budget_summary
        GROUP BY dept_id
        HAVING max_b > 5000
        ORDER BY rk
        LIMIT 100;
    """

    sql """
        WITH RECURSIVE path_finder(start_node, end_node, hops, visited_${tb_name3}) AS (
        SELECT 
            source_id, 
            target_id, 
            CAST(1 AS BIGINT), 
            CAST(source_id AS CHAR(500))
        FROM ${tb_name2} 
        WHERE source_id IN (SELECT id FROM ${tb_name3} WHERE type = 'seed')
        
        UNION ALL
        
        SELECT DISTINCT
            p.start_node,
            c.target_id,
            CAST(p.hops + 1 AS BIGINT),
            CAST(CONCAT(p.visited_${tb_name3}, ',', c.target_id) AS CHAR(500))
        FROM path_finder p
        JOIN (
            SELECT a.source_id, a.target_id 
            FROM ${tb_name2} a
            WHERE a.status = 'active'
        ) c ON p.end_node = c.source_id
        WHERE p.hops < 5 
          AND p.visited_${tb_name3} NOT LIKE CONCAT('%', CAST(c.target_id AS CHAR), '%')
    )
    SELECT 
        start_node, 
        end_node, 
        MIN(hops) as min_hops 
    FROM path_finder 
    GROUP BY start_node, end_node 
    ORDER BY min_hops ASC;"""

    sql """
        WITH RECURSIVE bom_recursive(part_id, sub_part_id, quantity, depth) AS (
        SELECT 
            part_id, 
            sub_part_id, 
            CAST(qty AS DOUBLE), 
            CAST(0 AS INT)
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY part_id ORDER BY qty DESC) as rn 
            FROM ${tb_name4}
        ) t 
        WHERE rn = 1
        
        UNION ALL
        
        SELECT 
            r.part_id, 
            b.sub_part_id, 
            CAST(r.quantity * b.qty AS DOUBLE), 
            CAST(r.depth + 1 AS INT)
        FROM bom_recursive r
        JOIN ${tb_name4} b ON r.sub_part_id = b.part_id
        WHERE r.depth < 20
    )
    SELECT 
        part_id, 
        SUM(quantity) as total_qty,
        COUNT(DISTINCT sub_part_id) as unique_subs
    FROM bom_recursive
    WHERE depth > 0
    GROUP BY part_id
    HAVING total_qty > 0
    ORDER BY total_qty DESC;"""

    sql """
        WITH RECURSIVE outer_cte(id, val, lvl) AS (
        SELECT 
            cast(id as bigint), 
            cast(val as bigint), 
            CAST(0 AS bigint) 
        FROM (WITH RECURSIVE inner_recursive(n) AS (
                SELECT CAST(1 AS INT)
                UNION ALL
                SELECT CAST(n + 1 AS INT) FROM inner_recursive WHERE n < 3
            )
            SELECT n as id, n * 10 as val FROM inner_recursive) as  t
        
        UNION ALL
        
        SELECT 
            CAST(o.id + 100 AS bigint), 
            CAST(o.val + 10 AS bigint), 
            CAST(o.lvl + 1 AS bigINT)
        FROM outer_cte o
        WHERE o.lvl < 2
    )
    SELECT * FROM outer_cte ORDER BY id;"""

    sql """
        WITH RECURSIVE multi_branch_cte(node_id, label, depth) AS (
        -- branch1：no recursive Anchor 1
        SELECT CAST(id AS INT), CAST('Type_A' AS CHAR(10)), CAST(0 AS INT)
        FROM ${tb_name3} WHERE type = 'seed'
        UNION
        -- branch 2：no recursive Anchor 2
        SELECT CAST(id AS INT), CAST('Type_B' AS CHAR(10)), CAST(0 AS INT)
        FROM ${tb_name3} WHERE type = 'normal' AND id < 2
        
        UNION ALL
        
        -- branch3：recursive part
        SELECT 
            CAST(c.target_id AS INT), 
            CAST(m.label AS CHAR(10)), 
            CAST(m.depth + 1 AS INT)
        FROM multi_branch_cte m
        JOIN ${tb_name2} c ON m.node_id = c.source_id
        WHERE m.depth < 5
        )
        SELECT 
            label, 
            depth, 
            COUNT(DISTINCT node_id) as node_count
        FROM multi_branch_cte
        GROUP BY label, depth
        HAVING node_count > 0
        ORDER BY label, depth;
        """

    sql """
        WITH RECURSIVE complex_logic(curr_id, total_score, step_path) AS (
        SELECT 
            id, 
            CAST(budget AS DOUBLE), 
            CAST(dept_name AS CHAR(200))
        FROM (
            SELECT *, ROW_NUMBER() OVER(ORDER BY budget DESC) as rank_id 
            FROM ${tb_name1}
        ) d WHERE rank_id = 1
        
        UNION ALL
        
        SELECT 
            CAST(t.id AS INT), 
            CAST(c.total_score + t.budget AS DOUBLE), 
            CAST(CONCAT(c.step_path, '->', t.dept_name) AS CHAR(200))
        FROM ${tb_name1} t
        INNER JOIN complex_logic c ON t.parent_id = c.curr_id
        WHERE c.total_score < 50000 
          AND t.dept_name NOT IN (SELECT dept_name FROM ${tb_name1} WHERE budget < 100)
        )
        SELECT 
            curr_id, 
            total_score, 
            step_path,
            DENSE_RANK() OVER(ORDER BY total_score DESC) as score_rank
        FROM complex_logic
        WHERE step_path LIKE '%研发%'
        ORDER BY score_rank
        LIMIT 10;
        """

    sql """
        WITH RECURSIVE 
        num_seq(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM num_seq WHERE n < 5
        ),
        char_path(lvl, str) AS (
            SELECT CAST(1 AS INT), CAST('A' AS CHAR(100))
            UNION ALL
            SELECT CAST(lvl + 1 AS INT), CAST(CONCAT(str, '->', 'B') AS CHAR(100)) 
            FROM char_path WHERE lvl < 5
        )
        SELECT a.n, b.str 
        FROM num_seq a 
        JOIN char_path b ON a.n = b.lvl
        ORDER BY a.n;
        """

    sql """
        WITH RECURSIVE complex_join_cte(id, p_id, info, score) AS (
            -- Anchor
            SELECT id, parent_id, CAST(dept_name AS CHAR(100)), CAST(budget AS DOUBLE)
            FROM ${tb_name1} WHERE parent_id IS NULL
            UNION ALL
            -- Recursive part：Join another table to logical judge
            SELECT 
                t.id, t.parent_id, 
                CAST(CONCAT(c.info, '/', t.dept_name) AS CHAR(100)),
                CAST(CASE WHEN t.budget > 1000 THEN c.score + t.budget ELSE c.score END AS DOUBLE)
            FROM ${tb_name1} t
            JOIN complex_join_cte c ON t.parent_id = c.id
            LEFT JOIN ${tb_name3} n ON t.id = n.id
            WHERE c.score < 100000 AND (n.type IS NULL OR n.type != 'blocked')
        )
        SELECT * FROM complex_join_cte WHERE score > 5000;
        """

    sql """
        WITH RECURSIVE sub_cte(id) AS (
            SELECT CAST(101 AS INT)
            UNION ALL
            SELECT CAST(parent_id AS INT) FROM ${tb_name1} d 
            JOIN sub_cte s ON d.id = s.id
            WHERE d.parent_id IS NOT NULL
        )
        SELECT * FROM ${tb_name1} 
        WHERE id IN (SELECT id FROM sub_cte)
           OR parent_id IN (SELECT id FROM sub_cte);
        """

    sql """
        WITH RECURSIVE grouped_cte(grp_key, total_val, depth) AS (
            SELECT 
                cast(parent_id as int), 
                cast(SUM(budget) as double), 
                CAST(0 AS INT)
            FROM ${tb_name1} 
            GROUP BY parent_id
            UNION ALL
            SELECT 
                CAST(d.id AS INT), 
                CAST(SUM(d.budget + g.total_val) AS DOUBLE), 
                CAST(g.depth + 1 AS INT)
            FROM ${tb_name1} d
            JOIN grouped_cte g ON d.parent_id = g.grp_key
            WHERE g.depth < 3
            GROUP BY d.id, g.depth
        )
        SELECT grp_key, MAX(total_val) FROM grouped_cte GROUP BY grp_key;
        """

    test {
        sql """
        WITH RECURSIVE multi_union(id, tag) AS (
            -- two Anchor branch
            SELECT CAST(1 AS INT), CAST('start1' AS CHAR(50))
            UNION
            SELECT CAST(2 AS INT), CAST('start2' AS CHAR(50))
            UNION ALL
            -- two recursive branch 
            SELECT CAST(id + 2 AS INT), CAST('step_a' AS CHAR(50)) FROM multi_union WHERE id < 5
            UNION ALL
            SELECT CAST(id + 4 AS INT), CAST('step_b' AS CHAR(50)) FROM multi_union WHERE id < 5
        )
        SELECT tag, COUNT(*) FROM multi_union GROUP BY tag;
        """
        exception """recursive reference to query multi_union must not appear within its non-recursive term"""
    }

    sql """
        WITH RECURSIVE tree_data(id, p_id, val) AS (
            SELECT id, parent_id, CAST(budget AS DOUBLE) FROM ${tb_name1} WHERE parent_id IS NULL
            UNION ALL
            SELECT d.id, d.parent_id, CAST(d.budget AS DOUBLE)
            FROM ${tb_name1} d JOIN tree_data t ON d.parent_id = t.id
        )
        SELECT 
            *,
            SUM(val) OVER(PARTITION BY p_id ORDER BY val DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
            LAG(val) OVER(ORDER BY id) as prev_val
        FROM tree_data;
        """

    sql """
        WITH RECURSIVE complex_predicate_cte(id, category) AS (
            SELECT id, CAST('ROOT' AS CHAR(20)) FROM ${tb_name1} WHERE parent_id IS NULL
            UNION ALL
            SELECT 
                d.id,
                CAST(
                    CASE 
                        WHEN d.budget > 5000 THEN 'High'
                        WHEN d.id IN (SELECT source_id FROM ${tb_name2}) THEN 'Connected'
                        ELSE 'Normal'
                    END AS CHAR(20)
                )
            FROM ${tb_name1} d
            JOIN complex_predicate_cte c ON d.parent_id = c.id
            WHERE c.category != 'End'
        )
        SELECT category, COUNT(*) FROM complex_predicate_cte GROUP BY category;
        """

    sql """
        WITH RECURSIVE
          cte_1(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_1 WHERE n < 5
          ),
          cte_2(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_2 WHERE n < 5
          ),
          cte_3(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_3 WHERE n < 5
          ),
          cte_4(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_4 WHERE n < 5
          ),
          cte_5(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_5 WHERE n < 5
          ),
          cte_6(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_6 WHERE n < 5
          ),
          cte_7(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_7 WHERE n < 5
          ),
          cte_8(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_8 WHERE n < 5
          ),
          cte_9(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_9 WHERE n < 5
          ),
          cte_10(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_10 WHERE n < 5
        )
        SELECT * 
        FROM cte_1
        JOIN cte_2 ON cte_1.n = cte_2.n
        JOIN cte_3 ON cte_1.n = cte_3.n
        JOIN cte_4 ON cte_1.n = cte_4.n
        JOIN cte_5 ON cte_1.n = cte_5.n
        JOIN cte_6 ON cte_1.n = cte_6.n
        JOIN cte_7 ON cte_1.n = cte_7.n
        JOIN cte_8 ON cte_1.n = cte_8.n
        JOIN cte_9 ON cte_1.n = cte_9.n
        JOIN cte_10 ON cte_1.n = cte_10.n;
        """


}
