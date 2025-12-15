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

suite("complex_rec_cte_test") {

    sql """
        CREATE TABLE IF NOT EXISTS departments (
            id INT,
            parent_id INT,
            dept_name VARCHAR(50),
            budget DECIMAL(18, 2)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");"""

    sql """
        CREATE TABLE IF NOT EXISTS connections (
            source_id INT,
            target_id INT,
            status VARCHAR(20)
        )
        DUPLICATE KEY(source_id)
        DISTRIBUTED BY HASH(source_id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");"""

    sql """
        CREATE TABLE IF NOT EXISTS nodes (
            id INT,
            type VARCHAR(20)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """
        CREATE TABLE IF NOT EXISTS raw_bom (
            part_id INT,
            sub_part_id INT,
            qty DOUBLE
        )
        DUPLICATE KEY(part_id)
        DISTRIBUTED BY HASH(part_id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");"""

    sql """
        INSERT INTO departments VALUES 
        (1, NULL, '总部', 10000.00),
        (10, 1, '研发部', 5000.00),
        (11, 1, '市场部', 4000.00),
        (101, 10, '后端开发', 2000.00),
        (102, 10, '前端开发', 1500.00),
        (111, 11, '线上推广', 2000.00);"""

    sql """
        INSERT INTO nodes VALUES (1, 'seed'), (2, 'normal'), (3, 'normal'), (4, 'normal');
        INSERT INTO connections VALUES 
        (1, 2, 'active'), (2, 3, 'active'), (3, 4, 'active'), 
        (4, 1, 'active'), (2, 4, 'active'), (1, 3, 'inactive');"""

    sql """
        INSERT INTO raw_bom VALUES 
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
            FROM departments 
            WHERE parent_id IS NULL
            UNION ALL
            SELECT 
                t.id, 
                t.parent_id, 
                t.dept_name, 
                CAST(CONCAT(c.path, '->', t.dept_name) AS CHAR(200)),
                t.budget
            FROM departments t 
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
            FROM departments curr
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
        WITH RECURSIVE path_finder(start_node, end_node, hops, visited_nodes) AS (
        SELECT 
            source_id, 
            target_id, 
            CAST(1 AS BIGINT), 
            CAST(source_id AS CHAR(500))
        FROM connections 
        WHERE source_id IN (SELECT id FROM nodes WHERE type = 'seed')
        
        UNION ALL
        
        SELECT DISTINCT
            p.start_node,
            c.target_id,
            CAST(p.hops + 1 AS BIGINT),
            CAST(CONCAT(p.visited_nodes, ',', c.target_id) AS CHAR(500))
        FROM path_finder p
        JOIN (
            SELECT a.source_id, a.target_id 
            FROM connections a
            WHERE a.status = 'active'
        ) c ON p.end_node = c.source_id
        WHERE p.hops < 5 
          AND p.visited_nodes NOT LIKE CONCAT('%', CAST(c.target_id AS CHAR), '%')
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
            FROM raw_bom
        ) t 
        WHERE rn = 1
        
        UNION ALL
        
        SELECT 
            r.part_id, 
            b.sub_part_id, 
            CAST(r.quantity * b.qty AS DOUBLE), 
            CAST(r.depth + 1 AS INT)
        FROM bom_recursive r
        JOIN raw_bom b ON r.sub_part_id = b.part_id
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
        FROM nodes WHERE type = 'seed'
        UNION
        -- branch 2：no recursive Anchor 2
        SELECT CAST(id AS INT), CAST('Type_B' AS CHAR(10)), CAST(0 AS INT)
        FROM nodes WHERE type = 'normal' AND id < 2
        
        UNION ALL
        
        -- branch3：recursive part
        SELECT 
            CAST(c.target_id AS INT), 
            CAST(m.label AS CHAR(10)), 
            CAST(m.depth + 1 AS INT)
        FROM multi_branch_cte m
        JOIN connections c ON m.node_id = c.source_id
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
            FROM departments
        ) d WHERE rank_id = 1
        
        UNION ALL
        
        SELECT 
            CAST(t.id AS INT), 
            CAST(c.total_score + t.budget AS DOUBLE), 
            CAST(CONCAT(c.step_path, '->', t.dept_name) AS CHAR(200))
        FROM departments t
        INNER JOIN complex_logic c ON t.parent_id = c.curr_id
        WHERE c.total_score < 50000 
          AND t.dept_name NOT IN (SELECT dept_name FROM departments WHERE budget < 100)
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
            FROM departments WHERE parent_id IS NULL
            UNION ALL
            -- Recursive part：Join another table to logical judge
            SELECT 
                t.id, t.parent_id, 
                CAST(CONCAT(c.info, '/', t.dept_name) AS CHAR(100)),
                CAST(CASE WHEN t.budget > 1000 THEN c.score + t.budget ELSE c.score END AS DOUBLE)
            FROM departments t
            JOIN complex_join_cte c ON t.parent_id = c.id
            LEFT JOIN nodes n ON t.id = n.id
            WHERE c.score < 100000 AND (n.type IS NULL OR n.type != 'blocked')
        )
        SELECT * FROM complex_join_cte WHERE score > 5000;
        """

    sql """
        WITH RECURSIVE sub_cte(id) AS (
            SELECT CAST(101 AS INT)
            UNION ALL
            SELECT CAST(parent_id AS INT) FROM departments d 
            JOIN sub_cte s ON d.id = s.id
            WHERE d.parent_id IS NOT NULL
        )
        SELECT * FROM departments 
        WHERE id IN (SELECT id FROM sub_cte)
           OR parent_id IN (SELECT id FROM sub_cte);
        """

    sql """
        WITH RECURSIVE grouped_cte(grp_key, total_val, depth) AS (
            SELECT 
                cast(parent_id as int), 
                cast(SUM(budget) as double), 
                CAST(0 AS INT)
            FROM departments 
            GROUP BY parent_id
            UNION ALL
            SELECT 
                CAST(d.id AS INT), 
                CAST(SUM(d.budget + g.total_val) AS DOUBLE), 
                CAST(g.depth + 1 AS INT)
            FROM departments d
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
            SELECT id, parent_id, CAST(budget AS DOUBLE) FROM departments WHERE parent_id IS NULL
            UNION ALL
            SELECT d.id, d.parent_id, CAST(d.budget AS DOUBLE)
            FROM departments d JOIN tree_data t ON d.parent_id = t.id
        )
        SELECT 
            *,
            SUM(val) OVER(PARTITION BY p_id ORDER BY val DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
            LAG(val) OVER(ORDER BY id) as prev_val
        FROM tree_data;
        """

    sql """
        WITH RECURSIVE complex_predicate_cte(id, category) AS (
            SELECT id, CAST('ROOT' AS CHAR(20)) FROM departments WHERE parent_id IS NULL
            UNION ALL
            SELECT 
                d.id,
                CAST(
                    CASE 
                        WHEN d.budget > 5000 THEN 'High'
                        WHEN d.id IN (SELECT source_id FROM connections) THEN 'Connected'
                        ELSE 'Normal'
                    END AS CHAR(20)
                )
            FROM departments d
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
          ),
          cte_11(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_11 WHERE n < 5
          ),
          cte_12(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_12 WHERE n < 5
          ),
          cte_13(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_13 WHERE n < 5
          ),
          cte_14(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_14 WHERE n < 5
          ),
          cte_15(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_15 WHERE n < 5
          ),
          cte_16(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_16 WHERE n < 5
          ),
          cte_17(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_17 WHERE n < 5
          ),
          cte_18(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_18 WHERE n < 5
          ),
          cte_19(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_19 WHERE n < 5
          ),
          cte_20(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_20 WHERE n < 5
          ),
          cte_21(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_21 WHERE n < 5
          ),
          cte_22(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_22 WHERE n < 5
          ),
          cte_23(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_23 WHERE n < 5
          ),
          cte_24(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_24 WHERE n < 5
          ),
          cte_25(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_25 WHERE n < 5
          ),
          cte_26(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_26 WHERE n < 5
          ),
          cte_27(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_27 WHERE n < 5
          ),
          cte_28(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_28 WHERE n < 5
          ),
          cte_29(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_29 WHERE n < 5
          ),
          cte_30(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_30 WHERE n < 5
          ),
          cte_31(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_31 WHERE n < 5
          ),
          cte_32(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_32 WHERE n < 5
          ),
          cte_33(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_33 WHERE n < 5
          ),
          cte_34(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_34 WHERE n < 5
          ),
          cte_35(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_35 WHERE n < 5
          ),
          cte_36(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_36 WHERE n < 5
          ),
          cte_37(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_37 WHERE n < 5
          ),
          cte_38(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_38 WHERE n < 5
          ),
          cte_39(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_39 WHERE n < 5
          ),
          cte_40(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_40 WHERE n < 5
          ),
          cte_41(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_41 WHERE n < 5
          ),
          cte_42(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_42 WHERE n < 5
          ),
          cte_43(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_43 WHERE n < 5
          ),
          cte_44(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_44 WHERE n < 5
          ),
          cte_45(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_45 WHERE n < 5
          ),
          cte_46(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_46 WHERE n < 5
          ),
          cte_47(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_47 WHERE n < 5
          ),
          cte_48(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_48 WHERE n < 5
          ),
          cte_49(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_49 WHERE n < 5
          ),
          cte_50(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_50 WHERE n < 5
          ),
          cte_51(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_51 WHERE n < 5
          ),
          cte_52(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_52 WHERE n < 5
          ),
          cte_53(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_53 WHERE n < 5
          ),
          cte_54(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_54 WHERE n < 5
          ),
          cte_55(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_55 WHERE n < 5
          ),
          cte_56(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_56 WHERE n < 5
          ),
          cte_57(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_57 WHERE n < 5
          ),
          cte_58(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_58 WHERE n < 5
          ),
          cte_59(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_59 WHERE n < 5
          ),
          cte_60(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_60 WHERE n < 5
          ),
          cte_61(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_61 WHERE n < 5
          ),
          cte_62(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_62 WHERE n < 5
          ),
          cte_63(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_63 WHERE n < 5
          ),
          cte_64(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_64 WHERE n < 5
          ),
          cte_65(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_65 WHERE n < 5
          ),
          cte_66(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_66 WHERE n < 5
          ),
          cte_67(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_67 WHERE n < 5
          ),
          cte_68(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_68 WHERE n < 5
          ),
          cte_69(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_69 WHERE n < 5
          ),
          cte_70(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_70 WHERE n < 5
          ),
          cte_71(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_71 WHERE n < 5
          ),
          cte_72(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_72 WHERE n < 5
          ),
          cte_73(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_73 WHERE n < 5
          ),
          cte_74(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_74 WHERE n < 5
          ),
          cte_75(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_75 WHERE n < 5
          ),
          cte_76(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_76 WHERE n < 5
          ),
          cte_77(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_77 WHERE n < 5
          ),
          cte_78(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_78 WHERE n < 5
          ),
          cte_79(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_79 WHERE n < 5
          ),
          cte_80(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_80 WHERE n < 5
          ),
          cte_81(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_81 WHERE n < 5
          ),
          cte_82(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_82 WHERE n < 5
          ),
          cte_83(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_83 WHERE n < 5
          ),
          cte_84(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_84 WHERE n < 5
          ),
          cte_85(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_85 WHERE n < 5
          ),
          cte_86(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_86 WHERE n < 5
          ),
          cte_87(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_87 WHERE n < 5
          ),
          cte_88(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_88 WHERE n < 5
          ),
          cte_89(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_89 WHERE n < 5
          ),
          cte_90(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_90 WHERE n < 5
          ),
          cte_91(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_91 WHERE n < 5
          ),
          cte_92(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_92 WHERE n < 5
          ),
          cte_93(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_93 WHERE n < 5
          ),
          cte_94(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_94 WHERE n < 5
          ),
          cte_95(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_95 WHERE n < 5
          ),
          cte_96(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_96 WHERE n < 5
          ),
          cte_97(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_97 WHERE n < 5
          ),
          cte_98(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_98 WHERE n < 5
          ),
          cte_99(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_99 WHERE n < 5
          ),
          cte_100(n) AS (
            SELECT CAST(1 AS INT)
            UNION ALL
            SELECT CAST(n + 1 AS INT) FROM cte_100 WHERE n < 5
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
        JOIN cte_10 ON cte_1.n = cte_10.n
        JOIN cte_11 ON cte_1.n = cte_11.n
        JOIN cte_12 ON cte_1.n = cte_12.n
        JOIN cte_13 ON cte_1.n = cte_13.n
        JOIN cte_14 ON cte_1.n = cte_14.n
        JOIN cte_15 ON cte_1.n = cte_15.n
        JOIN cte_16 ON cte_1.n = cte_16.n
        JOIN cte_17 ON cte_1.n = cte_17.n
        JOIN cte_18 ON cte_1.n = cte_18.n
        JOIN cte_19 ON cte_1.n = cte_19.n
        JOIN cte_20 ON cte_1.n = cte_20.n
        JOIN cte_21 ON cte_1.n = cte_21.n
        JOIN cte_22 ON cte_1.n = cte_22.n
        JOIN cte_23 ON cte_1.n = cte_23.n
        JOIN cte_24 ON cte_1.n = cte_24.n
        JOIN cte_25 ON cte_1.n = cte_25.n
        JOIN cte_26 ON cte_1.n = cte_26.n
        JOIN cte_27 ON cte_1.n = cte_27.n
        JOIN cte_28 ON cte_1.n = cte_28.n
        JOIN cte_29 ON cte_1.n = cte_29.n
        JOIN cte_30 ON cte_1.n = cte_30.n
        JOIN cte_31 ON cte_1.n = cte_31.n
        JOIN cte_32 ON cte_1.n = cte_32.n
        JOIN cte_33 ON cte_1.n = cte_33.n
        JOIN cte_34 ON cte_1.n = cte_34.n
        JOIN cte_35 ON cte_1.n = cte_35.n
        JOIN cte_36 ON cte_1.n = cte_36.n
        JOIN cte_37 ON cte_1.n = cte_37.n
        JOIN cte_38 ON cte_1.n = cte_38.n
        JOIN cte_39 ON cte_1.n = cte_39.n
        JOIN cte_40 ON cte_1.n = cte_40.n
        JOIN cte_41 ON cte_1.n = cte_41.n
        JOIN cte_42 ON cte_1.n = cte_42.n
        JOIN cte_43 ON cte_1.n = cte_43.n
        JOIN cte_44 ON cte_1.n = cte_44.n
        JOIN cte_45 ON cte_1.n = cte_45.n
        JOIN cte_46 ON cte_1.n = cte_46.n
        JOIN cte_47 ON cte_1.n = cte_47.n
        JOIN cte_48 ON cte_1.n = cte_48.n
        JOIN cte_49 ON cte_1.n = cte_49.n
        JOIN cte_50 ON cte_1.n = cte_50.n
        JOIN cte_51 ON cte_1.n = cte_51.n
        JOIN cte_52 ON cte_1.n = cte_52.n
        JOIN cte_53 ON cte_1.n = cte_53.n
        JOIN cte_54 ON cte_1.n = cte_54.n
        JOIN cte_55 ON cte_1.n = cte_55.n
        JOIN cte_56 ON cte_1.n = cte_56.n
        JOIN cte_57 ON cte_1.n = cte_57.n
        JOIN cte_58 ON cte_1.n = cte_58.n
        JOIN cte_59 ON cte_1.n = cte_59.n
        JOIN cte_60 ON cte_1.n = cte_60.n
        JOIN cte_61 ON cte_1.n = cte_61.n
        JOIN cte_62 ON cte_1.n = cte_62.n
        JOIN cte_63 ON cte_1.n = cte_63.n
        JOIN cte_64 ON cte_1.n = cte_64.n
        JOIN cte_65 ON cte_1.n = cte_65.n
        JOIN cte_66 ON cte_1.n = cte_66.n
        JOIN cte_67 ON cte_1.n = cte_67.n
        JOIN cte_68 ON cte_1.n = cte_68.n
        JOIN cte_69 ON cte_1.n = cte_69.n
        JOIN cte_70 ON cte_1.n = cte_70.n
        JOIN cte_71 ON cte_1.n = cte_71.n
        JOIN cte_72 ON cte_1.n = cte_72.n
        JOIN cte_73 ON cte_1.n = cte_73.n
        JOIN cte_74 ON cte_1.n = cte_74.n
        JOIN cte_75 ON cte_1.n = cte_75.n
        JOIN cte_76 ON cte_1.n = cte_76.n
        JOIN cte_77 ON cte_1.n = cte_77.n
        JOIN cte_78 ON cte_1.n = cte_78.n
        JOIN cte_79 ON cte_1.n = cte_79.n
        JOIN cte_80 ON cte_1.n = cte_80.n
        JOIN cte_81 ON cte_1.n = cte_81.n
        JOIN cte_82 ON cte_1.n = cte_82.n
        JOIN cte_83 ON cte_1.n = cte_83.n
        JOIN cte_84 ON cte_1.n = cte_84.n
        JOIN cte_85 ON cte_1.n = cte_85.n
        JOIN cte_86 ON cte_1.n = cte_86.n
        JOIN cte_87 ON cte_1.n = cte_87.n
        JOIN cte_88 ON cte_1.n = cte_88.n
        JOIN cte_89 ON cte_1.n = cte_89.n
        JOIN cte_90 ON cte_1.n = cte_90.n
        JOIN cte_91 ON cte_1.n = cte_91.n
        JOIN cte_92 ON cte_1.n = cte_92.n
        JOIN cte_93 ON cte_1.n = cte_93.n
        JOIN cte_94 ON cte_1.n = cte_94.n
        JOIN cte_95 ON cte_1.n = cte_95.n
        JOIN cte_96 ON cte_1.n = cte_96.n
        JOIN cte_97 ON cte_1.n = cte_97.n
        JOIN cte_98 ON cte_1.n = cte_98.n
        JOIN cte_99 ON cte_1.n = cte_99.n
        JOIN cte_100 ON cte_1.n = cte_100.n;
        """


}
