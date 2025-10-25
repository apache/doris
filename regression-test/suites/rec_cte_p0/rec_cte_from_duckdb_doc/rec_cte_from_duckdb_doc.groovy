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

import org.codehaus.groovy.runtime.IOGroovyMethods

// https://duckdb.org/docs/stable/sql/query_syntax/with#recursive-ctes
suite ("rec_cte_from_duckdb_doc") {
    qt_q1 """
    WITH RECURSIVE FibonacciNumbers (
        RecursionDepth,
        FibonacciNumber,
        NextNumber
    ) AS (
        -- Base case
        SELECT
            cast(0 as int) AS RecursionDepth,
            cast(0 as int) AS FibonacciNumber,
            cast(1 as int) AS NextNumber
        UNION
        ALL -- Recursive step
        SELECT
            cast((fib.RecursionDepth + 1) as int) AS RecursionDepth,
            fib.NextNumber AS FibonacciNumber,
            cast((fib.FibonacciNumber + fib.NextNumber) as int) AS NextNumber
        FROM
            FibonacciNumbers fib
        WHERE
            cast((fib.RecursionDepth + 1) as int) < 10
    )
    SELECT
        *
    FROM
        FibonacciNumbers fn ORDER BY fn.RecursionDepth;
    """

    sql "DROP TABLE IF EXISTS tag;"
    sql """
        CREATE TABLE tag
        (
            id int,
            name varchar(100),
            subclassof int
        ) DUPLICATE KEY (id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """INSERT INTO tag VALUES
    (1, 'U2',     5),
    (2, 'Blur',   5),
    (3, 'Oasis',  5),
    (4, '2Pac',   6),
    (5, 'Rock',   7),
    (6, 'Rap',    7),
    (7, 'Music',  9),
    (8, 'Movies', 9),
    (9, 'Art', NULL);"""

    qt_q2 """
    WITH RECURSIVE tag_hierarchy(id, source, path) AS (
            SELECT id, name, array(name) AS path
            FROM tag
            WHERE subclassof IS NULL
        UNION ALL
            SELECT tag.id, tag.name, array_concat(array(tag.name), tag_hierarchy.path)
            FROM tag, tag_hierarchy
            WHERE tag.subclassof = tag_hierarchy.id
        )
    SELECT path
    FROM tag_hierarchy
    WHERE source = 'Oasis';
    """

    sql "DROP TABLE IF EXISTS edge;"
    sql """
    CREATE TABLE edge
    (
        node1id int,
        node2id int
    ) DUPLICATE KEY (node1id)
    DISTRIBUTED BY HASH(node1id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """
    INSERT INTO edge VALUES
    (1, 3), (1, 5), (2, 4), (2, 5), (2, 10), (3, 1),
    (3, 5), (3, 8), (3, 10), (5, 3), (5, 4), (5, 8),
    (6, 3), (6, 4), (7, 4), (8, 1), (9, 4);
    """

    qt_q3 """
    WITH RECURSIVE paths(startNode, endNode, path) AS (
            SELECT -- Define the path as the first edge of the traversal
                node1id AS startNode,
                node2id AS endNode,
                array_concat(array(node1id), array(node2id)) AS path
            FROM edge
            WHERE node1id = 1
            UNION ALL
            SELECT -- Concatenate new edge to the path
                paths.startNode AS startNode,
                node2id AS endNode,
                array_concat(path, array(node2id)) AS path
            FROM paths
            JOIN edge ON paths.endNode = node1id
            -- Prevent adding a repeated node to the path.
            -- This ensures that no cycles occur.
            WHERE array_contains(paths.path, node2id) = false
        )
    SELECT startNode, endNode, path
    FROM paths
    ORDER BY array_size(path), path;
    """

    // do not support subquery containing recursive cte
    //qt_q4 """
    //WITH RECURSIVE paths(startNode, endNode, path) AS (
    //        SELECT -- Define the path as the first edge of the traversal
    //            node1id AS startNode,
    //            node2id AS endNode,
    //            array_concat(array(node1id), array(node2id)) AS path
    //        FROM edge
    //        WHERE startNode = 1
    //        UNION ALL
    //        SELECT -- Concatenate new edge to the path
    //            paths.startNode AS startNode,
    //            node2id AS endNode,
    //            array_concat(path, array(node2id)) AS path
    //        FROM paths
    //        JOIN edge ON paths.endNode = node1id
    //        -- Prevent adding a node that was visited previously by any path.
    //        -- This ensures that (1) no cycles occur and (2) only nodes that
    //        -- were not visited by previous (shorter) paths are added to a path.
    //        WHERE NOT EXISTS (
    //            SELECT 1 FROM paths previous_paths
    //                WHERE array_contains(previous_paths.path, node2id)
    //            )
    //    )
    //SELECT startNode, endNode, path
    //FROM paths
    //ORDER BY array_size(path), path;
    //"""

    //qt_q5 """
    //WITH RECURSIVE paths(startNode, endNode, path, endReached) AS (
    //SELECT -- Define the path as the first edge of the traversal
    //        node1id AS startNode,
    //        node2id AS endNode,
    //        array_concat(array(node1id), array(node2id)) AS path,
    //        (node2id = 8) AS endReached
    //    FROM edge
    //    WHERE startNode = 1
    //UNION ALL
    //SELECT -- Concatenate new edge to the path
    //        paths.startNode AS startNode,
    //        node2id AS endNode,
    //        array_concat(path, array(node2id)) AS path,
    //        max(CASE WHEN node2id = 8 THEN 1 ELSE 0 END)
    //            OVER (ROWS BETWEEN UNBOUNDED PRECEDING
    //                        AND UNBOUNDED FOLLOWING) AS endReached
    //    FROM paths
    //    JOIN edge ON paths.endNode = node1id
    //    WHERE NOT EXISTS (
    //            FROM paths previous_paths
    //            WHERE array_contains(previous_paths.path, node2id)
    //        )
    //    AND paths.endReached = 0
    //)
    //SELECT startNode, endNode, path
    //FROM paths
    //WHERE endNode = 8
    //ORDER BY array_size(path), path;
    //"""
}
