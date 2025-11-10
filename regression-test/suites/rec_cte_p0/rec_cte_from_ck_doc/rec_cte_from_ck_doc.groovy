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

// https://clickhouse.com/docs/sql-reference/statements/select/with
suite ("rec_cte_from_ck_doc") {
    qt_q1 """
    WITH RECURSIVE test_table AS (
        SELECT cast(1 as int) AS number
    UNION ALL
        SELECT cast(number + 1 as int) FROM test_table WHERE number < 100
    )
    SELECT sum(number) FROM test_table;
    """

    sql "DROP TABLE IF EXISTS tree;"
    sql """
        CREATE TABLE tree
        (
            id int,
            parent_id int,
            data varchar(100)
        ) DUPLICATE KEY (id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """INSERT INTO tree VALUES (0, NULL, 'ROOT'), (1, 0, 'Child_1'), (2, 0, 'Child_2'), (3, 1, 'Child_1_1');"""

    qt_q2 """
    WITH RECURSIVE search_tree AS (
        SELECT id, parent_id, data
        FROM tree t
        WHERE t.id = 0
    UNION ALL
        SELECT t.id, t.parent_id, t.data
        FROM tree t, search_tree st
        WHERE t.parent_id = st.id
    )
    SELECT * FROM search_tree order BY id;
    """

    qt_q3 """
    WITH RECURSIVE search_tree AS (
        SELECT id, parent_id, data, array(t.id) AS path
        FROM tree t
        WHERE t.id = 0
    UNION ALL
        SELECT t.id, t.parent_id, t.data, array_concat(path, array(t.id))
        FROM tree t, search_tree st
        WHERE t.parent_id = st.id
    )
    SELECT * FROM search_tree ORDER BY path;
    """

    qt_q4 """
    WITH RECURSIVE search_tree AS (
        SELECT id, parent_id, data, array(t.id) AS path, cast(0 as int) AS depth
        FROM tree t
        WHERE t.id = 0
    UNION ALL
        SELECT t.id, t.parent_id, t.data, array_concat(path, array(t.id)), cast(depth + 1 as int)
        FROM tree t, search_tree st
        WHERE t.parent_id = st.id
    )
    SELECT * FROM search_tree ORDER BY depth, id;
    """

    sql "DROP TABLE IF EXISTS graph;"
    sql """
        CREATE TABLE graph
        (
            c_from int,
            c_to int,
            label varchar(100)
        ) DUPLICATE KEY (c_from) DISTRIBUTED BY HASH(c_from) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """INSERT INTO graph VALUES (1, 2, '1 -> 2'), (1, 3, '1 -> 3'), (2, 3, '2 -> 3'), (1, 4, '1 -> 4'), (4, 5, '4 -> 5');"""

    qt_q5 """
    WITH RECURSIVE search_graph AS (
        SELECT c_from, c_to, label FROM graph g
        UNION ALL
        SELECT g.c_from, g.c_to, g.label
        FROM graph g, search_graph sg
        WHERE g.c_from = sg.c_to
    )
    SELECT DISTINCT * FROM search_graph ORDER BY c_from, c_to;
    """

    sql "INSERT INTO graph VALUES (5, 1, '5 -> 1');"
    test {
        sql """
            WITH RECURSIVE search_graph AS (
                SELECT c_from, c_to, label FROM graph g
                UNION ALL
                SELECT g.c_from, g.c_to, g.label
                FROM graph g, search_graph sg
                WHERE g.c_from = sg.c_to
            )
            SELECT DISTINCT * FROM search_graph ORDER BY c_from, c_to;
        """
        exception "ABORTED"
    }

    // test global rf
    sql "set enable_runtime_filter_prune = false;"
    test {
        sql """
        WITH RECURSIVE search_graph AS (
            SELECT c_from, c_to, label FROM graph g
            UNION ALL
            SELECT g.c_from, g.c_to, g.label
            FROM graph g join [shuffle] search_graph sg
            on g.c_from = sg.c_to
        )
        SELECT DISTINCT * FROM search_graph ORDER BY c_from, c_to;
        """
        exception "ABORTED"
    }

    // do not support use limit to stop recursion now
    //qt_q6 """
    //WITH RECURSIVE test_table AS (
    //    SELECT cast(1 as int) AS number
    //UNION ALL
    //    SELECT cast(number + 1 as int) FROM test_table
    //)
    //SELECT sum(number) FROM test_table LIMIT 100;
    //"""
}
