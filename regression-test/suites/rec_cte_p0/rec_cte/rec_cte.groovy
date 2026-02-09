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

suite ("rec_cte") {
    qt_sql """
    WITH RECURSIVE test_table AS (
    SELECT
        cast(1.0 as double) AS number
    UNION
    SELECT
        cos(number)
    FROM
        test_table
    )
    SELECT
        number
    FROM
        test_table order by number;
    """

    qt_sql """
    WITH RECURSIVE test_table AS (
        SELECT cast(10 as int) AS number
    UNION ALL
        SELECT cast(number - 1 as int) FROM test_table WHERE number > 0
    )
    SELECT sum(number) FROM test_table;
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

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            node1id AS k1,
            node2id AS k2
        FROM edge
        UNION
        SELECT
            k1,
            cast(sum(k2) as int)
        FROM t1 GROUP BY k1
    )
    SELECT * FROM t1 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            node1id AS k1,
            node2id AS k2
        FROM edge
        UNION
        SELECT
            cast(sum(k1) as int),
            k2
        FROM t1 GROUP BY k2
    )
    SELECT * FROM t1 ORDER BY 1,2;
    """

    test {
        sql """
            WITH RECURSIVE t1(k1, k2) AS (
                SELECT
                    node1id AS k1,
                    node2id AS k2
                FROM edge
                UNION
                SELECT
                    cast(sum(k1 + 1) as int),
                    k2
                FROM t1 GROUP BY k2
            )
            SELECT * FROM t1 ORDER BY 1,2;
        """
        exception "ABORTED"
    }  

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            node1id AS k1,
            node2id AS k2
        FROM edge
        UNION
        SELECT
            cast(sum(k1 + 1) as int),
            k2
        FROM t1 WHERE k1 < 100 GROUP BY k2
    )
    SELECT * FROM t1 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            node1id AS k1,
            node2id AS k2
        FROM edge
        UNION
        SELECT
            k1,
            cast(sum(k2) OVER (PARTITION BY k1 ORDER BY k1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as int)
        FROM t1
    )
    SELECT * FROM t1 ORDER BY 1,2;
    """

    test {
        sql """
            WITH RECURSIVE t1(k1, k2) AS (
                SELECT
                    1,2
                UNION ALL
                SELECT
                    1,2
                FROM t1 GROUP BY k1
            )
            SELECT * FROM t1 ORDER BY 1,2;
        """
        exception "ABORTED"
    }  

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,2
        UNION
        SELECT
            1,2
        FROM t1 GROUP BY k1
    )
    SELECT * FROM t1 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,2
        UNION
        SELECT
            3,4
        FROM t1 GROUP BY k1
    )
    SELECT * FROM t1 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,2
        UNION
        SELECT
            3,4
        FROM t1 GROUP BY k1
    ),
    t2(k1, k2) AS (
        SELECT
            11,22
        UNION
        SELECT
            33,44
        FROM t2 GROUP BY k1
    )
    SELECT * FROM t1 UNION  select * from t2 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,2
        UNION
        SELECT
            3,4
        FROM t1 GROUP BY k1
    ),
    t2(k1, k2) AS (
        SELECT
            11,22
        UNION
        SELECT t2.k1, t2.k2 FROM t1,t2
    )
    SELECT * FROM t1 UNION  select * from t2 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,2
        UNION
        SELECT
            3,4
        FROM t1 GROUP BY k1
    ),
    t2(k1, k2) AS (
        SELECT
            11,22
        UNION
        SELECT t1.k1, t2.k2 FROM t1,t2
    )
    select * from t2 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,2
        UNION
        SELECT
            3,4
        FROM t1 GROUP BY k1
    ),
    t2(k1, k2) AS (
        SELECT
            2,3
        UNION
        SELECT least(t1.k1,t2.k1), least(t1.k2,t2.k2) FROM t1,t2
    )
    select * from t2 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,2
        UNION
        SELECT
            3,4
        FROM t1 GROUP BY k1
    ),
    t2(k1, k2) AS (
        SELECT
            11,22
        UNION
        SELECT t1.k1, t1.k2 FROM t1
    )
    SELECT * FROM t1 UNION  select * from t2 ORDER BY 1,2;
    """

    qt_sql """
    WITH RECURSIVE t1(k1, k2) AS (
        SELECT
            1,
            2
        UNION
        SELECT
            3,
            4
        FROM
            t1
        GROUP BY
            k1
    ),
    t2(k1, k2) AS (
        SELECT
            11,
            22
        UNION
        SELECT
            t2.k2,
            tx.k1
        FROM
            t1,
            t2,
            (
                WITH RECURSIVE t1(k1, k2) AS (
                    SELECT
                        1,
                        2
                    UNION
                    SELECT
                        3,
                        4
                    FROM
                        t1
                    GROUP BY
                        k1
                ),
                t2(k1, k2) AS (
                    SELECT
                        11,
                        22
                    UNION
                    SELECT
                        t2.k1,
                        t2.k2
                    FROM
                        t1,
                        t2
                )
                SELECT
                    *
                FROM
                    t1
                UNION
                select
                    *
                from
                    t2
                ORDER BY
                    1,
                    2
            ) tx
    )
    SELECT
        *
    FROM
        t1
    UNION
    select
        *
    from
        t2
    ORDER BY
        1,
        2;
    """
}
