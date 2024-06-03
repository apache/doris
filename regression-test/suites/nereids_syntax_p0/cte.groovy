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

suite("cte") {
    sql "SET enable_nereids_planner=true"

    sql "DROP VIEW IF EXISTS cte_v1"
    sql "DROP VIEW IF EXISTS cte_v2"

    sql """
        CREATE VIEW cte_v1 AS
        SELECT *
        FROM supplier
    """

    sql """
        CREATE VIEW cte_v2 AS
        SELECT *
        FROM supplier
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql """
        SET enable_pipeline_engine = true
    """

    order_qt_cte_1 """
        WITH cte1 AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 30
        )
        SELECT *
        FROM cte1 as t1, cte1 as t2
    """

    // test multiple CTEs
    order_qt_cte_2 """
        WITH cte1 AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 20
        ), cte2 AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 30
        ), cte3 AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 10
        )
        SELECT *
        FROM cte1, cte2
    """

    // test ordered reference between different tables
    order_qt_cte_3 """
        WITH cte1 AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 30
        ), cte2 AS (
            SELECT s_suppkey
            FROM cte1
            WHERE s_suppkey < 20
        ), cte3 AS (
            SELECT s_suppkey
            FROM cte2
            WHERE s_suppkey < 10
        )
        SELECT *
        FROM cte2, cte3
    """

    // if the CTE name is the same as an existing table name, the CTE's name will be chosen and used first
    order_qt_cte_4 """
        WITH part AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 30
        ), customer AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 20
        )
        SELECT *
        FROM part, customer
    """

    // the processing logic is similiar to cte_4 when the CTE name is the same as an existing view name
    order_qt_cte_5 """
        WITH v1 AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 30
        ), v2 AS (
            SELECT s_suppkey
            FROM supplier
            WHERE s_suppkey < 10
        )
        SELECT *
        FROM v1, v2
    """

    // test column aliases in CTE
    order_qt_cte_6 """
        WITH cte1 (skey, sname) AS (
            SELECT s_suppkey as sk, s_name
            FROM supplier
            WHERE s_suppkey < 30
        ), cte2 (skey2, sname2) AS (
            SELECT s_suppkey, s_name
            FROM supplier
            WHERE s_suppkey < 20
        )
        SELECT *
        FROM cte1, cte2
    """

    // if the size of column aliases is smaller than WithClause's outputSlots, we will replace the corresponding number of front slots with column aliases.
    order_qt_cte_7 """
        WITH cte1 (skey, sname) AS (
            SELECT *
            FROM supplier
            WHERE s_suppkey < 30
        ), cte2 (skey2) AS (
            SELECT s_suppkey, s_name
            FROM supplier
            WHERE s_suppkey < 20
        )
        SELECT *
        FROM cte1, cte2
    """

    // using CTE in From Clause
    order_qt_cte_8 """
        WITH cte1 (skey, sname) AS (
            SELECT *
            FROM supplier
            WHERE s_suppkey < 30
        ), cte2 (skey2) AS (
            SELECT s_suppkey, s_name
            FROM supplier
            WHERE s_suppkey < 20
        )
        SELECT *
        FROM (
            SELECT *
            FROM cte2
        ) t1
    """

    // using CTE in subqueries
    order_qt_cte_9 """
        WITH cte1 (skey, sname) AS (
            SELECT *
            FROM supplier
            WHERE s_suppkey < 30
        ), cte2 (skey2) AS (
            SELECT s_suppkey, s_name
            FROM supplier
            WHERE s_suppkey < 20
        )
        SELECT *
        FROM supplier
        WHERE s_suppkey in (
            SELECT skey2
            FROM cte2
        )
    """

    // using CTE in having clause
    order_qt_cte_10 """
        WITH cte1 (skey, sname) AS (
            SELECT *
            FROM supplier
        ), cte2 (region) AS (
            SELECT s_region
            FROM supplier
            WHERE s_region in ("ASIA", "AMERICA")
        )
        SELECT s_region, count(*) cnt
        FROM cte1
        GROUP BY s_region
        HAVING s_region in (
            SELECT region
            FROM cte2
        )
    """

    // reference CTE which contains aliases in WithClause repeatedly
    order_qt_cte_11 """
        WITH cte1 AS (
            SELECT s_suppkey as sk, s_name as sn
            FROM supplier
        ), cte2 AS (
            SELECT sk, sn
            FROM cte1
            WHERE sk < 20
        )
        SELECT *
        FROM cte1 JOIN cte2
        ON cte1.sk = cte2.sk
    """

    order_qt_cte12 """
            WITH t1 AS (
                WITH t AS (
                    SELECT 
                        s_suppkey
                    FROM 
                        supplier
                ) 
                    SELECT 
                        s_suppkey
                    FROM
                    t
            )
            SELECT 
              *
            FROM t1;
     
    """

    qt_cte13 """
            SELECT abs(dd.s_suppkey)
            FROM (
            WITH part AS 
                (SELECT s_suppkey
                FROM supplier
                WHERE s_suppkey < 30 )
                    SELECT p1.s_suppkey
                    FROM part p1
                    JOIN part p2
                        ON p1.s_suppkey = p2.s_suppkey
                    WHERE p1.s_suppkey > 0 ) dd
                WHERE dd.s_suppkey > 0
                ORDER BY dd.s_suppkey;
    """

    sql "set enable_pipeline_engine=true"

    qt_cte14 """
            SELECT abs(dd.s_suppkey)
            FROM (
            WITH part AS 
                (SELECT s_suppkey
                FROM supplier
                WHERE s_suppkey < 30 )
                    SELECT p1.s_suppkey
                    FROM part p1
                    JOIN part p2
                        ON p1.s_suppkey = p2.s_suppkey
                    WHERE p1.s_suppkey > 0 ) dd
                WHERE dd.s_suppkey > 0
                ORDER BY dd.s_suppkey;
    """

    test {
        sql = "WITH cte1 (a1, A1) AS (SELECT * FROM supplier) SELECT * FROM cte1"

        exception = "Duplicated CTE column alias"
    }

    test {
        sql = "WITH cte1 (a1, a2) AS (SELECT s_suppkey FROM supplier) SELECT * FROM cte1"

        exception = "CTE [cte1] returns 2 columns, but 1 labels were specified"
    }

    test {
        sql = "WITH cte1 AS (SELECT * FROM cte2), cte2 AS (SELECT * FROM supplier) SELECT * FROM cte1, cte2"

        exception = "[cte2] does not exist in database"
    }

    test {
        sql = "WITH cte1 AS (SELECT * FROM not_existed_table) SELECT * FROM supplier"

        exception = "[not_existed_table] does not exist in database"
    }

    test {
        sql = "WITH cte1 AS (SELECT * FROM supplier), cte1 AS (SELECT * FROM part) SELECT * FROM cte1"

        exception = "[cte1] cannot be used more than once"
    }

    explain {
        sql("WITH cte_0 AS ( SELECT 1 AS a ) SELECT * from cte_0 t1 join cte_0 t2 on true WHERE false;")
        notContains "MultiCastDataSinks"
    }

    sql "WITH cte_0 AS ( SELECT 1 AS a ) SELECT * from cte_0 t1 LIMIT 10 UNION SELECT * from cte_0 t1 LIMIT 10"

    qt_cte_with_repeat """
        with cte_0 as (select lo_orderkey, lo_linenumber, grouping_id(lo_orderkey) as id from lineorder group by cube(lo_orderkey, lo_linenumber))
        select * from cte_0 order by lo_orderkey, lo_linenumber, id
    """

    qt_test """
        SELECT * FROM (
        WITH temptable as (
        SELECT 1 Id, '2023-08-25 00:00:00' UpdateDateTime, 10 Value
        UNION
        SELECT 1 Id, '2023-08-25 01:00:00' UpdateDateTime, 20 Value
        )
        SELECT temptable.Id, temptable.UpdateDateTime, temptable.Value, rolling.RollingValue FROM temptable
        LEFT JOIN (
        SELECT Id, UpdateDateTime, SUM(Value) OVER (PARTITION BY CAST(UpdateDateTime AS DATE) ORDER BY UpdateDateTime) AS RollingValue
        FROM temptable
        GROUP BY Id, UpdateDateTime, Value
        ) rolling ON temptable.Id = rolling.Id AND temptable.UpdateDateTime = rolling.UpdateDateTime
        ) tab
        WHERE Id IN (1, 2) order by 1, 2, 3, 4
    """

    // rewrite cte children should work well with cost based rewrite rule. rely on rewrite rule: InferSetOperatorDistinct
    sql """
        WITH cte_0 AS ( SELECT 1 AS a ), cte_1 AS ( SELECT 1 AS a ) select * from cte_0, cte_1 union select * from cte_0, cte_1
    """
}

