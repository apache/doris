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
suite("test_qualify_query") {
    sql "create database if not exists qualify_test"
    sql "use qualify_test"
    sql "DROP TABLE IF EXISTS sales"
    sql """
           CREATE TABLE sales (
               year INT,
               country STRING,
               product STRING,
               profit INT
            ) 
            DISTRIBUTED BY HASH(`year`) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            )
        """
    sql """
        INSERT INTO sales VALUES
        (2000,'Finland','Computer',1501),
        (2000,'Finland','Phone',100),
        (2001,'Finland','Phone',10),
        (2000,'India','Calculator',75),
        (2000,'India','Calculator',76),
        (2000,'India','Computer',1201),
        (2000,'USA','Calculator',77),
        (2000,'USA','Computer',1502),
        (2001,'USA','Calculator',50),
        (2001,'USA','Computer',1503),
        (2001,'USA','Computer',1202),
        (2001,'USA','TV',150),
        (2001,'USA','TV',101);
        """

    qt_select_1 "select year + 1 as year, country from sales where year >= 2000 qualify row_number() over (order by year) > 1 order by year,country;"

    qt_select_4 "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year = 2000 qualify rk = 1;"

    qt_select_5 "select year, country, product, profit, row_number() over (partition by year, country order by profit desc) as rk from sales where year = 2000 qualify rk = 1 order by year, country, product, profit;"

    qt_select_6 "select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1 order by year, country;"

    qt_select_7 "select year, country, profit from (select year, country, profit from (select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200) t where rk = 1) a where year >= 2000 qualify row_number() over (order by profit) = 1;"

    qt_select_8 "select year, country, profit from (select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1) a qualify row_number() over (order by profit) = 1;"

    qt_select_9 "select * except(year) replace(profit+1 as profit), row_number() over (order by profit) as rk from sales where year >= 2000 qualify rk = 1;"

    qt_select_10 "select * except(year) replace(profit+1 as profit) from sales where year >= 2000 qualify row_number() over (order by year) > profit;"

    qt_select_12 "select year + 1, if(country = 'USA', 'usa' , country), case when profit < 200 then 200 else profit end as new_profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1 order by new_profit;"

    qt_select_13 "select year + 1, if(country = 'USA', 'usa' , country), case when profit < 200 then 200 else profit end as new_profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc)  = 1 order by new_profit;"

    qt_select_14 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by profit desc, country) = 1 order by country,profit;"

    qt_select_15 "select *,row_number() over (partition by year order by profit desc, country) as rk from sales where year >= 2000 qualify rk = 1 order by country,profit;"

    qt_select_16 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by if(profit > 200, profit, profit+200) desc, country) = profit order by country;"

    qt_select_17 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by case when profit > 200 then profit else profit+200 end desc, country) = profit order by country;"

    qt_select_18 "select distinct x.year, x.country, x.product from sales x left join sales y on x.year = y.year left join sales z on x.year = z.year where x.year >= 2000 qualify row_number() over (partition by x.year order by x.profit desc) = x.profit order by year;"

    qt_select_19 "select year, country, profit, row_number() over (order by profit) as rk1, row_number() over (order by country) as rk2 from (select * from sales) a where year >= 2000 qualify rk1 = 1 and rk2 > 2;"

    qt_select_20 "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year >= 2000 qualify rk + 1 > 1 * 100;"

    qt_select_21 "select year, country, profit, row_number() over (order by profit) as rk from (select * from sales) a where year >= 2000 qualify rk in (1,2,3);"

    qt_select_22 "select year, country, profit, row_number() over (order by profit) as rk from (select * from sales) a where year >= 2000 qualify rk = (select 1);"

    qt_select_23 "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year >= 2000 qualify rk = (select max(year) from sales);"

    qt_select_24 "select year+1, country, sum(profit) as total from sales where year >= 2000 and country = 'Finland' group by year,country having sum(profit) > 100 qualify row_number() over (order by year) = 1;"

    qt_select_25 "select year, country, profit from (select * from sales) a where year >= 2000 qualify row_number() over (partition by year, country order by profit desc) = 1 order by year, country, profit;"

    qt_select_26 "select year + 1, country from sales where year >= 2000 and country = 'Finland' group by year,country qualify row_number() over (order by year) > 1;"

    qt_select_27 "select year + 1, country, row_number() over (order by year) as rk from sales where year >= 2000 and country = 'Finland' group by year,country qualify rk > 1;"

    qt_select_28 "select year + 1, country, sum(profit) as total from sales where year >= 2000 group by year,country having sum(profit) > 1700 qualify row_number() over (order by year) = 1;"

    qt_select_29 "select distinct year + 1,country from sales qualify row_number() over (order by profit + 1) = 1;"

    qt_select_30 "select distinct year,country, row_number() over (order by profit + 1) as rk from sales qualify row_number() over (order by profit + 1) = 1;"

    qt_select_31 "select distinct year + 1 as year,country from sales where country = 'Finland' group by year, country qualify row_number() over (order by year) = 1;"

    qt_select_32 "select distinct year,country from sales having sum(profit) > 100 qualify row_number() over (order by year) > 100;"

    qt_select_33 "select distinct year,country,rank() over (order by year) from sales where country = 'USA' having sum(profit) > 100 qualify row_number() over (order by year) > 1;"

    qt_select_34 "select distinct year,country,rank() over (order by year) from sales where country = 'India' having sum(profit) > 100;"

    qt_select_35 "select year + 1, country from sales having profit >= 100 qualify row_number() over (order by profit) = 6;"

    qt_select_36 "select year + 1, country, row_number() over (order by profit) rk from sales having profit >= 100 qualify rk = 6;"

    // correlated subquery: an outer column referenced in qualify after an explicit group by
    // should not be treated as a missing inner group-by column under ONLY_FULL_GROUP_BY.
    qt_select_37 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          GROUP BY i.k
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND o.flag = 1
        )
        ORDER BY o.k;
    """

    // an inner non-grouped column referenced in qualify should still be rejected
    // under ONLY_FULL_GROUP_BY.
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
              UNION ALL
              SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k
              FROM (
                SELECT CAST(1 AS INT) AS k, CAST(1 AS INT) AS not_grouped
                UNION ALL
                SELECT CAST(2 AS INT) AS k, CAST(2 AS INT) AS not_grouped
              ) AS i
              GROUP BY i.k
              QUALIFY row_number() OVER (ORDER BY i.k) = 1
                      AND i.not_grouped = 1
            )
        """
        exception "must appear in the GROUP BY clause"
    }

    // correlated subquery over a plain project (no group by): the outer column in
    // qualify must not be pushed into the inner project's output.
    qt_select_38 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND o.flag = 1
        )
        ORDER BY o.k;
    """

    // correlated subquery over qualify -> having -> project: the outer column in
    // qualify must not be pushed into the inner project's output.
    qt_select_39 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          HAVING i.k >= 1
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND o.flag = 1
        )
        ORDER BY o.k;
    """

    // qualify -> having -> agg where both the having and the qualify reference correlated outer
    // columns. The window in qualify is extracted into a project above the having, so the having's
    // correlated predicate must be conjoined into the qualify to be decorrelated together.
    // o.h = 1 for k = 10 while o.flag = 1: the having predicate must still filter it out.
    qt_select_40 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag, CAST(0 AS INT) AS h
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag, CAST(1 AS INT) AS h
        ) AS o
        WHERE EXISTS (
          SELECT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          GROUP BY i.k
          HAVING o.h = 1
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND o.flag = 1
        )
        ORDER BY o.k;
    """

    // positive counterpart of select_40: only k = 10 satisfies both o.h = 1 and o.flag = 1.
    qt_select_41 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag, CAST(1 AS INT) AS h
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag, CAST(0 AS INT) AS h
        ) AS o
        WHERE EXISTS (
          SELECT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          GROUP BY i.k
          HAVING o.h = 1
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND o.flag = 1
        )
        ORDER BY o.k;
    """

    // qualify -> project where the qualify references a project alias (f) whose producer is a
    // correlated outer column (o.flag). The alias-producer dependency must be preserved so the
    // correlation is still extracted even though the project contains a window expression.
    qt_select_42 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY rn = 1
                  AND f = 1
        )
        ORDER BY o.k;
    """

    // negative counterpart of select_42: with o.flag = 0 everywhere the alias-resolved
    // correlation must still filter out every outer row.
    qt_select_43 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(0 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY rn = 1
                  AND f = 1
        )
        ORDER BY o.k;
    """

    // DISTINCT subquery where the having and the qualify carry separate correlated outer
    // predicates: both must stay on the same decorrelatable side of the distinct barrier,
    // otherwise one of them is left dangling in the apply's right subtree.
    // o.h = 0 < count(*) (2 distinct groups) and o.flag = 1 both hold only for k = 10.
    qt_select_44 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag, CAST(0 AS INT) AS h
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(1 AS INT) AS flag, CAST(5 AS INT) AS h
        ) AS o
        WHERE EXISTS (
          SELECT DISTINCT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          HAVING o.h < count(*)
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND o.flag = 1
        )
        ORDER BY o.k;
    """

    // a project over a constant UNION ALL that references a correlated outer column: the
    // project must not be pushed through the union (the outer slot has no producer inside
    // the union children), and the correlation must still be decorrelated correctly.
    qt_select_45 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, o.flag AS f
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND f = 1
        )
        ORDER BY o.k;
    """

    // a correlated having predicate that depends on the aggregate result cannot be evaluated
    // below the window project and must be rejected instead of being silently dropped or
    // moved above the window (which would change the evaluation order).
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(0 AS INT) AS h
            ) AS o
            WHERE EXISTS (
              SELECT i.k
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              GROUP BY i.k
              HAVING count(*) = o.h
              QUALIFY row_number() OVER (ORDER BY i.k) = 1
            )
        """
        exception "in HAVING depending on the aggregate result"
    }

    // an aggregate output alias that only depends on outer correlated columns cannot be
    // produced by the aggregate and must be rejected explicitly.
    sql """SET sql_mode = '';"""
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              GROUP BY i.k
              QUALIFY f = 1 AND rn = 1
            )
        """
        exception "only depends on outer correlated columns is not supported"
    }

    // Under ONLY_FULL_GROUP_BY an unaliased select item that is an outer correlated column
    // (e.g. `SELECT i.k, o.flag ... GROUP BY i.k`) stays a raw SlotReference in the aggregate
    // output, so the alias-only check above would skip it and NormalizeAggregate would report a
    // false GROUP BY error. Classify direct correlated slot outputs too and reject them with
    // the targeted unsupported-shape error.
    sql """SET sql_mode = 'ONLY_FULL_GROUP_BY';"""
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k, o.flag, row_number() OVER (ORDER BY i.k) AS rn
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              GROUP BY i.k
              QUALIFY o.flag = 1 AND rn = 1
            )
        """
        exception "Aggregate output column 'flag' that is an outer correlated column is not supported"
    }
    sql """SET sql_mode = '';"""

    // A volatile correlated predicate in a DISTINCT subquery must stay on its original side of
    // the distinct barrier (visible outer slots do not make a predicate constant, and moving a
    // volatile predicate changes how many times it is evaluated). random() * 0.0 keeps the
    // predicate volatile during analysis but is effectively `o.flag > 0.5`, so the result is
    // deterministic: only k = 10 (flag = 1) survives, with duplicate k rows in the input.
    qt_select_46 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT DISTINCT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND o.flag + random() * 0.0 > 0.5
        )
        ORDER BY o.k;
    """

    // A volatile alias producer (e.g. random() + o.flag AS f) must not be substituted into the
    // qualify, otherwise the volatile expression would be evaluated twice with different values.
    // This usage is rejected with a clear error instead of silently producing a mismatched result
    // or failing later with a cryptic slot-validation error.
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k, random() + o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              QUALIFY f >= o.flag AND rn = 1
            )
        """
        exception "QUALIFY referencing a correlated outer column through a volatile expression"
    }

    // A replaceable correlated alias (f = o.flag) must still be resolved even when another qualify
    // conjunct contains an uncorrelated IN subquery. The alias replacement is only skipped inside
    // subquery-containing conjuncts, not for the whole qualify.
    qt_select_47 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY rn = 1
                  AND f = 1
                  AND i.k IN (SELECT CAST(1 AS INT) UNION ALL SELECT CAST(2 AS INT))
        )
        ORDER BY o.k;
    """

    // A correlated alias referenced inside a subquery-containing qualify conjunct cannot be
    // resolved (the replacement would descend into the subquery and break the apply's slot
    // ownership), so this usage is rejected with a clear error.
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              QUALIFY rn = 1
                      AND f IN (SELECT CAST(1 AS INT) UNION ALL SELECT CAST(2 AS INT))
            )
        """
        exception "QUALIFY referencing a correlated outer column through a subquery"
    }

    // A project alias whose producer contains a subquery (e.g. o.flag + (SELECT max(x.k) ...) AS f)
    // must not be substituted into the qualify (the subquery would be copied into two plan nodes and
    // only one copy would be unnested). This usage is rejected with a clear error.
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k, (o.flag + (SELECT max(x.k) FROM (SELECT CAST(1 AS INT) AS k UNION ALL SELECT CAST(2 AS INT) AS k) AS x)) AS f, row_number() OVER (ORDER BY i.k) AS rn
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              QUALIFY f > 0 AND rn = 1
            )
        """
        exception "QUALIFY referencing a correlated outer column through a subquery"
    }

    // A project alias whose producer is a window expression over only outer columns (e.g.
    // row_number() OVER (ORDER BY o.flag) AS rn) must not be substituted into the qualify
    // (it would be re-extracted into a fresh alias endlessly). This usage is rejected with a
    // clear error instead of hanging or failing with a cryptic slot-validation error.
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k, row_number() OVER (ORDER BY o.flag) AS rn
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              QUALIFY rn = 1
            )
        """
        exception "QUALIFY referencing a correlated outer column through a window expression"
    }

    // A replaceable correlated alias in one conjunct must still be resolved even when other
    // conjuncts contain subqueries: the alias replacement never crosses a subquery's apply
    // boundary, so the alias (o.flag -> f) is substituted only in the subquery-free conjuncts
    // and the scalar subquery / nested EXISTS are unnested independently. Full-pipeline
    // mixed scalar/EXISTS coverage: o.flag = 1 and the scalar subquery > 0 and the nested
    // EXISTS hold only for k = 10.
    qt_select_48 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY rn = 1
                  AND f = 1
                  AND (SELECT max(x.k) FROM (SELECT CAST(1 AS INT) AS k UNION ALL SELECT CAST(3 AS INT) AS k) AS x) > 0
                  AND EXISTS (SELECT 1 FROM (SELECT CAST(1 AS INT) AS v) AS j WHERE j.v = i.k)
        )
        ORDER BY o.k;
    """

    // A nested subquery in a DISTINCT subquery's QUALIFY that is correlated to a column which
    // is output by the inner query (here i.k) stays on its original side of the distinct
    // barrier and is decorrelated together with the outer correlation: both predicates hold
    // only for k = 10.
    qt_select_49 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT DISTINCT i.k
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
                  AND (o.flag = 1 AND EXISTS (SELECT 1 FROM (SELECT CAST(1 AS INT) AS v) AS j WHERE j.v = i.k))
        )
        ORDER BY o.k;
    """

    // A nested subquery in QUALIFY correlated to a column of the inner query that is NOT in the
    // SELECT list cannot be decorrelated: getInputSlots() does not traverse the subquery's inner
    // plan, so the hidden correlation (i.not_grouped) is never surfaced in the project output and
    // the apply would be left with a dangling slot. This shape is rejected with a clear error
    // instead of failing with a cryptic slot-validation error.
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT DISTINCT i.k
              FROM (
                SELECT CAST(1 AS INT) AS k, CAST(5 AS INT) AS not_grouped
                UNION ALL
                SELECT CAST(2 AS INT) AS k, CAST(6 AS INT) AS not_grouped
              ) AS i
              QUALIFY row_number() OVER (ORDER BY i.k) = 1
                      AND (o.flag = 1 AND EXISTS (SELECT 1 FROM (SELECT CAST(100 AS INT) AS v) AS j WHERE j.v = i.not_grouped))
            )
        """
        exception "QUALIFY nested subquery referencing column 'not_grouped' that is not in the inner query output"
    }

    // Same hidden non-output correlation through a nested scalar subquery: the scalar subquery
    // correlates to i.not_grouped which is not produced by the DISTINCT query, so it cannot be
    // decorrelated and is rejected with a clear error.
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT DISTINCT i.k
              FROM (
                SELECT CAST(1 AS INT) AS k, CAST(5 AS INT) AS not_grouped
                UNION ALL
                SELECT CAST(2 AS INT) AS k, CAST(6 AS INT) AS not_grouped
              ) AS i
              QUALIFY row_number() OVER (ORDER BY i.k) = 1
                      AND (o.flag = 1 AND (SELECT max(x.k) FROM (SELECT CAST(1 AS INT) AS k UNION ALL SELECT CAST(2 AS INT) AS k) AS x WHERE x.k = i.not_grouped) > 0)
            )
        """
        exception "QUALIFY nested subquery referencing column 'not_grouped' that is not in the inner query output"
    }

    // For a plain (non-DISTINCT) project, a nested subquery in QUALIFY correlated to a column of
    // the inner query that is not in the SELECT list is still supported: the correlation slot is
    // owned by the child plan and is surfaced in the lower project output (here it is also
    // directly referenced by `i.not_grouped > 0`, so it is already classified as a child-owned
    // support slot). The upper/lower project split gives the nested apply a left child that owns
    // i.not_grouped and the unchanged upper project strips the helper. Both predicates hold only
    // for k = 10.
    qt_select_50 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k, CAST(5 AS INT) AS not_grouped
            UNION ALL
            SELECT CAST(2 AS INT) AS k, CAST(6 AS INT) AS not_grouped
          ) AS i
          QUALIFY rn = 1
                  AND i.not_grouped > 0
                  AND o.flag = 1
                  AND EXISTS (SELECT 1 FROM (SELECT CAST(5 AS INT) AS v) AS j WHERE j.v = i.not_grouped)
        )
        ORDER BY o.k;
    """

    // Same surfacing for a plain project when the nested subquery is the ONLY reference to the
    // non-output column: the correlation slot is not visible to getInputSlots(), but because
    // there is no distinct/aggregate barrier it is added to the project output and the nested
    // apply is decorated normally. Holds only for k = 10.
    qt_select_51 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k, CAST(5 AS INT) AS not_grouped
            UNION ALL
            SELECT CAST(2 AS INT) AS k, CAST(6 AS INT) AS not_grouped
          ) AS i
          QUALIFY rn = 1
                  AND o.flag = 1
                  AND EXISTS (SELECT 1 FROM (SELECT CAST(5 AS INT) AS v) AS j WHERE j.v = i.not_grouped)
        )
        ORDER BY o.k;
    """

    // A correlated alias consumed ONLY by HAVING (f = o.flag is referenced in HAVING but not in
    // QUALIFY) must still be resolved: the having conjuncts take part in the alias classification,
    // so `HAVING f = 1` is rewritten to `HAVING o.flag = 1` and the correlation stays visible to
    // subquery unnesting even though the project carries a window expression. Both predicates
    // (f = 1 and rn = 1) hold only for k = 10.
    qt_select_52 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          HAVING f = 1
          QUALIFY rn = 1
        )
        ORDER BY o.k;
    """

    // negative counterpart of select_52: with o.flag = 0 everywhere the alias-resolved correlation
    // in HAVING must still filter out every outer row.
    qt_select_53 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(0 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          HAVING f = 1
          QUALIFY rn = 1
        )
        ORDER BY o.k;
    """

    // Same alias-in-HAVING resolution for a DISTINCT project: the rewritten `HAVING o.flag = 1`
    // stays together with the qualify on the decorrelatable side of the distinct barrier, and the
    // window is evaluated inside the qualify. Holds only for k = 10.
    qt_select_54 """
        SELECT o.k
        FROM (
          SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
          UNION ALL
          SELECT CAST(20 AS INT) AS k, CAST(0 AS INT) AS flag
        ) AS o
        WHERE EXISTS (
          SELECT DISTINCT i.k, o.flag AS f
          FROM (
            SELECT CAST(1 AS INT) AS k
            UNION ALL
            SELECT CAST(2 AS INT) AS k
          ) AS i
          HAVING f = 1
          QUALIFY row_number() OVER (ORDER BY i.k) = 1
        )
        ORDER BY o.k;
    """

    // An aggregate output alias that only depends on outer correlated columns and is consumed
    // ONLY by HAVING cannot be produced by the aggregate, and the HAVING conjuncts must take part
    // in the aggregate-output classification too. This shape is rejected explicitly (instead of
    // leaving the outer slot dangling below the window project).
    sql """SET sql_mode = '';"""
    test {
        sql """
            SELECT o.k
            FROM (
              SELECT CAST(10 AS INT) AS k, CAST(1 AS INT) AS flag
            ) AS o
            WHERE EXISTS (
              SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn
              FROM (
                SELECT CAST(1 AS INT) AS k
                UNION ALL
                SELECT CAST(2 AS INT) AS k
              ) AS i
              GROUP BY i.k
              HAVING f = 1
              QUALIFY rn = 1
            )
        """
        exception "only depends on outer correlated columns is not supported"
    }
    sql """SET sql_mode = '';"""
}





