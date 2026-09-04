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

// ############################################################################
// Real query shapes over ADBC tables: joins, aggregation, windows, set
// operations, subqueries, CTEs, and writing the result back into Doris.
//
// None of these are pushed anywhere. The connector sends one flat
// SELECT ... [WHERE] [LIMIT] per scan and Doris does everything else, so what
// is under test is that an ADBC scan behaves like any other source of rows
// once the planner starts building on top of it -- that it reports the right
// column types for a join key, survives being read twice in a self join,
// and hands the executor rows in a shape the rest of the engine accepts.
//
// Every query runs TWICE, once against the ADBC catalog and once against the
// very same Doris tables natively, and the two results must be identical.
// That comparison is the assertion; the baselines beside it exist so a change
// shows up as a diff rather than only as a failure message. Note what the
// double run buys: for a query this complex, a baseline alone cannot
// distinguish "ADBC returned the wrong rows" from "this query always returned
// these rows", because nobody hand-computes a window function.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_query_shapes", "p0,external") {
    String repoRoot = new File(context.config.suitePath).getParentFile().getParentFile()
            .getAbsolutePath()
    String thirdparty = System.getenv("DORIS_THIRDPARTY")
    if (thirdparty == null || thirdparty.isEmpty()) {
        thirdparty = "${repoRoot}/thirdparty"
    }
    String driverPath = context.config.otherConfigs.get("adbcDriverPath")
    if (driverPath == null || driverPath.isEmpty()) {
        driverPath = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }

    if (!new File(driverPath).canRead()) {
        logger.info("SKIPPED test_adbc_query_shapes: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "COMPLEX QUERIES OVER ADBC TABLES ARE NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_query_shapes_catalog"
    String dbName = "test_adbc_query_shapes_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    sql """
        CREATE TABLE internal.${dbName}.customers (
          `c_id` int NOT NULL,
          `c_name` varchar(64) NULL,
          `c_region` varchar(32) NULL,
          `c_since` date NULL
        ) DISTRIBUTED BY HASH(`c_id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    // Customer 6 has no orders and customer 7 has a null region: the outer-join and grouping assertions
    // are only meaningful with a row on each side that the other side lacks.
    sql """
        INSERT INTO internal.${dbName}.customers VALUES
          (1, 'alice',  'east',  '2020-01-01'),
          (2, 'bob',    'west',  '2020-06-15'),
          (3, 'carol',  'east',  '2021-03-20'),
          (4, 'dave',   'north', '2021-11-11'),
          (5, 'erin',   'west',  '2022-02-02'),
          (6, 'frank',  'south', '2022-07-07'),
          (7, 'grace',  NULL,    '2023-05-05')
    """

    sql """
        CREATE TABLE internal.${dbName}.orders (
          `o_id` int NOT NULL,
          `o_cid` int NULL,
          `o_amount` decimalv3(10, 2) NULL,
          `o_ts` datetime(3) NULL,
          `o_status` varchar(16) NULL
        ) DISTRIBUTED BY HASH(`o_id`) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    // Order 12 points at a customer that does not exist, which is what makes the right/anti joins say
    // something; order 11 has a null amount so the aggregates have to handle it.
    sql """
        INSERT INTO internal.${dbName}.orders VALUES
          (101, 1, 100.00, '2023-01-01 10:00:00.000', 'paid'),
          (102, 1, 200.50, '2023-01-05 11:30:00.500', 'paid'),
          (103, 1,  50.25, '2023-02-01 09:15:00.250', 'refunded'),
          (104, 2, 300.00, '2023-01-10 14:00:00.000', 'paid'),
          (105, 2, 400.75, '2023-03-01 16:45:00.750', 'pending'),
          (106, 3,  10.00, '2023-02-15 08:00:00.000', 'paid'),
          (107, 3,  20.00, '2023-02-16 08:00:00.000', 'paid'),
          (108, 3,  30.00, '2023-02-17 08:00:00.000', 'refunded'),
          (109, 4, 999.99, '2023-04-01 12:00:00.000', 'paid'),
          (110, 5,   1.01, '2023-05-01 12:00:00.000', 'pending'),
          (111, 5,   NULL, '2023-05-02 12:00:00.000', 'pending'),
          (112, 99, 777.00, '2023-06-01 12:00:00.000', 'paid')
    """

    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "required"
        )
    """

    try {
        // Runs one query text against both the ADBC catalog and the Doris tables the catalog is reading,
        // and demands the same rows. @DB@ stands for whichever qualifier is being used.
        def sameAsSource = { String query ->
            def viaAdbc = sql(query.replace('@DB@', "${catalogName}.${dbName}"))
            def viaSource = sql(query.replace('@DB@', "internal.${dbName}"))
            assertEquals(viaSource.toString(), viaAdbc.toString(),
                    "reading through ADBC gave a different answer than the source did for:\n${query}")
            return viaAdbc
        }

        String adbcDb = "${catalogName}.${dbName}"

        // ---- aggregation ----

        sameAsSource("""
            SELECT count(*), count(o_amount), count(DISTINCT o_cid),
                   sum(o_amount), min(o_amount), max(o_amount)
            FROM @DB@.orders
        """)
        sameAsSource("""
            SELECT o_cid, count(*) AS n, sum(o_amount) AS total, avg(o_amount) AS avg_amount
            FROM @DB@.orders GROUP BY o_cid ORDER BY o_cid
        """)
        sameAsSource("""
            SELECT o_status, o_cid, count(*) AS n FROM @DB@.orders
            GROUP BY o_status, o_cid HAVING count(*) > 1 ORDER BY o_status, o_cid
        """)
        sameAsSource("""
            SELECT DISTINCT o_status FROM @DB@.orders ORDER BY o_status
        """)
        // GROUPING SETS reaches the aggregate node with a different shape than a plain GROUP BY, and the
        // null it synthesises for a rolled-up level must not be confused with the null already in the data.
        sameAsSource("""
            SELECT o_status, o_cid, count(*) AS n FROM @DB@.orders
            GROUP BY GROUPING SETS ((o_status), (o_cid), ())
            ORDER BY o_status, o_cid, n
        """)

        qt_agg_by_customer """
            SELECT o_cid, count(*) AS n, sum(o_amount) AS total
            FROM ${adbcDb}.orders GROUP BY o_cid ORDER BY o_cid
        """
        qt_agg_having """
            SELECT o_status, count(*) AS n FROM ${adbcDb}.orders
            GROUP BY o_status HAVING count(*) >= 2 ORDER BY o_status
        """

        // ---- ordering and paging ----

        sameAsSource("""
            SELECT o_id, o_cid, o_amount FROM @DB@.orders
            ORDER BY o_cid ASC, o_amount DESC, o_id ASC
        """)
        // Nulls have to sort where SQL says, not where they happen to land in an Arrow batch.
        sameAsSource("""
            SELECT o_id, o_amount FROM @DB@.orders ORDER BY o_amount ASC NULLS FIRST, o_id
        """)
        sameAsSource("""
            SELECT o_id FROM @DB@.orders ORDER BY o_id LIMIT 5 OFFSET 3
        """)

        qt_order_limit """SELECT o_id, o_amount FROM ${adbcDb}.orders ORDER BY o_id LIMIT 5 OFFSET 3"""

        // ---- window functions ----

        sameAsSource("""
            SELECT o_cid, o_id, o_amount,
                   row_number() OVER (PARTITION BY o_cid ORDER BY o_id) AS rn,
                   rank()       OVER (PARTITION BY o_cid ORDER BY o_amount DESC) AS rnk,
                   dense_rank() OVER (PARTITION BY o_cid ORDER BY o_amount DESC) AS drnk
            FROM @DB@.orders ORDER BY o_cid, o_id
        """)
        sameAsSource("""
            SELECT o_cid, o_id, o_amount,
                   sum(o_amount) OVER (PARTITION BY o_cid ORDER BY o_id
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running,
                   avg(o_amount) OVER (PARTITION BY o_cid) AS avg_in_group
            FROM @DB@.orders ORDER BY o_cid, o_id
        """)
        sameAsSource("""
            SELECT o_cid, o_id,
                   lag(o_id, 1)  OVER (PARTITION BY o_cid ORDER BY o_id) AS prev_id,
                   lead(o_id, 1) OVER (PARTITION BY o_cid ORDER BY o_id) AS next_id,
                   first_value(o_amount) OVER (PARTITION BY o_cid ORDER BY o_id) AS first_amount,
                   ntile(2) OVER (PARTITION BY o_cid ORDER BY o_id) AS half
            FROM @DB@.orders ORDER BY o_cid, o_id
        """)

        qt_window_rank """
            SELECT o_cid, o_id,
                   row_number() OVER (PARTITION BY o_cid ORDER BY o_id) AS rn
            FROM ${adbcDb}.orders ORDER BY o_cid, o_id
        """

        // ---- joins ----
        //
        // Both sides are ADBC tables here, so each join plans two independent remote scans.

        sameAsSource("""
            SELECT c.c_name, o.o_id, o.o_amount
            FROM @DB@.customers c JOIN @DB@.orders o ON c.c_id = o.o_cid
            ORDER BY c.c_name, o.o_id
        """)
        // Customer 6 has no orders: a left join that lost the row would still look plausible.
        sameAsSource("""
            SELECT c.c_id, c.c_name, count(o.o_id) AS n
            FROM @DB@.customers c LEFT JOIN @DB@.orders o ON c.c_id = o.o_cid
            GROUP BY c.c_id, c.c_name ORDER BY c.c_id
        """)
        // Order 112 points at a customer that does not exist.
        sameAsSource("""
            SELECT o.o_id, c.c_name
            FROM @DB@.customers c RIGHT JOIN @DB@.orders o ON c.c_id = o.o_cid
            ORDER BY o.o_id
        """)
        sameAsSource("""
            SELECT c.c_id, o.o_id
            FROM @DB@.customers c FULL OUTER JOIN @DB@.orders o ON c.c_id = o.o_cid
            ORDER BY c.c_id, o.o_id
        """)
        sameAsSource("""
            SELECT count(*) FROM @DB@.customers c CROSS JOIN @DB@.orders o
        """)
        // A self join reads the same remote table twice in one query.
        sameAsSource("""
            SELECT a.o_id, b.o_id
            FROM @DB@.orders a JOIN @DB@.orders b ON a.o_cid = b.o_cid AND a.o_id < b.o_id
            ORDER BY a.o_id, b.o_id
        """)
        // Three-way, so one scan's output feeds a join whose other input is itself a join.
        sameAsSource("""
            SELECT c.c_region, count(*) AS n
            FROM @DB@.customers c
            JOIN @DB@.orders o ON c.c_id = o.o_cid
            JOIN @DB@.customers c2 ON c2.c_region = c.c_region
            GROUP BY c.c_region ORDER BY c.c_region
        """)

        qt_join_inner """
            SELECT c.c_name, o.o_id, o.o_amount
            FROM ${adbcDb}.customers c JOIN ${adbcDb}.orders o ON c.c_id = o.o_cid
            ORDER BY c.c_name, o.o_id
        """
        qt_join_left """
            SELECT c.c_id, c.c_name, count(o.o_id) AS n
            FROM ${adbcDb}.customers c LEFT JOIN ${adbcDb}.orders o ON c.c_id = o.o_cid
            GROUP BY c.c_id, c.c_name ORDER BY c.c_id
        """

        // ---- subqueries ----

        sameAsSource("""
            SELECT c_id, c_name FROM @DB@.customers
            WHERE c_id IN (SELECT o_cid FROM @DB@.orders WHERE o_status = 'paid')
            ORDER BY c_id
        """)
        sameAsSource("""
            SELECT c_id, c_name FROM @DB@.customers c
            WHERE EXISTS (SELECT 1 FROM @DB@.orders o WHERE o.o_cid = c.c_id AND o.o_amount > 300)
            ORDER BY c_id
        """)
        sameAsSource("""
            SELECT c_id, c_name FROM @DB@.customers c
            WHERE NOT EXISTS (SELECT 1 FROM @DB@.orders o WHERE o.o_cid = c.c_id)
            ORDER BY c_id
        """)
        // A scalar subquery: one value, computed remotely, compared per row.
        sameAsSource("""
            SELECT o_id, o_amount FROM @DB@.orders
            WHERE o_amount > (SELECT avg(o_amount) FROM @DB@.orders)
            ORDER BY o_id
        """)
        // Correlated, in the select list.
        sameAsSource("""
            SELECT c.c_id,
                   (SELECT count(*) FROM @DB@.orders o WHERE o.o_cid = c.c_id) AS n
            FROM @DB@.customers c ORDER BY c.c_id
        """)
        // A derived table with its own aggregation feeding a join.
        sameAsSource("""
            SELECT c.c_name, t.total
            FROM @DB@.customers c
            JOIN (SELECT o_cid, sum(o_amount) AS total FROM @DB@.orders GROUP BY o_cid) t
              ON t.o_cid = c.c_id
            ORDER BY c.c_name
        """)

        qt_subquery_in """
            SELECT c_id, c_name FROM ${adbcDb}.customers
            WHERE c_id IN (SELECT o_cid FROM ${adbcDb}.orders WHERE o_status = 'paid')
            ORDER BY c_id
        """
        qt_subquery_not_exists """
            SELECT c_id, c_name FROM ${adbcDb}.customers c
            WHERE NOT EXISTS (SELECT 1 FROM ${adbcDb}.orders o WHERE o.o_cid = c.c_id)
            ORDER BY c_id
        """

        // ---- CTEs ----

        sameAsSource("""
            WITH paid AS (SELECT * FROM @DB@.orders WHERE o_status = 'paid'),
                 per_customer AS (SELECT o_cid, sum(o_amount) AS total FROM paid GROUP BY o_cid)
            SELECT c.c_name, p.total
            FROM per_customer p JOIN @DB@.customers c ON c.c_id = p.o_cid
            ORDER BY c.c_name
        """)
        // The same CTE consumed twice, so the scan under it is planned once and read by two consumers.
        sameAsSource("""
            WITH o AS (SELECT o_cid, o_amount FROM @DB@.orders)
            SELECT a.o_cid, count(*) FROM o a JOIN o b ON a.o_cid = b.o_cid
            GROUP BY a.o_cid ORDER BY a.o_cid
        """)

        qt_cte """
            WITH paid AS (SELECT * FROM ${adbcDb}.orders WHERE o_status = 'paid')
            SELECT o_cid, count(*) AS n FROM paid GROUP BY o_cid ORDER BY o_cid
        """

        // ---- set operations ----

        sameAsSource("""
            SELECT o_cid FROM @DB@.orders WHERE o_status = 'paid'
            UNION
            SELECT o_cid FROM @DB@.orders WHERE o_status = 'pending'
            ORDER BY 1
        """)
        sameAsSource("""
            SELECT o_cid FROM @DB@.orders WHERE o_status = 'paid'
            UNION ALL
            SELECT o_cid FROM @DB@.orders WHERE o_status = 'pending'
            ORDER BY 1
        """)
        sameAsSource("""
            SELECT o_cid FROM @DB@.orders WHERE o_status = 'paid'
            INTERSECT
            SELECT o_cid FROM @DB@.orders WHERE o_status = 'refunded'
            ORDER BY 1
        """)
        sameAsSource("""
            SELECT c_id FROM @DB@.customers
            EXCEPT
            SELECT o_cid FROM @DB@.orders
            ORDER BY 1
        """)

        qt_setop_union """
            SELECT o_cid FROM ${adbcDb}.orders WHERE o_status = 'paid'
            UNION
            SELECT o_cid FROM ${adbcDb}.orders WHERE o_status = 'pending'
            ORDER BY 1
        """

        // ---- expressions over remote columns ----

        sameAsSource("""
            SELECT o_id,
                   CASE WHEN o_amount IS NULL THEN 'unknown'
                        WHEN o_amount > 300 THEN 'large'
                        WHEN o_amount > 50  THEN 'medium'
                        ELSE 'small' END AS bucket,
                   coalesce(o_amount, 0) AS amount_or_zero,
                   nullif(o_status, 'paid') AS not_paid,
                   date_format(o_ts, '%Y-%m') AS month,
                   concat(o_status, '-', cast(o_cid AS string)) AS label
            FROM @DB@.orders ORDER BY o_id
        """)

        qt_expressions """
            SELECT o_id,
                   CASE WHEN o_amount IS NULL THEN 'unknown'
                        WHEN o_amount > 300 THEN 'large' ELSE 'small' END AS bucket,
                   coalesce(o_amount, 0) AS amount_or_zero
            FROM ${adbcDb}.orders ORDER BY o_id
        """

        // ---- the result written back into Doris ----
        //
        // An ADBC scan feeding a load is the shape a user actually runs, and it exercises the scan under a
        // sink rather than under a result set.

        sql """DROP TABLE IF EXISTS internal.${dbName}.copied"""
        sql """
            CREATE TABLE internal.${dbName}.copied (
              `o_id` int NOT NULL,
              `o_cid` int NULL,
              `o_amount` decimalv3(10, 2) NULL
            ) DISTRIBUTED BY HASH(`o_id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO internal.${dbName}.copied
            SELECT o_id, o_cid, o_amount FROM ${adbcDb}.orders WHERE o_status = 'paid'
        """
        qt_insert_select """
            SELECT o_id, o_cid, o_amount FROM internal.${dbName}.copied ORDER BY o_id
        """
        assertEquals(
                sql("""SELECT o_id, o_cid, o_amount FROM internal.${dbName}.orders
                       WHERE o_status = 'paid' ORDER BY o_id""").toString(),
                sql("""SELECT o_id, o_cid, o_amount FROM internal.${dbName}.copied
                       ORDER BY o_id""").toString(),
                "INSERT INTO ... SELECT through ADBC did not load the rows the source holds")

        sql """DROP TABLE IF EXISTS internal.${dbName}.ctas_target"""
        sql """
            CREATE TABLE internal.${dbName}.ctas_target
            PROPERTIES ("replication_num" = "1")
            AS SELECT o_cid, count(*) AS n, sum(o_amount) AS total
               FROM ${adbcDb}.orders GROUP BY o_cid
        """
        qt_ctas """SELECT o_cid, n, total FROM internal.${dbName}.ctas_target ORDER BY o_cid"""
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
