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
// Which predicates reach the source, which stay in Doris, and -- separately --
// whether the rows are right either way.
//
// Those are two different questions and this suite never lets one answer the
// other. BE re-evaluates every conjunct on whatever comes back, so pushing a
// predicate is PURE SPEED-UP: a scan that pushes nothing still returns the
// right rows. That means a row-count test can never tell whether pushdown
// works, and an EXPLAIN test can never tell whether the answer is right. So:
//
//   pushes / doesNotPush / pushesNothing   read the statement out of EXPLAIN
//   sameAsSource                           runs the SAME predicate natively
//                                          against the source table and demands
//                                          identical rows
//
// sameAsSource is the one that matters if pushdown is ever wrong rather than
// merely absent -- a mistranslated predicate changes which rows come back, and
// the source evaluating it itself is the independent answer.
//
// What is pushable is not a matter of opinion; it follows from two files:
//   ExprToConnectorExpressionConverter  Doris Expr -> neutral expression
//   AdbcQueryBuilder.render             neutral expression -> SQL, or nothing
// Anything the first turns into a ConnectorFunctionCall (arithmetic, function
// calls, casts, anything unrecognised) and anything the second has no branch
// for (LIKE, REGEXP, BETWEEN, <=>) is left behind by construction.
//
// Setup is the same as test_adbc_catalog_scan -- see its header.
// ############################################################################

suite("test_adbc_predicate_pushdown", "p0,external") {
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
        logger.info("SKIPPED test_adbc_predicate_pushdown: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "ADBC PREDICATE PUSHDOWN IS NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    String catalogName = "test_adbc_predicate_pushdown_catalog"
    String dbName = "test_adbc_predicate_pushdown_db"

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    sql """CREATE DATABASE internal.${dbName}"""

    sql """
        CREATE TABLE internal.${dbName}.t_pred (
          `id` int NOT NULL,
          `name` varchar(64) NULL,
          `score` double NULL,
          `amount` decimalv3(10, 2) NULL,
          `d` date NULL,
          `ts` datetime(6) NULL,
          `flag` boolean NULL,
          `big` bigint NULL,
          `score2` double NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // score2 exists for the one predicate that needs two nullable operands of ONE type: null-safe
    // equality. Against a literal, or against a NOT NULL column, Doris proves <=> equivalent to = (or
    // to IS NULL) and rewrites it before the scan; only column <=> column with both sides nullable
    // arrives as itself. Matching score's type keeps an implicit CAST out of it -- a CAST would be a
    // second, unrelated reason not to push, and the assertion would then pass without testing anything.
    // Nulls in every nullable column, and a quote in row 5. Without a row that actually contains the
    // quote, "name = 'O''Brien'" answers zero rows whether the literal was escaped correctly, was never
    // pushed, or matched nothing for a third reason.
    sql """
        INSERT INTO internal.${dbName}.t_pred VALUES
          (1, 'alice',  1.5,  10.25, '2024-01-01', '2024-01-01 00:00:00.000000', true,  1, 1.5),
          (2, 'bob',    2.5,  20.50, '2024-02-01', '2024-02-01 12:30:45.123456', false, 2, 9.5),
          (3, NULL,     3.5,  30.75, '2024-03-01', '2024-03-01 23:59:59.999999', true,  3, NULL),
          (4, 'carol',  NULL, NULL,  NULL,         NULL,                          NULL,  NULL, NULL),
          (5, "O'Brien", 5.5, 50.00, '2024-05-01', '2024-05-01 06:00:00.000001', false, 9223372036854775807, 5.5),
          (6, 'dave',   6.5,  60.00, '2024-06-01', '2024-06-01 06:00:00.000000', true,  -9223372036854775808, NULL)
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
        String table = "${catalogName}.${dbName}.t_pred"

        // ---- reading the statement out of EXPLAIN ----
        //
        // EXPLAIN never asks the driver to partition (planScan short-circuits on isExplainOnly), so the
        // statement shown is the one the query builder produced, whatever partitioned_read says.
        def remoteStatement = { String stmt ->
            String[] holder = new String[1]
            explain {
                sql(stmt)
                check { String plan ->
                    String line = plan.readLines().find { it.trim().startsWith("QUERY: ") }
                    assertNotNull(line, "no QUERY line in the plan:\n${plan}")
                    holder[0] = line.trim().substring("QUERY: ".length())
                    return true
                }
            }
            assertNotNull(holder[0], "EXPLAIN produced no remote statement for: ${stmt}")
            return holder[0]
        }

        def statementFor = { String where ->
            return remoteStatement("SELECT id FROM ${table} WHERE ${where}")
        }

        def pushes = { String where, String expected ->
            String q = statementFor(where)
            assertTrue(q.contains(expected),
                    "predicate [${where}] should have reached the source as [${expected}] but the "
                            + "statement was: ${q}")
            return q
        }

        def doesNotPush = { String where, String forbidden ->
            String q = statementFor(where)
            assertFalse(q.contains(forbidden),
                    "predicate [${where}] must not reach the source, but [${forbidden}] is in: ${q}")
            return q
        }

        // No WHERE clause at all: the strongest form, and the right one when the predicate under test is
        // the only conjunct. Checking merely that some keyword is absent would also pass if the connector
        // pushed the predicate under a different spelling.
        def pushesNothing = { String where ->
            String q = statementFor(where)
            assertFalse(q.contains("WHERE"),
                    "predicate [${where}] is not translatable, so the statement must carry no WHERE "
                            + "clause at all, but it was: ${q}")
            return q
        }

        // Correctness, entirely independent of the above: the source evaluates the same predicate itself.
        // If pushdown ever translates a predicate WRONGLY -- as opposed to not at all -- this is what
        // catches it, and no baseline can absorb the difference.
        def sameAsSource = { String where ->
            def viaAdbc = sql("SELECT id FROM ${table} WHERE ${where} ORDER BY id")
            def viaSource = sql("SELECT id FROM internal.${dbName}.t_pred WHERE ${where} ORDER BY id")
            assertEquals(viaSource.toString(), viaAdbc.toString(),
                    "predicate [${where}] selected different rows through ADBC than the source itself "
                            + "selects")
            return viaAdbc
        }

        // ---- comparisons ----

        pushes("id = 3", "`id` = 3")
        pushes("id != 3", "`id` != 3")
        pushes("id < 3", "`id` < 3")
        pushes("id <= 3", "`id` <= 3")
        pushes("id > 3", "`id` > 3")
        pushes("id >= 3", "`id` >= 3")
        ["id = 3", "id != 3", "id < 3", "id <= 3", "id > 3", "id >= 3"].each { sameAsSource(it) }

        // The literal on the left. The converter accepts either order and normalises to column-first.
        pushes("3 = id", "`id`")
        sameAsSource("3 = id")

        // ---- literal kinds ----
        //
        // AnsiDialect renders a literal only when it is sure the source reads that spelling back as the
        // same value, so each family below is a separate branch of renderLiteral.

        pushes("name = 'alice'", "`name` = 'alice'")
        // The quote must survive to the source as a doubled quote rather than ending the literal. Row 5 is
        // what gives this teeth: a wrong escaping either fails the remote statement or matches nothing.
        pushes("name = 'O''Brien'", "'O''Brien'")
        pushes("score > 2.5", "`score` > 2.5")
        pushes("amount > 20.50", "`amount` >")
        pushes("big = 9223372036854775807", "9223372036854775807")
        pushes("d = '2024-01-01'", "DATE '2024-01-01'")
        // The one comparison family this connector will not push, and the reason is that this source's
        // datetime column arrives as TIMESTAMPTZ: an instant. By the time the literal reaches the
        // dialect it has been converted to UTC, and standard SQL's TIMESTAMP '...' spelling carries no
        // zone, so the source would read that UTC wall clock as its own local time. East of UTC that
        // merely widens the match; west of UTC it drops rows the query wanted, and a scan cannot get
        // back rows the source never sent. sameAsSource below is what says the ANSWER is still right.
        pushesNothing("ts > '2024-01-01 00:00:00'")
        ["name = 'alice'", "name = 'O''Brien'", "score > 2.5", "amount > 20.50",
         "big = 9223372036854775807", "d = '2024-01-01'",
         "ts > '2024-01-01 00:00:00'"].each { sameAsSource(it) }

        // A boolean predicate may reach the scan as `flag = TRUE` or, after simplification, as the bare
        // column. Both are legitimate and both are pushable, so only the column is asserted -- pinning one
        // spelling would make this fail on a planner change that costs nothing.
        String flagQuery = pushes("flag = true", "`flag`")
        logger.info("boolean predicate reached the source as: ${flagQuery}")
        sameAsSource("flag = true")

        // ---- null tests ----

        pushes("name IS NULL", "`name` IS NULL")
        pushes("name IS NOT NULL", "`name` IS NOT NULL")
        sameAsSource("name IS NULL")
        sameAsSource("name IS NOT NULL")

        // A predicate that leaves the projected column all-null in the result. The projection is id, but BE
        // re-evaluates the predicate, so name is a query slot too and the source returns a column with no
        // non-null value in it -- the shape that once failed with "Unsupported arrow type for string
        // column: 9" because a source infers types from values.
        assertEquals(1, sql("SELECT id FROM ${table} WHERE name IS NULL").size())

        // ---- IN ----

        pushes("id IN (1, 3, 5)", "`id` IN (1, 3, 5)")
        pushes("id NOT IN (1, 3)", "NOT IN")
        pushes("name IN ('alice', 'bob')", "`name` IN ('alice', 'bob')")
        ["id IN (1, 3, 5)", "id NOT IN (1, 3)", "name IN ('alice', 'bob')"].each { sameAsSource(it) }

        // An IN whose list is not all literals is not pushable as a whole: the converter refuses the list
        // unless every item is one. A constant expression would not do here -- the planner folds that to a
        // literal before the scan ever sees it -- so the list holds a column instead.
        pushesNothing("id IN (1, length(name))")
        sameAsSource("id IN (1, length(name))")

        // ---- boolean connectives ----

        String andQuery = pushes("id > 1 AND score < 5.0", "`id` > 1")
        assertTrue(andQuery.contains("`score` < 5.0"), "the second conjunct was dropped: ${andQuery}")
        // A disjunction over ONE column does not reach the scan as a disjunction: Doris rewrites it
        // into an IN list long before planning. Asserting on " OR " here would be asserting on the
        // optimizer rather than on this connector, so the expectation is what actually arrives -- and
        // the OR-rendering path is covered by a two-column disjunction, which nothing rewrites.
        pushes("id = 1 OR id = 5", "`id` IN (1, 5)")
        pushes("id = 1 OR score > 4.0", " OR ")
        // NOT is normalised away before the scan too: Doris negates the operator (= becomes !=,
        // IS NULL becomes IS NOT NULL) and pushes NOT through AND/OR by De Morgan. So no SQL NOT
        // survives to be rendered; what must survive is the MEANING, which the sameAsSource pass
        // below checks. The connector's own NOT rendering is exercised by its unit tests.
        pushes("NOT (id = 1)", "`id` != 1")
        pushes("NOT (id = 1 AND score > 1.0)", " OR ")
        pushes("(id = 1 OR id = 2) AND name IS NOT NULL", "`id` IN (1, 2)")
        pushes("NOT (id IN (1, 2) OR (score > 1.0 AND name IS NULL))", "`id` NOT IN (1, 2)")
        ["id > 1 AND score < 5.0", "id = 1 OR id = 5", "id = 1 OR score > 4.0", "NOT (id = 1)",
         "NOT (id = 1 AND score > 1.0)", "(id = 1 OR id = 2) AND name IS NOT NULL",
         "NOT (id IN (1, 2) OR (score > 1.0 AND name IS NULL))"].each { sameAsSource(it) }

        // ---- what must NOT be pushed ----

        pushesNothing("name LIKE 'a%'")
        pushesNothing("name NOT LIKE 'a%'")
        pushesNothing("name REGEXP '^a'")
        pushesNothing("upper(name) = 'ALICE'")
        pushesNothing("abs(id) > 1")
        // Arithmetic between a column and a literal never reaches the scan as arithmetic: Nereids
        // folds id + 1 > 2 into id > 1 first, and pushing THAT is correct, not a leak. Arithmetic
        // over two columns is what the optimizer cannot fold away, so it is what pins the refusal.
        pushesNothing("id + big > 2")
        pushes("id + 1 > 2", "`id` > 1")
        pushesNothing("length(name) = 5")
        // Null-safe equality has no portable spelling: every substitute differs from it on nulls, and
        // getting that wrong changes which rows match rather than failing. Two nullable columns is the
        // only shape that reaches the scan as itself -- row 4 (both null) is what a substituted `=`
        // would drop. Against a literal Doris rewrites it to `=` first, which is sound in a filter and
        // is pushed as such: that is the optimizer's decision, not a leak in this connector.
        pushesNothing("score <=> score2")
        pushes("score <=> 2.5", "`score` = 2.5")
        ["name LIKE 'a%'", "name NOT LIKE 'a%'", "name REGEXP '^a'", "upper(name) = 'ALICE'",
         "abs(id) > 1", "id + big > 2", "id + 1 > 2", "length(name) = 5",
         "score <=> score2", "score <=> 2.5"].each { sameAsSource(it) }

        // A predicate over a date function. Not asserted as unpushable: some Doris versions rewrite this
        // into a range on the column itself, which IS pushable and is not a defect -- the connector would
        // then be pushing a comparison, not a function. Only the rows are asserted; the statement is logged
        // so a change in that rewrite is visible rather than silent.
        logger.info("a date-function predicate reached the source as: ${statementFor("year(d) = 2024")}")
        sameAsSource("year(d) = 2024")

        // A cast around the column is not a weaker predicate, it is a different one, so the connector
        // declines it. Whether the planner even produces a cast here depends on the version; what cannot
        // change is that the word CAST must never appear in a statement this connector generated, because
        // it has no branch that can emit one.
        String castQuery = statementFor("id > 1.5")
        assertFalse(castQuery.contains("CAST"), "a cast reached the source: ${castQuery}")
        logger.info("a comparison needing a cast produced: ${castQuery}")
        sameAsSource("id > 1.5")

        // BETWEEN arrives as its own neutral node, which the query builder has no branch for. Whether the
        // planner rewrote it into two comparisons first is a planner detail and is only logged; the word
        // BETWEEN itself can never legitimately appear.
        String betweenQuery = statementFor("id BETWEEN 2 AND 4")
        assertFalse(betweenQuery.contains("BETWEEN"),
                "the connector emitted a BETWEEN it has no renderer for: ${betweenQuery}")
        logger.info("BETWEEN reached the source as: ${betweenQuery}")
        sameAsSource("id BETWEEN 2 AND 4")

        // ---- mixing pushable and unpushable ----

        // AND is per-conjunct: the translatable half is pushed and the rest is left to Doris. Dropping a
        // conjunct from an AND only widens what the source returns, which BE then narrows again.
        String mixedAnd = pushes("id > 1 AND name LIKE 'a%'", "`id` > 1")
        assertFalse(mixedAnd.contains("LIKE"), "LIKE was pushed: ${mixedAnd}")
        sameAsSource("id > 1 AND name LIKE 'a%'")

        // OR is all-or-nothing. Dropping a branch of an OR NARROWS the result, so a partly pushed OR would
        // lose rows outright -- the one failure mode here that returns wrong data rather than being slow.
        pushesNothing("id = 1 OR name LIKE 'a%'")
        sameAsSource("id = 1 OR name LIKE 'a%'")

        // The same rule one level down -- except that this predicate does not reach the scan as a NOT
        // over an OR at all: De Morgan turns it into id != 1 AND name NOT LIKE 'a%' first, which is two
        // top-level conjuncts, so the AND rule above applies and the translatable half goes. What must
        // still hold is the same invariant: no half of the DISJUNCTION is pushed on its own. A pushed
        // LIKE here would be the failure that returns wrong rows rather than slow ones.
        String mixedNot = pushes("NOT (id = 1 OR name LIKE 'a%')", "`id` != 1")
        assertFalse(mixedNot.contains("LIKE"), "LIKE was pushed: ${mixedNot}")
        sameAsSource("NOT (id = 1 OR name LIKE 'a%')")

        // ---- the limit rule ----
        //
        // A row limit rides along only when EVERY predicate was pushed. Otherwise the source truncates
        // before Doris applies what it kept, and the query answers with too few rows. This is checked by
        // row count rather than through EXPLAIN, which regenerates the statement without a limit and so
        // has none to show either way.

        // Two rows match 'a%' or 'b%' in the source; both must come back despite the limit.
        assertEquals(2, sql("SELECT id FROM ${table} WHERE name LIKE 'a%' OR name LIKE 'b%' LIMIT 5").size(),
                "a limit was pushed alongside a predicate that stayed in Doris, so the source truncated "
                        + "before Doris filtered")
        assertEquals(1, sql("SELECT id FROM ${table} WHERE name LIKE 'b%' LIMIT 5").size())
        // Fully pushable, so the limit may go too; the answer must still be exactly two rows.
        assertEquals(2, sql("SELECT id FROM ${table} WHERE id > 1 ORDER BY id LIMIT 2").size())

        // ---- baselines for a representative subset ----
        //
        // The values, recorded. Everything above already compares against the source, so these exist to
        // make a change in behaviour visible in a diff rather than only in an assertion message.

        qt_pred_eq """SELECT id, name, score FROM ${table} WHERE id = 3"""
        qt_pred_ne """SELECT id, name FROM ${table} WHERE id != 3 ORDER BY id"""
        qt_pred_range """SELECT id, score FROM ${table} WHERE score >= 2.5 AND score <= 5.5 ORDER BY id"""
        qt_pred_in """SELECT id, name FROM ${table} WHERE id IN (1, 3, 5) ORDER BY id"""
        qt_pred_not_in """SELECT id, name FROM ${table} WHERE id NOT IN (1, 3) ORDER BY id"""
        qt_pred_null """SELECT id FROM ${table} WHERE name IS NULL ORDER BY id"""
        qt_pred_not_null """SELECT id FROM ${table} WHERE name IS NOT NULL ORDER BY id"""
        qt_pred_quote """SELECT id, name FROM ${table} WHERE name = 'O''Brien'"""
        qt_pred_date """SELECT id, d FROM ${table} WHERE d >= '2024-03-01' ORDER BY id"""
        qt_pred_ts """SELECT id, ts FROM ${table} WHERE ts > '2024-03-01 00:00:00' ORDER BY id"""
        qt_pred_or """SELECT id FROM ${table} WHERE id = 1 OR id = 5 ORDER BY id"""
        qt_pred_not """SELECT id FROM ${table} WHERE NOT (id = 1) ORDER BY id"""
        qt_pred_like """SELECT id, name FROM ${table} WHERE name LIKE '%o%' ORDER BY id"""
        qt_pred_mixed """SELECT id, name FROM ${table} WHERE id > 1 AND name LIKE '%o%' ORDER BY id"""
        qt_pred_between """SELECT id FROM ${table} WHERE id BETWEEN 2 AND 4 ORDER BY id"""
        qt_pred_deep """
            SELECT id FROM ${table}
            WHERE NOT (id IN (1, 2) OR (score > 5.0 AND name IS NOT NULL)) ORDER BY id
        """

        // ---- the same predicates on the single-statement path ----
        //
        // Everything above ran through the partitioned path. A catalog with that turned off builds the very
        // same statement but reads it as one range, so the two must agree on every predicate. Compared as
        // values rather than baselines: a baseline would pass if both paths broke identically.
        String singleRangeCatalog = "${catalogName}_single_range"
        sql """DROP CATALOG IF EXISTS ${singleRangeCatalog}"""
        sql """
            CREATE CATALOG ${singleRangeCatalog} PROPERTIES (
                "type" = "adbc",
                "driver_url" = "${driverPath}",
                "uri" = "grpc://127.0.0.1:${arrowPort}",
                "user" = "root",
                "password" = "",
                "partitioned_read" = "disabled"
            )
        """
        try {
            ["id > 1 AND score < 5.0", "id IN (1, 3, 5)", "name IS NULL", "name LIKE 'a%'",
             "id = 1 OR name LIKE 'a%'", "d = '2024-01-01'", "name = 'O''Brien'"].each { String where ->
                def partitioned = sql("SELECT id FROM ${table} WHERE ${where} ORDER BY id")
                def singleRange = sql(
                        "SELECT id FROM ${singleRangeCatalog}.${dbName}.t_pred WHERE ${where} ORDER BY id")
                assertEquals(singleRange.toString(), partitioned.toString(),
                        "the partitioned and single-statement paths disagreed on: ${where}")
            }
        } finally {
            sql """DROP CATALOG IF EXISTS ${singleRangeCatalog}"""
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        sql """DROP DATABASE IF EXISTS internal.${dbName} FORCE"""
    }
}
