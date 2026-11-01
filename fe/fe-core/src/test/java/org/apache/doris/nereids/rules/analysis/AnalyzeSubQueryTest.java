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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSubQueryTest extends TestWithFeService implements MemoPatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = ImmutableList.of(
            "SELECT * FROM (SELECT * FROM T1 T) T2",
            "SELECT * FROM T1 TT1 JOIN (SELECT * FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT * FROM T1 TT1 JOIN (SELECT TT2.ID FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT T.ID FROM T1 T",
            "SELECT A.ID FROM T1 A, T2 B WHERE A.ID = B.ID",
            "SELECT * FROM T1 JOIN T1 T2 ON T1.ID = T2.ID"
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    id bigint not null,\n"
                        + "    score bigint not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    @Test
    public void testTranslateCase() throws Exception {
        for (String sql : testSql) {
            StatementScopeIdGenerator.clear();
            StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            PhysicalPlan plan = planner.planWithLock(
                    parser.parseSingle(sql),
                    PhysicalProperties.ANY
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
        }
    }

    @Test
    public void testCaseSubQuery() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(0))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matches(
                    logicalProject(
                        logicalProject(
                            logicalProject(
                                logicalProject(
                                    logicalOlapScan().when(o -> true)
                                )
                            )
                        ).when(FieldChecker.check("projects", ImmutableList.of(
                            new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")),
                            new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("T"))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("T2"))))
                    )
                );
    }

    @Test
    public void testCaseMixed() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(1))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matches(
                    logicalProject(
                        innerLogicalJoin(
                            logicalProject(
                                logicalOlapScan()
                            ),
                            logicalProject(
                                logicalProject(
                                    logicalProject(
                                        logicalOlapScan()
                                    )
                                )
                            ).when(FieldChecker.check("projects", ImmutableList.of(
                                new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT2")),
                                new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("TT2"))))
                            )
                        )
                        .when(FieldChecker.check("otherJoinConjuncts",
                                ImmutableList.of(new EqualTo(
                                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")),
                        new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("T"))))
                    )
                );
    }

    @Test
    public void testCaseJoinSameTable() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(5))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matches(
                    logicalProject(
                        innerLogicalJoin(
                            logicalOlapScan(),
                            logicalProject(
                                logicalOlapScan()
                            )
                        )
                        .when(FieldChecker.check("otherJoinConjuncts", ImmutableList.of(new EqualTo(
                                new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("test", "T1")),
                                new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("test", "T1")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("test", "T1")),
                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")),
                        new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("T2"))))
                    )
                );
    }

    @Test
    public void testScalarSubquerySlotNullable() {
        List<String> nullableSqls = ImmutableList.of(
                // project list
                "select (select T3.id as k from T3 limit 1) from T1",
                "select (select T3.id as k from T3 where T3.score = T1.score limit 1) from T1",
                "select (select sum(T3.id) as k from T3) from T1",
                "select (select sum(T3.id) as k from T3 where T3.score = T1.score) from T1",
                "select (select sum(T3.id) as k from T3 group by T3.score limit 1) from T1",
                "select (select sum(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1) from T1",
                "select (select count(T3.id) as k from T3 group by T3.score limit 1) from T1",
                "select (select count(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1) from T1",

                // filter
                "select * from T1 where T1.id > (select T3.id as k from T3 limit 1)",
                "select * from T1 where T1.id > (select T3.id as k from T3 where T3.score = T1.score limit 1)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3 where T3.score = T1.score)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3 group by T3.score limit 1)",
                "select * from T1 where T1.id > (select sum(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1)",
                "select * from T1 where T1.id > (select count(T3.id) as k from T3 group by T3.score limit 1)",
                "select * from T1 where T1.id > (select count(T3.id) as k from T3 group by T3.score having T3.score = T1.score + 10 limit 1)"
        );

        List<String> notNullableSqls = ImmutableList.of(
                // project
                "select (select count(T3.id) as k from T3) from T1",
                "select (select count(T3.id) as k from T3 where T3.score = T1.score) from T1",

                // filter
                "select * from T1 where T1.id > (select count(T3.id) as k from T3)",
                "select * from T1 where T1.id > (select count(T3.id) as k from T3 where T3.score = T1.score)"
        );

        for (String sql : nullableSqls) {
            checkScalarSubquerySlotNullable(sql, true);
        }

        for (String sql : notNullableSqls) {
            checkScalarSubquerySlotNullable(sql, false);
        }
    }

    @Test
    public void testCorrelatedScalarSubqueryWithTopNProject() {
        String sql = "select T1.id from T1 where T1.score > "
                + "(select T2.score + 1 from T2 where T2.id = T1.id order by T2.score limit 1)";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(exception.getMessage().contains("limit is not supported in correlated subquery"));
    }

    @Test
    public void testCorrelatedScalarSubqueryWithTopN() {
        String sql = "select T1.id from T1 where T1.score > "
                + "(select T2.score from T2 where T2.id = T1.id order by T2.score limit 1)";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(exception.getMessage().contains("limit is not supported in correlated subquery"));
    }

    @Test
    public void testCorrelatedInSubqueryWithNestedLimit() {
        // The limit of the derived table keeps one row of the rows of the correlation key of an outer
        // row, so it cannot be evaluated once for the domains of every outer row (the rewrite which
        // unnests the subquery reads the value which the IN compares from the aggregation of one
        // domain): the subquery is reported instead of building a plan which reads the columns of the
        // outer query from the rows of another correlation key.
        String sql = "select T1.id from T1 where T1.id in "
                + "(select max(c) from (select count(*) c from T2 where T2.id = T1.id group by T2.score"
                + " limit 1) x)";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(
                exception.getMessage().contains("access outer query's column before limit is not supported"));
    }

    @Test
    public void testCorrelatedInSubqueryWithNestedTopN() {
        String sql = "select T1.id from T1 where T1.id in "
                + "(select max(c) from (select count(*) c from T2 where T2.id = T1.id group by T2.score"
                + " order by c limit 1) x)";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(
                exception.getMessage().contains("access outer query's column before limit is not supported"));
    }

    @Test
    public void testCorrelatedInSubqueryWithNestedLateralView() {
        // The lateral view of the derived table explodes the arrays of the rows of the correlation key
        // of an outer row, so it cannot be evaluated once for the domains of every outer row either.
        String sql = "select T1.id from T1 where T1.id in "
                + "(select max(e) from (select count(*) c, array_agg(T2.score) a from T2"
                + " where T2.id = T1.id group by T2.score) x lateral view explode(a) t as e)";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(
                exception.getMessage().contains("access outer query's column before lateral view is not supported"));
    }

    @Test
    public void testCorrelatedInSubqueryWithALateralViewWhichReadsTheOuterColumn() {
        // The generator of a lateral view below the correlated predicate reads the outer column, which
        // the rewrite of the subquery cannot produce on the inner side of the join.
        String sql = "select T1.id from T1 where T1.id in "
                + "(select e from T2 lateral view explode(array(T1.id, T2.score)) t as e"
                + " where T2.id = T1.id)";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(
                exception.getMessage().contains("access outer query's column in lateral view is not supported"));
    }

    @Test
    public void testCorrelatedScalarSubqueryWithALateralViewWhichReadsTheOuterColumn() {
        String sql = "select T1.id from T1 where T1.score > "
                + "(select e from T2 lateral view explode(array(T1.score, T2.score)) t as e)";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(
                exception.getMessage().contains("access outer query's column in lateral view is not supported"));
    }

    @Test
    public void testExistsOverScalarAggUnionOrderBy() {
        // EXISTS over scalar aggregate with ORDER BY wrapper and UNION ALL.
        // hasTopLevelScalarAgg() must see through LogicalSort to fold to TRUE/FALSE.
        String sql = "SELECT EXISTS ("
                + "SELECT COUNT(*) FROM ("
                + "SELECT id FROM T1 UNION ALL SELECT id FROM T2"
                + ") u ORDER BY 1"
                + ") AS result";
        PlanChecker.from(connectContext).analyze(sql);
    }

    @Test
    public void testNotExistsOverScalarAggUnionOrderBy() {
        String sql = "SELECT NOT EXISTS ("
                + "SELECT COUNT(*) FROM ("
                + "SELECT id FROM T1 UNION ALL SELECT id FROM T2"
                + ") u ORDER BY 1"
                + ") AS result";
        PlanChecker.from(connectContext).analyze(sql);
    }

    @Test
    public void testInSubqueryWhichReadsTheOuterColumnInItsAggregationIsRejected() {
        // The rewrite of a correlated IN subquery reads the value which the IN compares from the
        // aggregation of the domain of every outer row (see UnCorrelatedApplyAggregateFilter), so the
        // subquery may not read the outer column from its aggregation: the plan of the rewrite would
        // aggregate the value of the outer row and read it from a scan which does not produce it.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT sum(T2.score + T1.score) FROM T2)"));
        Assertions.assertTrue(exception.getMessage().contains("access outer query's column in aggregate"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testInSubqueryWhichReadsTheOuterColumnInItsProjectionIsRejected() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT T1.score FROM T2 WHERE T2.id = T1.id)"));
        Assertions.assertTrue(exception.getMessage().contains("access outer query's column in project"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testNestedAggregatedInSubqueryWhichReadsTheOuterColumnInItsFilterIsAnalyzed() {
        // the outer column of a filter below the aggregation of the subquery is the one which the
        // rewrite carries: the predicates of that filter become the condition of the join which pairs
        // an outer row with the rows of its domain, and the aggregations above it are grouped by the
        // correlation key of that row, however many of them the subquery has
        PlanChecker.from(connectContext).analyze(
                "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT max(c) FROM"
                        + " (SELECT count(*) AS c FROM T2 WHERE T2.id = T1.id GROUP BY T2.score) x)");
        PlanChecker.from(connectContext).analyze(
                "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT max(c) FROM"
                        + " (SELECT count(*) AS c FROM T2 WHERE T2.id = T1.id GROUP BY T2.score) x"
                        + " HAVING max(c) <= T1.score)");
    }

    @Test
    public void testInSubqueryWhichComputesAWindowIsRejected() {
        // The rewrite of a correlated IN subquery groups the aggregation of its domain by the
        // correlation key of the outer row, so the nodes of the subquery which sit above the correlated
        // predicate are evaluated on the rows of one correlation key. A window is evaluated on the rows
        // of the node it sits in, so the window of the rewrite would be evaluated over the rows of every
        // correlation key together, while the window of the subquery of the query is evaluated over the
        // rows of one domain.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT sum(T2.score) OVER () FROM T2"
                                + " WHERE T2.id = T1.id GROUP BY T2.score)"));
        Assertions.assertTrue(exception.getMessage().contains("before window function"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testScalarSubqueryWithGroupingSetsIsRejected() {
        // The grouping sets of the subquery are computed by a repeat node above the aggregation of the
        // domain (see containsARepeatAboveTheCorrelatedPredicate): the rewrite which unnests the
        // subquery reads the aggregation of that domain from below the repeat, so the repeat would be
        // evaluated on the rows of every correlation key together and the correlation predicate of the
        // subquery would have no aggregation below it to carry it.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id, (SELECT count(*) FROM T2 WHERE T2.id = T1.id"
                                + " GROUP BY GROUPING SETS ((T2.score), ())) FROM T1"));
        Assertions.assertTrue(exception.getMessage().contains("before grouping sets"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testInSubqueryWithGroupingSetsIsRejected() {
        // The IN subquery of the query is compared with the value of the grouping sets of every
        // correlation key together when the repeat is not reported: the value of the subquery of an
        // outer row would be the value of the aggregation of the domains of the other outer rows.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT count(*) FROM T2"
                                + " WHERE T2.id = T1.id GROUP BY GROUPING SETS ((T2.score), ()))"));
        Assertions.assertTrue(exception.getMessage().contains("before grouping sets"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testExistsSubqueryWithGroupingSetsIsRejected() {
        // The EXISTS of the outer rows whose correlated domain is empty would be true when the grouping
        // sets of the subquery are computed for the rows of every correlation key together: the group
        // above the aggregation of the domain of one of the other keys has a row as well.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE EXISTS (SELECT count(*) FROM T2"
                                + " WHERE T2.id = T1.id GROUP BY GROUPING SETS ((T2.score), ()))"));
        Assertions.assertTrue(exception.getMessage().contains("before grouping sets"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testGroupingSetsBelowTheCorrelatedPredicateIsAccepted() {
        // a repeat below the correlated predicate computes the rows which that predicate selects, so
        // the rewrite keeps its evaluation domain unchanged
        PlanChecker.from(connectContext).analyze(
                "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT count(*) FROM"
                        + " (SELECT id, score FROM T2 GROUP BY GROUPING SETS ((id, score), ())) x"
                        + " WHERE x.id = T1.id)");
    }

    @Test
    public void testInSubqueryWithJoinAboveTheCorrelatedPredicateIsRejected() {
        // The join combines the rows of the domain of an outer row with the rows of its other side,
        // and the rewrite reads the aggregation of that domain from below the join
        // (see containsAJoinAboveTheCorrelatedPredicate): the join would be evaluated once for the
        // rows of every correlation key together.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT count(*) FROM"
                                + " (SELECT T2.id, T2.score FROM T2 WHERE T2.score = T1.id) x"
                                + " JOIN T3 j ON x.id = j.id)"));
        Assertions.assertTrue(exception.getMessage().contains("before join"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testExistsSubqueryWithJoinAboveTheCorrelatedPredicateIsRejected() {
        // The aggregation above the join groups the rows which the join produces, and the keys of the
        // correlation are the keys of one branch of the join alone: the EXISTS of an outer row whose
        // domain is empty would be decided by the rows of another correlation key.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE EXISTS (SELECT count(*) FROM"
                                + " (SELECT T2.id, T2.score FROM T2 WHERE T2.score = T1.id) x"
                                + " JOIN T3 j ON x.id = j.id GROUP BY x.score)"));
        Assertions.assertTrue(exception.getMessage().contains("before join"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testJoinBelowTheCorrelatedPredicateIsAccepted() {
        // a join below the correlated predicate is part of the rows which that predicate selects (the
        // domain of an outer row), so the rewrite keeps it as it is
        PlanChecker.from(connectContext).analyze(
                "SELECT T1.id FROM T1 WHERE T1.id IN (SELECT count(*) FROM T2"
                        + " JOIN T3 ON T2.id = T3.id WHERE T2.score = T1.id)");
    }

    @Test
    public void testComputedProjectionBelowTheAggregationOfTheDomainIsRejected() {
        // The projection computes the columns which the aggregation above it reads from the rows of
        // the correlated domain, and the rewrite drops the projections between the filter of the
        // WHERE clause and the aggregation (see containsAComputedProjectionBelowTheAggregation): the
        // computed column of the projection would be missing below the aggregation.
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT T1.id FROM T1 WHERE T1.id NOT IN (SELECT max(c) FROM"
                                + " (SELECT count(z) c FROM (SELECT T2.id, T2.score + 1 z FROM T2"
                                + " WHERE T2.score = T1.id) p GROUP BY p.id HAVING count(z) > 0) x)"));
        Assertions.assertTrue(exception.getMessage().contains("below the aggregation"),
                "unexpected message: " + exception.getMessage());
    }

    @Test
    public void testProjectionOfTheDomainBelowTheAggregationIsAccepted() {
        // a projection which only passes the columns of its child through is redundant below the
        // aggregation of the domain, so the rewrite drops it and keeps the subquery
        PlanChecker.from(connectContext).analyze(
                "SELECT T1.id FROM T1 WHERE T1.id NOT IN (SELECT max(c) FROM"
                        + " (SELECT count(id) c FROM (SELECT T2.score, T2.id FROM T2"
                        + " WHERE T2.score = T1.id) p GROUP BY p.id) x)");
    }

    @Test
    public void testConstantProjectionOfTheDomainIsAccepted() {
        // the subquery does not aggregate the rows of its domain, so the rules which rewrite a set
        // membership read those rows (see UnCorrelatedApplyFilter): only the projections below an
        // aggregation are dropped by the rewrite of the aggregating subqueries, so the constant
        // projection of the select list of this subquery is accepted
        PlanChecker.from(connectContext).analyze(
                "SELECT T1.id FROM T1 WHERE T1.score + 2 IN (SELECT 1 FROM T2"
                        + " WHERE T1.id IS NULL AND T1.id IS NOT NULL)");
    }

    @Test
    public void testExistsCorrelatedScalarAggUnionOrderBy() {
        // Correlated EXISTS over scalar aggregate + UNION ALL + ORDER BY.
        // Must fold to TRUE/FALSE before checkNoCorrelatedSlotsUnderSetOp().
        String sql = "SELECT id FROM T1 t1 WHERE EXISTS ("
                + "SELECT COUNT(*) FROM ("
                + "SELECT id FROM T2 t2 WHERE t1.id = t2.id"
                + " UNION ALL "
                + "SELECT id FROM T3 t3 WHERE t1.id = t3.id"
                + ") u ORDER BY 1"
                + ") ORDER BY id";
        PlanChecker.from(connectContext).analyze(sql);
    }

    @Test
    public void testExistsOverScalarAggUnionDerivedTable() {
        // EXISTS over a scalar aggregate wrapped in a derived-table alias:
        //   WHERE EXISTS (SELECT * FROM (SELECT COUNT(*) FROM (<union>) u) a)
        // hasTopLevelScalarAgg() must see through LogicalSubQueryAlias to fold.
        String sql = "SELECT id FROM T1 t1 WHERE EXISTS ("
                + "SELECT * FROM ("
                + "SELECT COUNT(*) FROM ("
                + "SELECT id FROM T2 t2 WHERE t1.id = t2.id"
                + " UNION ALL "
                + "SELECT id FROM T3 t3 WHERE t1.id = t3.id"
                + ") u"
                + ") a"
                + ") ORDER BY id";
        PlanChecker.from(connectContext).analyze(sql);
    }

    @Test
    public void testExistsOverScalarAggUnionDerivedTableNotCorrelated() {
        // Non-correlated variant of the derived-table shape.
        String sql = "SELECT EXISTS ("
                + "SELECT * FROM ("
                + "SELECT COUNT(*) FROM ("
                + "SELECT id FROM T1 UNION ALL SELECT id FROM T2"
                + ") u"
                + ") a"
                + ") AS result";
        PlanChecker.from(connectContext).analyze(sql);
    }

    private void checkScalarSubquerySlotNullable(String sql, boolean outputNullable) {
        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .getPlan();
        List<LogicalProject<?>> projectList = Lists.newArrayList();
        List<LogicalPlan> plansAboveApply = Lists.newArrayList();
        root.foreach(plan -> {
            if (plan instanceof LogicalProject && plan.child(0) instanceof LogicalApply) {
                projectList.add((LogicalProject<?>) plan);
            }
            if (!(plan instanceof LogicalApply) && plan.anyMatch(p -> p instanceof LogicalApply)) {
                plansAboveApply.add((LogicalPlan) plan);
            }
        });

        Assertions.assertEquals(1, projectList.size());
        LogicalProject<?> project = projectList.get(0);
        LogicalApply<?, ?> apply = (LogicalApply<?, ?>) project.child();

        Assertions.assertNotNull(project);
        Assertions.assertNotNull(apply);

        List<String> slotKName = ImmutableList.of("k", "any_value(k)", "ifnull(k, 0)");
        NamedExpression output = project.getProjects().stream()
                .filter(e -> slotKName.contains(e.getName()))
                .findFirst().orElse(null);
        Assertions.assertNotNull(output);
        Assertions.assertEquals(outputNullable, output.nullable());

        Slot applySubqueySlot = apply.getOutput().stream()
                .filter(e -> slotKName.contains(e.getName()))
                .findFirst().orElse(null);
        Assertions.assertNotNull(applySubqueySlot);
        if (apply.isCorrelated()) {
            // apply will change to outer join
            Assertions.assertTrue(applySubqueySlot.nullable());
        } else {
            Assertions.assertEquals(outputNullable, applySubqueySlot.nullable());
        }

        for (LogicalPlan plan : plansAboveApply) {
            Assertions.assertTrue(plan.getInputSlots().stream()
                    .filter(slot -> slot.getExprId().equals(applySubqueySlot.getExprId()))
                    .allMatch(slot -> slot.nullable() == applySubqueySlot.nullable()));

            Assertions.assertTrue(plan.getOutput().stream()
                    .filter(slot -> slot.getExprId().equals(applySubqueySlot.getExprId()))
                    .allMatch(slot -> slot.nullable() == applySubqueySlot.nullable()));
        }
    }
}
