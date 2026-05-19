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

import org.apache.doris.common.NereidsException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.InApplyToJoin;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderApply;
import org.apache.doris.nereids.rules.rewrite.UnCorrelatedApplyFilter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class AnalyzeCTETest extends TestWithFeService implements MemoPatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    private final String multiCte = "WITH cte1 AS (SELECT s_suppkey FROM supplier WHERE s_suppkey < 5), "
            + "cte2 AS (SELECT s_suppkey FROM cte1 WHERE s_suppkey < 3)"
            + "SELECT * FROM cte1, cte2";

    private final String cteWithColumnAlias = "WITH cte1 (skey) AS (SELECT s_suppkey, s_nation FROM supplier WHERE s_suppkey < 5), "
            + "cte2 (sk2) AS (SELECT skey FROM cte1 WHERE skey < 3)"
            + "SELECT * FROM cte1, cte2";

    private final String cteConsumerInSubQuery = "WITH cte1 AS (SELECT * FROM supplier), "
            + "cte2 AS (SELECT * FROM supplier WHERE s_region in (\"ASIA\", \"AFRICA\"))"
            + "SELECT s_region, count(*) FROM cte1 GROUP BY s_region HAVING s_region in (SELECT s_region FROM cte2)";

    private final String cteConsumerJoin = "WITH cte1 AS (SELECT s_suppkey AS sk FROM supplier WHERE s_suppkey < 5), "
            + "cte2 AS (SELECT sk FROM cte1 WHERE sk < 3)"
            + "SELECT * FROM cte1 JOIN cte2 ON cte1.sk = cte2.sk";

    private final String cteLeadingJoin = "WITH cte1 AS (SELECT /*+ leading(supplier customer) */ s_suppkey AS sk "
            + "FROM supplier join customer on c_nation = s_nation), "
            + "cte2 AS (SELECT sk FROM cte1 WHERE sk < 3)"
            + "SELECT /*+ leading(cte2 cte1) */ * FROM cte1 JOIN cte2 ON cte1.sk = cte2.sk";

    private final String cteReferToAnotherOne = "WITH V1 AS (SELECT s_suppkey FROM supplier), "
            + "V2 AS (SELECT s_suppkey FROM V1)"
            + "SELECT * FROM V2";

    private final String cteJoinSelf = "WITH cte1 AS (SELECT s_suppkey FROM supplier)"
            + "SELECT * FROM cte1 AS t1, cte1 AS t2";

    private final String cteNested = "WITH cte1 AS ("
            + "WITH cte2 AS (SELECT s_suppkey FROM supplier) SELECT * FROM cte2)"
            + " SELECT * FROM cte1";

    private final String cteInTheMiddle = "SELECT * FROM (WITH cte1 AS (SELECT s_suppkey FROM supplier)"
            + " SELECT * FROM cte1) a";

    private final String cteWithDiffRelationId = "with s as (select * from supplier) select * from s as s1, s as s2";

    private final List<String> correlatedSubqueryWithCteSqls = ImmutableList.of(
            "SELECT * FROM supplier outer_s WHERE EXISTS ("
                    + "WITH picked AS (SELECT s_suppkey, s_region FROM supplier) "
                    + "SELECT 1 FROM picked WHERE picked.s_region = outer_s.s_region)",
            "SELECT * FROM supplier outer_s WHERE outer_s.s_suppkey IN ("
                    + "WITH picked AS (SELECT s_suppkey, s_region FROM supplier) "
                    + "SELECT picked.s_suppkey FROM picked WHERE picked.s_region = outer_s.s_region)"
    );

    private final String scalarSubqueryWithCteSql = "SELECT * FROM supplier outer_s WHERE outer_s.s_suppkey = ("
            + "WITH picked AS (SELECT s_suppkey FROM supplier WHERE s_nation = 'PERU') "
            + "SELECT min(picked.s_suppkey) FROM picked)";

    private final String correlatedSlotUnderCteProducerSql = "SELECT * FROM supplier outer_s WHERE EXISTS ("
            + "WITH picked AS (SELECT s_suppkey FROM supplier WHERE s_region = outer_s.s_region) "
            + "SELECT 1 FROM picked)";

    private final String correlatedOuterCteAliasShadowSql = "WITH seed AS ("
            + "SELECT id, grp, dt, ts6, ts3_n, i_nn, i_n, j_nn FROM cte_alias_shadow_outer "
            + "UNION ALL "
            + "SELECT id, grp, dt, ts6, ts3_n, i_nn, i_n, j_nn "
            + "FROM cte_alias_shadow_outer WHERE 1 = 0) "
            + "SELECT id FROM seed s "
            + "WHERE s.i_nn IN ("
            + "SELECT CASE WHEN b.i_n IS NULL THEN b.j_nn ELSE b.i_nn END "
            + "FROM cte_alias_shadow_inner b "
            + "WHERE b.grp = s.grp "
            + "AND (DATE(b.ts6) = s.dt OR b.ts3_n <=> s.ts3_n))";

    private final String correlatedOuterCteAliasNestedShadowSql = "WITH seed AS ("
            + "SELECT id, grp, v FROM cte_alias_shadow_outer_nested "
            + "UNION ALL "
            + "SELECT id, grp, v FROM cte_alias_shadow_outer_nested WHERE 1 = 0) "
            + "SELECT id FROM seed s "
            + "WHERE EXISTS ("
            + "SELECT 1 FROM cte_alias_shadow_inner_nested b "
            + "WHERE b.probe = s.v.x)";

    private final String currentScopeNestedFieldShouldShadowUnusableOuterPrefixSql = "WITH seed AS ("
            + "SELECT id, v FROM cte_alias_shadow_outer_prefix_only "
            + "UNION ALL "
            + "SELECT id, v FROM cte_alias_shadow_outer_prefix_only WHERE 1 = 0) "
            + "SELECT id FROM seed s "
            + "WHERE EXISTS ("
            + "SELECT 1 FROM cte_alias_shadow_inner_local_nested b "
            + "WHERE b.probe = s.v.x)";

    private final List<String> testSqls = ImmutableList.of(
            multiCte, cteWithColumnAlias, cteConsumerInSubQuery, cteConsumerJoin, cteReferToAnotherOne, cteJoinSelf,
            cteNested, cteInTheMiddle, cteWithDiffRelationId
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        SSBUtils.createTables(this);
        createView("CREATE VIEW V1 AS SELECT * FROM part");
        createView("CREATE VIEW V2 AS SELECT * FROM part");
        createTables(
                "create table test.cte_alias_shadow_outer\n"
                        + "(id int, grp int, dt date, ts6 datetimev2(6) not null, ts3_n datetimev2(3) null, "
                        + "i_nn int not null, i_n int null, j_nn int not null)\n"
                        + "duplicate key(id, grp, dt)\n"
                        + "distributed by hash(id) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.cte_alias_shadow_inner\n"
                        + "(id int, grp int, dt date, ts6 datetimev2(6) not null, ts3_n datetimev2(3) null, "
                        + "i_nn int not null, i_n int null, j_nn int not null, s varchar(32) null)\n"
                        + "duplicate key(id, grp, dt)\n"
                        + "distributed by hash(id) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.cte_alias_shadow_outer_nested\n"
                        + "(id int, grp int, v struct<x:int>)\n"
                        + "duplicate key(id, grp)\n"
                        + "distributed by hash(id) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.cte_alias_shadow_inner_nested\n"
                        + "(grp int, probe int, s struct<v:struct<x:int>>)\n"
                        + "duplicate key(grp, probe)\n"
                        + "distributed by hash(grp) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.cte_alias_shadow_outer_prefix_only\n"
                        + "(id int, v int)\n"
                        + "duplicate key(id)\n"
                        + "distributed by hash(id) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.cte_alias_shadow_inner_local_nested\n"
                        + "(probe int, s struct<v:struct<x:int>>)\n"
                        + "duplicate key(probe)\n"
                        + "distributed by hash(probe) buckets 1\n"
                        + "properties('replication_num' = '1');"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    /* ********************************************************************************************
     * Test CTE
     * ******************************************************************************************** */

    @Test
    public void testTranslateCase() throws Exception {
        for (String sql : testSqls) {
            StatementScopeIdGenerator.clear();
            StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
            PhysicalPlan plan = new NereidsPlanner(statementContext).planWithLock(
                    parser.parseSingle(sql),
                    PhysicalProperties.ANY
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator(new PlanTranslatorContext()).translatePlan(plan);
        }
    }

    @Test
    public void testLeadingCte() throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, cteLeadingJoin);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(parser.parseSingle(cteLeadingJoin), PhysicalProperties.ANY);
        Assertions.assertTrue(planner.getCascadesContext().isLeadingDisableJoinReorder());
    }

    @Test
    public void testCTEInHavingAndSubquery() {

        PlanChecker.from(connectContext)
                .analyze(cteConsumerInSubQuery)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .applyBottomUp(new InApplyToJoin())
                .matches(
                        logicalFilter(
                                logicalProject(
                                        logicalJoin(
                                                logicalAggregate(),
                                                logicalProject()
                                        )
                                )

                        )
                );
    }

    @Test
    public void testCTEWithAlias() {
        PlanChecker.from(connectContext)
                .analyze(cteConsumerJoin)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalCTEAnchor(
                                        logicalCTEProducer(),
                                        logicalProject(
                                                logicalJoin(
                                                        logicalCTEConsumer(),
                                                        logicalCTEConsumer()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    public void testCorrelatedSubqueryWithCte() throws Exception {
        for (String sql : correlatedSubqueryWithCteSqls) {
            StatementScopeIdGenerator.clear();
            Plan plan = PlanChecker.from(connectContext)
                    .analyze(sql)
                    .getPlan();
            Set<Plan> applyNodes = plan.collect(LogicalApply.class::isInstance);

            Assertions.assertEquals(1, applyNodes.size(), sql);
            LogicalApply<?, ?> apply = (LogicalApply<?, ?>) applyNodes.iterator().next();
            Assertions.assertTrue(apply.isCorrelated(), sql);
            Assertions.assertTrue(apply.child(1).anyMatch(LogicalCTEAnchor.class::isInstance), sql);
        }
    }

    @Test
    public void testScalarSubqueryWithCte() throws Exception {
        Plan plan = PlanChecker.from(connectContext)
                .analyze(scalarSubqueryWithCteSql)
                .getPlan();
        Set<Plan> cteAnchors = plan.collect(LogicalCTEAnchor.class::isInstance);

        Assertions.assertFalse(cteAnchors.isEmpty());
    }

    @Test
    public void testCorrelatedSlotUnderCteProducerInSubquery() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(correlatedSlotUnderCteProducerSql),
                "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("Unsupported correlated subquery in cte"));
    }

    @Test
    public void testCorrelatedOuterCteAliasNotShadowedByInnerColumn() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze(correlatedOuterCteAliasShadowSql)
                .getPlan();
        Set<Plan> applyNodes = plan.collect(LogicalApply.class::isInstance);

        Assertions.assertEquals(1, applyNodes.size());
        Assertions.assertTrue(((LogicalApply<?, ?>) applyNodes.iterator().next()).isCorrelated());
    }

    @Test
    public void testCorrelatedOuterCteAliasNestedReferenceNotShadowedByInnerColumn() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze(correlatedOuterCteAliasNestedShadowSql)
                .getPlan();
        Set<Plan> applyNodes = plan.collect(LogicalApply.class::isInstance);

        Assertions.assertEquals(1, applyNodes.size());
        Assertions.assertTrue(((LogicalApply<?, ?>) applyNodes.iterator().next()).isCorrelated());
    }

    @Test
    public void testCurrentScopeNestedFieldShadowsUnusableOuterPrefix() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze(currentScopeNestedFieldShouldShadowUnusableOuterPrefixSql)
                .getPlan();
        Set<Plan> applyNodes = plan.collect(LogicalApply.class::isInstance);

        Assertions.assertEquals(1, applyNodes.size());
        Assertions.assertFalse(((LogicalApply<?, ?>) applyNodes.iterator().next()).isCorrelated());
    }

    @Test
    public void testCTEWithAnExistedTableOrViewName() {
        PlanChecker.from(connectContext)
                .analyze(cteReferToAnotherOne)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalCTEAnchor(
                                        logicalCTEProducer(),
                                        logicalProject(
                                                logicalCTEConsumer()
                                        )
                                )
                        )
                );

    }

    @Test
    public void testDifferenceRelationId() {
        PlanChecker.from(connectContext)
                .analyze(cteWithDiffRelationId)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalProject(
                                        logicalJoin(
                                                logicalSubQueryAlias(
                                                        logicalCTEConsumer()
                                                ),
                                                logicalSubQueryAlias(
                                                        logicalCTEConsumer()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    public void testCteInTheMiddle() {
        PlanChecker.from(connectContext)
                .analyze(cteInTheMiddle)
                .matches(
                        logicalProject(
                                logicalSubQueryAlias(
                                        logicalCTEAnchor(
                                                logicalCTEProducer(),
                                                logicalProject(
                                                        logicalCTEConsumer()
                                                )
                                        )
                                )
                        )

                );
    }

    @Test
    public void testCteNested() {
        PlanChecker.from(connectContext)
                .analyze(cteNested)
                .matches(
                        logicalCTEAnchor(
                                logicalCTEProducer(
                                        logicalSubQueryAlias(
                                                logicalCTEAnchor(
                                                        logicalCTEProducer(),
                                                        logicalProject(
                                                                logicalCTEConsumer()
                                                        )
                                                )
                                        )
                                ),
                                logicalProject(
                                        logicalCTEConsumer()
                                )
                        )
                );
    }

    @Test
    public void testRecCteOutputNullable() {
        String sql = new StringBuilder()
                .append("WITH RECURSIVE test_table AS (\n")
                .append("    SELECT 1 UNION ALL\n")
                .append("    SELECT 2 FROM test_table\n")
                .append(")\n")
                .append("SELECT * FROM test_table;")
                .toString();
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        this.logicalRecursiveUnion(
                                logicalRecursiveUnionAnchor(
                                logicalProject(
                                        logicalOneRowRelation(
                                        )
                                ).when(project -> project.getProjects().get(0).child(0) instanceof Nullable)),
                                logicalRecursiveUnionProducer(
                                        logicalProject(
                                                logicalProject(
                                                        logicalCTEConsumer()
                                                )
                                        ).when(project -> project.getProjects().get(0).child(0) instanceof Nullable)
                                )
                        )
                );
    }

    @Test
    public void testRecCteWithoutRecKeyword() {
        String sql = new StringBuilder()
                .append("WITH RECURSIVE t1 AS (\n")
                .append("    SELECT 1\n")
                .append("UNION ALL\n")
                .append("    SELECT 2 FROM t1\n")
                .append("),\n").append("t2 AS (\n")
                .append("    SELECT 3\n")
                .append("UNION ALL\n")
                .append("    SELECT 4 FROM t1, t2\n")
                .append(")\n")
                .append("SELECT * FROM t2;")
                .toString();
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        this.logicalRecursiveUnion(
                                logicalRecursiveUnionAnchor(
                                        logicalProject(
                                                logicalOneRowRelation(
                                                )
                                        )
                                ),
                                logicalRecursiveUnionProducer(
                                        logicalProject(
                                                logicalProject(
                                                        logicalJoin()
                                                )
                                        )
                                )
                        ).when(cte -> cte.getCteName().equals("t2"))
                );
    }

    @Test
    public void testRecCteMultipleUnion() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select\n")
                .append("        1 as c1,\n").append("        1 as c2\n").append("),\n").append("t2 as (\n")
                .append("    select\n").append("        2 as c1,\n").append("        2 as c2\n").append("),\n")
                .append("xx as (\n").append("    select\n").append("        c1,\n").append("        c2\n")
                .append("    from\n").append("        t1\n").append("    union\n").append("    select\n")
                .append("        c1,\n").append("        c2\n").append("    from\n").append("        t2\n")
                .append("    union\n").append("    select\n").append("        c1,\n").append("        c2\n")
                .append("    from\n").append("        xx\n").append(")\n").append("select\n").append("    *\n")
                .append("from\n").append("    xx;").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                ExplainCommand.ExplainLevel.ANALYZED_PLAN);
        MemoTestUtils.initMemoAndValidState(planner.getCascadesContext());
        PlanChecker.from(planner.getCascadesContext()).matches(
                this.logicalRecursiveUnion(
                        logicalRecursiveUnionAnchor(
                            logicalProject(
                                    logicalUnion())),
                        logicalRecursiveUnionProducer()).when(cte -> cte.getCteName().equals("xx")));
    }

    /* ********************************************************************************************
     * Test CTE Exceptions
     * ******************************************************************************************** */

    @Test
    public void testCTEExceptionOfDuplicatedColumnAlias() {
        String sql = "WITH cte1 (a1, A1) AS (SELECT * FROM supplier)"
                + "SELECT * FROM cte1";

        NereidsException exception = Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("Duplicated CTE column alias: [a1] in CTE [cte1]"));
    }

    @Test
    public void testCTEExceptionOfColumnAliasSize() {
        String sql = "WITH cte1 (a1, a2) AS "
                + "(SELECT s_suppkey FROM supplier)"
                + "SELECT * FROM cte1";

        NereidsException exception = Assertions.assertThrows(NereidsException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        System.out.println(exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("CTE [cte1] returns 2 columns, "
                + "but 1 labels were specified."));
    }

    @Test
    public void testCTEExceptionOfReferenceInWrongOrder() {
        String sql = "WITH cte1 AS (SELECT * FROM cte2), "
                + "cte2 AS (SELECT * FROM supplier)"
                + "SELECT * FROM cte1, cte2";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[cte2] does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfErrorInUnusedCTE() {
        String sql = "WITH cte1 AS (SELECT * FROM not_existed_table)"
                + "SELECT * FROM supplier";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> PlanChecker.from(connectContext).checkPlannerResult(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[not_existed_table] does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfDuplicatedCTEName() {
        String sql = "WITH cte1 AS (SELECT * FROM supplier), "
                    + "cte1 AS (SELECT * FROM part)"
                    + "SELECT * FROM cte1";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[cte1] cannot be used more than once"));
    }

    @Test
    public void testCTEExceptionOfRefterCTENameNotInScope() {
        String sql = "WITH cte1 AS (WITH cte2 AS (SELECT * FROM supplier) SELECT * FROM cte2)"
                + "SELECT * FROM cte2";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("Table [cte2] does not exist in database"));
    }

    @Test
    public void testRecCteWithoutRecKeywordException() {
        String sql = new StringBuilder()
                .append("WITH t1 AS (\n")
                .append("    SELECT 1 UNION ALL\n")
                .append("    SELECT 2 FROM t1\n")
                .append(")\n")
                .append("SELECT * FROM t1;")
                .toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.ANALYZED_PLAN), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("Table [t1] does not exist in database"));
    }

    @Test
    public void testRecCteDatatypeException() {
        String sql = new StringBuilder().append("WITH RECURSIVE t1 AS (\n").append("    SELECT 1 AS number\n")
                .append("UNION ALL\n").append("    SELECT number + 1 FROM t1 WHERE number < 100\n").append(")\n")
                .append("SELECT number FROM t1;").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.ANALYZED_PLAN),
                "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("please add cast manually to get expect datatype"));
    }

    @Test
    public void testRecCteMultipleUnionException() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select\n")
                .append("        1 as c1,\n").append("        1 as c2\n").append("),\n").append("t2 as (\n")
                .append("    select\n").append("        2 as c1,\n").append("        2 as c2\n").append("),\n")
                .append("xx as (\n").append("    select\n").append("        c1,\n").append("        c2\n")
                .append("    from\n").append("        t1\n").append("    union\n").append("    select\n")
                .append("        c1,\n").append("        c2\n").append("    from\n").append("        xx\n")
                .append("    union\n").append("    select\n").append("        c1,\n").append("        c2\n")
                .append("    from\n").append("        t2\n").append(")\n").append("select\n").append("    *\n")
                .append("from\n").append("    xx").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.ANALYZED_PLAN),
                "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage()
                .contains("recursive reference to query xx must not appear within its non-recursive term"));
    }

    @Test
    public void testRecCteNoUnionException() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select 1 \n")
                .append("        intersect\n").append("    select 2 from t1\n").append(")\n").append("select\n")
                .append("    *\n").append("from\n").append("    t1").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.ANALYZED_PLAN),
                "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("recursive cte must be union"));
    }

    @Test
    public void testRecCteAnchorException() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select 1 from t1\n")
                .append("        union\n").append("    select 2 from t1\n").append(")\n").append("select\n")
                .append("    *\n").append("from\n").append("    t1;").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.ANALYZED_PLAN),
                "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage()
                .contains("recursive reference to query t1 must not appear within its non-recursive term"));
    }

    @Test
    public void testRecCteMoreThanOnceException() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select 1\n")
                .append("        union\n").append("    select 2 from t1 x, t1 y\n").append(")\n").append("select\n")
                .append("    *\n").append("from\n").append("    t1").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.ANALYZED_PLAN),
                "Not throw expected exception.");
        Assertions.assertTrue(
                exception.getMessage().contains("recursive reference to query t1 must not appear more than once"));
    }

    @Test
    public void testRecCteInSubqueryException() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select\n")
                .append("        1 as c1,\n").append("        1 as c2\n").append("),\n").append("xx as (\n")
                .append("    select\n").append("        2 as c1,\n").append("        2 as c2\n").append("    from\n")
                .append("        t1\n").append("    union\n").append("    select\n").append("        3 as c1,\n")
                .append("        3 as c2\n").append("    from\n")
                .append("        t1 where t1.c1 in (select c1 from xx)\n").append(")\n").append("select\n")
                .append("    *\n").append("from\n").append("    xx").toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.ANALYZED_PLAN),
                "Not throw expected exception.");
        Assertions.assertTrue(
                exception.getMessage().contains("Table [xx] does not exist in database"));
    }
}
