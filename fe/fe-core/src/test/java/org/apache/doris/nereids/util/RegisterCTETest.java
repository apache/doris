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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.analysis.CTEContext;
import org.apache.doris.nereids.rules.rewrite.AggregateDisassemble;
import org.apache.doris.nereids.rules.rewrite.logical.InApplyToJoin;
import org.apache.doris.nereids.rules.rewrite.logical.PushApplyUnderFilter;
import org.apache.doris.nereids.rules.rewrite.logical.PushApplyUnderProject;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class RegisterCTETest extends TestWithFeService implements PatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    private final String sql1 = "WITH cte1 AS (SELECT s_suppkey FROM supplier WHERE s_suppkey < 5), "
            + "cte2 AS (SELECT s_suppkey FROM cte1 WHERE s_suppkey < 3)"
            + "SELECT * FROM cte1, cte2";

    private final String sql2 = "WITH cte1 (skey) AS (SELECT s_suppkey, s_nation FROM supplier WHERE s_suppkey < 5), "
            + "cte2 (sk2) AS (SELECT skey FROM cte1 WHERE skey < 3)"
            + "SELECT * FROM cte1, cte2";

    private final String sql3 = "WITH cte1 AS (SELECT * FROM supplier), "
            + "cte2 AS (SELECT * FROM supplier WHERE s_region in (\"ASIA\", \"AFRICA\"))"
            + "SELECT s_region, count(*) FROM cte1 GROUP BY s_region HAVING s_region in (SELECT s_region FROM cte2)";

    private final String sql4 = "WITH cte1 AS (SELECT s_suppkey AS sk FROM supplier WHERE s_suppkey < 5), "
            + "cte2 AS (SELECT sk FROM cte1 WHERE sk < 3)"
            + "SELECT * FROM cte1 JOIN cte2 ON cte1.sk = cte2.sk";

    private final String sql5 = "WITH V1 AS (SELECT s_suppkey FROM supplier), "
            + "V2 AS (SELECT s_suppkey FROM V1)"
            + "SELECT * FROM V2";

    private final String sql6 = "WITH cte1 AS (SELECT s_suppkey FROM supplier)"
            + "SELECT * FROM cte1 AS t1, cte1 AS t2";

    private final List<String> testSql = ImmutableList.of(
            sql1, sql2, sql3, sql4, sql5, sql6
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        SSBUtils.createTables(this);
        createView("CREATE VIEW V1 AS SELECT * FROM part");
        createView("CREATE VIEW V2 AS SELECT * FROM part");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    private CTEContext getCTEContextAfterRegisterCTE(String sql) {
        return PlanChecker.from(connectContext)
                .analyze(sql)
                .getCascadesContext().getStatementContext().getCteContext();
    }

    /* ********************************************************************************************
     * Test CTE
     * ******************************************************************************************** */

    @Test
    public void testTranslateCase() throws Exception {
        new MockUp<RuleSet>() {
            @Mock
            public List<Rule> getExplorationRules() {
                return Lists.newArrayList(new AggregateDisassemble().build());
            }
        };

        for (String sql : testSql) {
            NamedExpressionUtil.clear();
            StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
            PhysicalPlan plan = new NereidsPlanner(statementContext).plan(
                    parser.parseSingle(sql),
                    PhysicalProperties.ANY
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator().translatePlan(plan, new PlanTranslatorContext());
        }
    }

    @Test
    public void testCTERegister() {
        CTEContext cteContext = getCTEContextAfterRegisterCTE(sql1);

        Assertions.assertTrue(cteContext.containsCTE("cte1")
                && cteContext.containsCTE("cte2"));

        LogicalPlan cte2InitialPlan = cteContext.getInitialCTEPlan("cte2");
        PlanChecker.from(connectContext, cte2InitialPlan).matchesFromRoot(
                logicalProject(
                    logicalFilter(
                        logicalSubQueryAlias(
                            logicalProject(
                                logicalFilter(
                                    unboundRelation()
                                )
                            )
                        )
                    )
                )
        );
    }

    @Test
    public void testCTERegisterWithColumnAlias() {
        CTEContext cteContext = getCTEContextAfterRegisterCTE(sql2);

        Assertions.assertTrue(cteContext.containsCTE("cte1")
                && cteContext.containsCTE("cte2"));

        // check initial plan
        LogicalPlan cte1InitialPlan = cteContext.getInitialCTEPlan("cte1");

        List<NamedExpression> targetProjects = new ArrayList<>();
        targetProjects.add(new UnboundAlias(new UnboundSlot("s_suppkey"), "skey"));
        targetProjects.add(new UnboundSlot("s_nation"));

        PlanChecker.from(connectContext, cte1InitialPlan)
                .matches(
                    logicalProject(
                    ).when(FieldChecker.check("projects", targetProjects))
                );

        // check analyzed plan
        LogicalPlan cte1AnalyzedPlan = cteContext.getAnalyzedCTEPlan("cte1");

        targetProjects = new ArrayList<>();
        targetProjects.add(new Alias(new ExprId(7),
                new SlotReference(new ExprId(0), "s_suppkey", VarcharType.INSTANCE,
                false, ImmutableList.of("defaulst_cluster:test", "supplier")), "skey"));
        targetProjects.add(new SlotReference(new ExprId(4), "s_nation", VarcharType.INSTANCE,
                false, ImmutableList.of("defaulst_cluster:test", "supplier")));
        PlanChecker.from(connectContext, cte1AnalyzedPlan)
                .matches(
                    logicalProject(
                    ).when(FieldChecker.check("projects", targetProjects))
                );
    }

    @Test
    public void testCTEInHavingAndSubquery() {
        SlotReference region1 = new SlotReference(new ExprId(5), "s_region", VarcharType.INSTANCE,
                false, ImmutableList.of("cte1"));
        SlotReference region2 = new SlotReference(new ExprId(12), "s_region", VarcharType.INSTANCE,
                false, ImmutableList.of("cte2"));
        SlotReference count = new SlotReference(new ExprId(14), "count()", BigIntType.INSTANCE,
                false, ImmutableList.of());
        Alias countAlias = new Alias(new ExprId(14), new Count(), "count()");

        PlanChecker.from(connectContext)
                .analyze(sql3)
                .applyBottomUp(new PushApplyUnderProject())
                .applyBottomUp(new PushApplyUnderFilter())
                .applyBottomUp(new InApplyToJoin())
                .matches(
                        logicalProject(
                            logicalJoin(
                                logicalAggregate()
                                    .when(FieldChecker.check("outputExpressions", ImmutableList.of(region1, countAlias)))
                                    .when(FieldChecker.check("groupByExpressions", ImmutableList.of(region1))),
                                any()
                            ).when(FieldChecker.check("joinType", JoinType.LEFT_SEMI_JOIN))
                                .when(FieldChecker.check("otherJoinConjuncts", ImmutableList.of(
                                    new EqualTo(region1, region2)
                                )))
                        ).when(FieldChecker.check("projects", ImmutableList.of(region1, count)))
                );
    }

    @Test
    public void testCTEWithAlias() {
        SlotReference skInCTE1 = new SlotReference(new ExprId(7), "sk", IntegerType.INSTANCE,
                false, ImmutableList.of("cte1"));
        SlotReference skInCTE2 = new SlotReference(new ExprId(15), "sk", IntegerType.INSTANCE,
                false, ImmutableList.of("cte2"));
        Alias skAlias = new Alias(new ExprId(7),
                new SlotReference(new ExprId(0), "s_suppkey", IntegerType.INSTANCE,
                        false, ImmutableList.of("default_cluster:test", "supplier")), "sk");
        PlanChecker.from(connectContext)
                .analyze(sql4)
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalProject().when(FieldChecker.check("projects", ImmutableList.of(skAlias))),
                            logicalProject().when(FieldChecker.check("projects", ImmutableList.of(skInCTE2)))
                        ).when(FieldChecker.check("joinType", JoinType.INNER_JOIN))
                            .when(FieldChecker.check("otherJoinConjuncts", ImmutableList.of(
                                new EqualTo(skInCTE1, skInCTE2)
                            )))
                    ).when(FieldChecker.check("projects", ImmutableList.of(skInCTE1, skInCTE2)))
                );
    }

    @Test
    public void testCTEWithAnExistedTableOrViewName() {
        SlotReference suppkeyInV1 = new SlotReference(new ExprId(7), "s_suppkey", IntegerType.INSTANCE,
                false, ImmutableList.of("V1"));
        SlotReference suppkeyInV2 = new SlotReference(new ExprId(7), "s_suppkey", IntegerType.INSTANCE,
                false, ImmutableList.of("V2"));
        SlotReference suppkeyInSupplier = new SlotReference(new ExprId(7), "s_suppkey", IntegerType.INSTANCE,
                false, ImmutableList.of("default_cluster:test", "supplier"));
        PlanChecker.from(connectContext)
                .analyze(sql5)
                .matches(
                    logicalProject(
                        logicalProject(
                            logicalProject()
                                .when(FieldChecker.check("projects", ImmutableList.of(suppkeyInSupplier)))
                        ).when(FieldChecker.check("projects", ImmutableList.of(suppkeyInV1)))
                    ).when(FieldChecker.check("projects", ImmutableList.of(suppkeyInV2)))
                );

    }


    /* ********************************************************************************************
     * Test CTE Exceptions
     * ******************************************************************************************** */

    @Test
    public void testCTEExceptionOfDuplicatedColumnAlias() {
        String sql = "WITH cte1 (a1, A1) AS (SELECT * FROM supplier)"
                + "SELECT * FROM cte1";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).checkPlannerResult(sql);
        }, "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("Duplicated CTE column alias: [a1] in CTE [cte1]"));
    }

    @Test
    public void testCTEExceptionOfColumnAliasSize() {
        String sql = "WITH cte1 (a1, a2) AS "
                + "(SELECT s_suppkey FROM supplier)"
                + "SELECT * FROM cte1";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).checkPlannerResult(sql);
        }, "Not throw expected exception.");
        System.out.println(exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("CTE [cte1] returns 2 columns, "
                + "but 1 labels were specified."));
    }

    @Test
    public void testCTEExceptionOfReferenceInWrongOrder() {
        String sql = "WITH cte1 AS (SELECT * FROM cte2), "
                + "cte2 AS (SELECT * FROM supplier)"
                + "SELECT * FROM cte1, cte2";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            PlanChecker.from(connectContext).checkPlannerResult(sql);
        }, "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[cte2] does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfErrorInUnusedCTE() {
        String sql = "WITH cte1 AS (SELECT * FROM not_existed_table)"
                + "SELECT * FROM supplier";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            PlanChecker.from(connectContext).checkPlannerResult(sql);
        }, "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[not_existed_table] does not exist in database"));
    }

    @Test
    public void testCTEExceptionOfDuplicatedCTEName() {
        String sql = "WITH cte1 AS (SELECT * FROM supplier), "
                    + "cte1 AS (SELECT * FROM part)"
                    + "SELECT * FROM cte1";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        }, "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("[cte1] cannot be used more than once"));
    }
}
