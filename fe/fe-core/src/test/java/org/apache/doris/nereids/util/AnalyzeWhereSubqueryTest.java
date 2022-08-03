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
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.logical.ApplyPullFilterOnAgg;
import org.apache.doris.nereids.rules.rewrite.logical.ApplyPullFilterOnProjectUnderAgg;
import org.apache.doris.nereids.rules.rewrite.logical.ExistsApplyToJoin;
import org.apache.doris.nereids.rules.rewrite.logical.InApplyToJoin;
import org.apache.doris.nereids.rules.rewrite.logical.PushApplyUnderFilter;
import org.apache.doris.nereids.rules.rewrite.logical.PushApplyUnderProject;
import org.apache.doris.nereids.rules.rewrite.logical.ScalarApplyToJoin;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Max;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class AnalyzeWhereSubqueryTest extends TestWithFeService implements PatternMatchSupported {
    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = ImmutableList.of(
            "scalar",
            "select * from t6 where t6.k1 < (select sum(t7.k3) from t7 where t7.v2 = t6.k2)",
            "select * from t6 where t6.k1 = (select sum(t7.k3) from t7 where t7.v2 = t6.k2)",
            "select * from t6 where t6.k1 != (select sum(t7.k3) from t7 where t7.v2 = t6.k2)",
            "in",
            "select * from t6 where t6.k1 in (select t7.k3 from t7 where t7.v2 = t6.k2)",
            "select * from t6 where t6.k1 not in (select t7.k3 from t7 where t7.v2 = t6.k2)",
            "exists",
            "select * from t6 where exists (select t7.k3 from t7 where t6.k2 = t7.v2)",
            "select * from t6 where not exists (select t7.k3 from t7 where t6.k2 = t7.v2)",
            "in (in and scalar)",
            "select * from t6 where t6.k1 in ("
                    + "select t7.k3 from t7 where "
                    + "t7.k3 in (select t8.k1 from t8 where t8.k1 = 3) "
                    + "and t7.v2 > (select sum(t9.k2) from t9 where t9.k2 = t7.v1))",
            "exists and not exists",
            "select * from t6 where exists (select t7.k3 from t7 where t6.k2 = t7.v2) and not exists (select t8.k2 from t8 where t6.k2 = t8.k2)",
            "with subquery alias",
            "select * from t6 where t6.k1 < (select max(aa) from (select v1 as aa from t7 where t6.k2=t7.v2) t2 )"
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "create table test.t6\n"
                        + "(k1 bigint, k2 bigint)\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.t7\n"
                        + "(k1 bigint not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.t8\n"
                        + "(k1 int, k2 int)\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.t9\n"
                        + "(k1 varchar(40), k2 bigint)\n"
                        + "partition by range(k2)\n"
                        + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    public void testTranslateCase() throws Exception {
        for (String sql : testSql) {
            if (!sql.contains("select")) {
                continue;
            }
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
    public void testWhereSql2AfterAnalyzed() {
        //after analyze
        PlanChecker.from(connectContext)
                .analyze(testSql.get(2))
                .matches(
                        logicalApply(
                                any(),
                                logicalAggregate(
                                        logicalFilter()
                                ).when(FieldChecker.check("outputExpressions", ImmutableList.of(
                                        new Alias(new ExprId(7),
                                                new Sum(
                                                        new SlotReference(new ExprId(4), "k3",
                                                                new BigIntType(), true,
                                                                ImmutableList.of("default_cluster:test", "t7"))),
                                                "sum(k3)"))))
                        ).when(FieldChecker.check("correlationSlot", ImmutableList.of(
                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6"))
                        )))
                );
    }

    @Test
    public void testWhereSql2AfterAggFilterRule() {
        //after aggFilter rule
        PlanChecker.from(connectContext)
                .analyze(testSql.get(2))
                .applyBottomUp(new ApplyPullFilterOnAgg())
                .matches(
                        logicalApply(
                                any(),
                                logicalAggregate().when(FieldChecker.check("outputExpressions", ImmutableList.of(
                                                new Alias(new ExprId(7), new Sum(
                                                        new SlotReference(new ExprId(4), "k3", new BigIntType(), true,
                                                                ImmutableList.of("default_cluster:test", "t7"))),
                                                        "sum(k3)"),
                                                new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t7"))
                                        )))
                                        .when(FieldChecker.check("groupByExpressions", ImmutableList.of(
                                                new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t7"))
                                        )))
                        ).when(FieldChecker.check("correlationSlot", ImmutableList.of(
                                        new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t6"))
                                )))
                                .when(FieldChecker.check("correlationFilter", Optional.of(
                                        new EqualTo(new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t7")),
                                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t6"))
                                        ))))
                );
    }

    @Test
    public void testWhereSql2AfterScalarToJoin() {
        //after Scalar CorrelatedJoin to join
        PlanChecker.from(connectContext)
                .analyze(testSql.get(2))
                .applyBottomUp(new ApplyPullFilterOnAgg())
                .applyBottomUp(new ScalarApplyToJoin())
                .matches(
                        logicalJoin(
                                any(),
                                logicalAggregate()
                        ).when(FieldChecker.check("joinType", JoinType.LEFT_OUTER_JOIN))
                                .when(FieldChecker.check("otherJoinCondition",
                                        Optional.of(new EqualTo(new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t7")),
                                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t6"))))))
                );
    }

    @Test
    public void testInSql5AfterAnalyze() {
        //"select * from t6 where t6.k1 in (select t7.k3 from t7 where t7.v2 = t6.k2)"
        PlanChecker.from(connectContext)
                .analyze(testSql.get(5))
                .matches(
                        logicalApply(
                                any(),
                                logicalProject().when(FieldChecker.check("projects", ImmutableList.of(
                                        new SlotReference(new ExprId(4), "k3", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t7"))))
                                )
                        ).when(FieldChecker.check("correlationSlot", ImmutableList.of(
                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")))))
                );
    }

    @Test
    public void testInSql5AfterPushProjectRule() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(5))
                .applyBottomUp(new PushApplyUnderProject())
                .matches(
                        logicalProject(
                                logicalApply().when(FieldChecker.check("correlationFilter", Optional.empty()))
                                        .when(FieldChecker.check("correlationSlot", ImmutableList.of(
                                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t6")))))
                        ).when(FieldChecker.check("projects", ImmutableList.of(
                                new SlotReference(new ExprId(0), "k1", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")),
                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")))))
                );
    }

    @Test
    public void testInSql5AfterPushFilterRule() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(5))
                .applyBottomUp(new PushApplyUnderProject())
                .applyBottomUp(new PushApplyUnderFilter())
                .matches(
                        logicalApply().when(FieldChecker.check("correlationFilter", Optional.of(
                                new EqualTo(new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t7")),
                                        new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t6"))))))
                );
    }

    @Test
    public void testInSql5AfterInToJoin() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(5))
                .applyBottomUp(new PushApplyUnderProject())
                .applyBottomUp(new PushApplyUnderFilter())
                .applyBottomUp(new InApplyToJoin())
                .matches(
                        logicalJoin().when(FieldChecker.check("joinType", JoinType.LEFT_SEMI_JOIN))
                                .when(FieldChecker.check("otherJoinCondition", Optional.of(
                                        new And(new EqualTo(new SlotReference(new ExprId(0), "k1", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t6")),
                                                new SlotReference(new ExprId(2), "k1", new BigIntType(), false,
                                                        ImmutableList.of("default_cluster:test", "t7"))),
                                                new EqualTo(new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t7")),
                                                        new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                                ImmutableList.of("default_cluster:test", "t6"))))
                                )))
                );
    }

    @Test
    public void testExistSql8AfterAnalyze() {
        //"select * from t6 where exists (select t7.k3 from t7 where t6.k2 = t7.v2)"
        PlanChecker.from(connectContext)
                .analyze(testSql.get(8))
                .matches(
                        logicalApply(
                                any(),
                                logicalProject().when(FieldChecker.check("projects", ImmutableList.of(
                                        new SlotReference(new ExprId(4), "k3", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t7"))))
                                )
                        ).when(FieldChecker.check("correlationSlot", ImmutableList.of(
                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")))))
                );
    }

    @Test
    public void testExistSql8AfterPushProjectRule() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(8))
                .applyBottomUp(new PushApplyUnderProject())
                .matches(
                        logicalProject(
                                logicalApply().when(FieldChecker.check("correlationFilter", Optional.empty()))
                                        .when(FieldChecker.check("correlationSlot", ImmutableList.of(
                                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t6")))))
                        ).when(FieldChecker.check("projects", ImmutableList.of(
                                new SlotReference(new ExprId(0), "k1", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")),
                                new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")))))
                );
    }

    @Test
    public void testExistSql8AfterPushFilterRule() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(8))
                .applyBottomUp(new PushApplyUnderProject())
                .applyBottomUp(new PushApplyUnderFilter())
                .matches(
                        logicalApply().when(FieldChecker.check("correlationFilter", Optional.of(
                                new EqualTo(new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")),
                                        new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t7"))))))
                );
    }

    @Test
    public void testExistSql8AfterInToJoin() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(8))
                .applyBottomUp(new PushApplyUnderProject())
                .applyBottomUp(new PushApplyUnderFilter())
                .applyBottomUp(new ExistsApplyToJoin())
                .matches(
                        logicalJoin().when(FieldChecker.check("joinType", JoinType.LEFT_SEMI_JOIN))
                                .when(FieldChecker.check("otherJoinCondition", Optional.of(
                                        new EqualTo(new SlotReference(new ExprId(1), "k2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t6")),
                                                new SlotReference(new ExprId(6), "v2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t7")))
                                )))
                );
    }

    @Test
    public void testSql15AfterAnalyze() {
        //select * from t6 where t6.k1 < (select max(aa) from (select v1 as aa from t7 where t6.k2=t7.v2) t2 )
        PlanChecker.from(connectContext)
                .analyze(testSql.get(15))
                .matches(
                        logicalApply(
                                any(),
                                logicalAggregate(
                                        logicalProject(
                                                logicalFilter()
                                        ).when(FieldChecker.check("projects", ImmutableList.of(
                                                new Alias(new ExprId(0), new SlotReference(new ExprId(6), "v1", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t7")), "aa")
                                        )))
                                ).when(FieldChecker.check("outputExpressions", ImmutableList.of(
                                        new Alias(new ExprId(8), new Max(new SlotReference(new ExprId(0), "aa",  new BigIntType(), true,
                                                ImmutableList.of("t2"))), "max(aa)")
                                )))
                                        .when(FieldChecker.check("groupByExpressions", ImmutableList.of()))
                        ).when(FieldChecker.check("correlationSlot", ImmutableList.of(
                                new SlotReference(new ExprId(2), "k2", new BigIntType(), true,
                                        ImmutableList.of("default_cluster:test", "t6")))))
                );
    }

    @Test
    public void testSql15AfterChangeProjectFilter() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(15))
                .applyBottomUp(new ApplyPullFilterOnProjectUnderAgg())
                .matches(
                        logicalApply(
                                any(),
                                logicalAggregate(
                                        logicalFilter(
                                                logicalProject().when(FieldChecker.check("projects", ImmutableList.of(
                                                        new Alias(new ExprId(0), new SlotReference(new ExprId(6), "v1", new BigIntType(), true,
                                                                ImmutableList.of("default_cluster:test", "t7")), "aa"),
                                                        new SlotReference(new ExprId(3), "k1", new BigIntType(), false,
                                                                ImmutableList.of("default_cluster:test", "t7")),
                                                        new SlotReference(new ExprId(4), "k2", new VarcharType(128), true,
                                                                ImmutableList.of("default_cluster:test", "t7")),
                                                        new SlotReference(new ExprId(5), "k3", new BigIntType(), true,
                                                                ImmutableList.of("default_cluster:test", "t7")),
                                                        new SlotReference(new ExprId(6), "v1", new BigIntType(), true,
                                                                        ImmutableList.of("default_cluster:test", "t7")),
                                                        new SlotReference(new ExprId(7), "v2", new BigIntType(), true,
                                                                ImmutableList.of("default_cluster:test", "t7"))
                                                )))
                                        )
                                )
                        )
                );
    }

    @Test
    public void testSql15AfterChangeAggFilter() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(15))
                .applyBottomUp(new ApplyPullFilterOnProjectUnderAgg())
                .applyBottomUp(new ApplyPullFilterOnAgg())
                .matches(
                        logicalApply(
                                any(),
                                logicalAggregate(
                                        logicalProject()
                                ).when(FieldChecker.check("outputExpressions", ImmutableList.of(
                                        new Alias(new ExprId(8), new Max(new SlotReference(new ExprId(0), "aa",  new BigIntType(), true,
                                                ImmutableList.of("t2"))), "max(aa)"),
                                        new SlotReference(new ExprId(7), "v2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t7")))))
                                        .when(FieldChecker.check("groupByExpressions", ImmutableList.of(
                                                new SlotReference(new ExprId(7), "v2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t7"))
                                        )))
                        )
                );
    }

    @Test
    public void testSql15AfterScalarTOJoin() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(15))
                .applyBottomUp(new ApplyPullFilterOnProjectUnderAgg())
                .applyBottomUp(new ApplyPullFilterOnAgg())
                .applyBottomUp(new ScalarApplyToJoin())
                .matches(
                        logicalJoin(
                                any(),
                                logicalAggregate(
                                        logicalProject()
                                )
                        ).when(FieldChecker.check("joinType", JoinType.LEFT_OUTER_JOIN))
                                .when(FieldChecker.check("otherJoinCondition", Optional.of(
                                        new EqualTo(new SlotReference(new ExprId(2), "k2", new BigIntType(), true,
                                                ImmutableList.of("default_cluster:test", "t6")),
                                                new SlotReference(new ExprId(7), "v2", new BigIntType(), true,
                                                        ImmutableList.of("default_cluster:test", "t7")))
                                )))
                );
    }
}
