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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

/**
 * Merged suite: these tests only need the shared fixture provided by the base class, so they are
 * kept in one class on purpose. Every extra test class pays a full FE startup, which dominates the
 * runtime of tests this small.
 *
 * <p>Replaces the former standalone classes:
 * <ul>
 *   <li>BuildStructInfoTest</li>
 *   <li>EliminateJoinTest</li>
 *   <li>HyperGraphAggTest</li>
 *   <li>HyperGraphComparatorTest</li>
 *   <li>NullRejectInferenceTest</li>
 * </ul>
 */
public class MvExplorationSuiteTest extends SqlTestBase {

    private static final TestMaterializedViewRule TEST_RULE = new TestMaterializedViewRule();

    // -------------------------------------------------------------------------
    // from BuildStructInfoTest
    // -------------------------------------------------------------------------

    @Test
    void testSimpleSQL() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T1, T2, T3, T4 "
                + "where "
                + "T1.id = T2.id and "
                + "T2.score = T3.score and "
                + "T3.id = T4.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .matches(logicalJoin()
                        .when(j -> {
                            HyperGraph.builderForMv(j);
                            return true;
                        }));

    }

    @Test
    void testStructInfoNode() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T1 inner join "
                + "(select sum(id) as id from T2 where id = 1) T2 "
                + "on T1.id = T2.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .matches(logicalJoin()
                        .when(j -> {
                            HyperGraph hyperGraph = HyperGraph.builderForMv(j).build();
                            Assertions.assertTrue(hyperGraph.getNodes().stream()
                                    .allMatch(n -> n.getPlan()
                                            .collectToList(GroupPlan.class::isInstance).isEmpty()));
                            return true;
                        }));

    }

    @Test
    void testFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T1 left outer join "
                + " (select id from T2 where id = 1) T2 "
                + "on T1.id = T2.id ";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin()
                        .when(j -> {
                            HyperGraph structInfo = HyperGraph.builderForMv(j).build();
                            Assertions.assertTrue(structInfo.getJoinEdge(0).getJoinType().isLeftOuterJoin());
                            Assertions.assertEquals(0, structInfo.getFilterEdge(0).getLeftRejectEdge().size());
                            Assertions.assertEquals(1, structInfo.getFilterEdge(0).getRightRejectEdge().size());
                            return true;
                        }));

        sql = "select * from (select id from T1 where id = 0) T1 left outer join T2 "
                + "on T1.id = T2.id ";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin()
                        .when(j -> {
                            HyperGraph structInfo = HyperGraph.builderForMv(j).build();
                            Assertions.assertTrue(structInfo.getJoinEdge(0).getJoinType().isLeftOuterJoin());
                            return true;
                        }));
    }

    @Test
    void testPlanCheckerWithJoin() {
        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE"
                        + ",PRUNE_EMPTY_PARTITION"
                        + ",ELIMINATE_GROUP_BY_KEY_BY_UNIFORM"
                        + ",ELIMINATE_CONST_JOIN_CONDITION"
                        + ",CONSTANT_PROPAGATION"
        );
        PlanChecker.from(connectContext)
                .checkExplain("select * from "
                                + "(select * from lineitem "
                                + "where l_shipdate >= \"2023-12-01\" and l_shipdate <= \"2023-12-03\") t1 "
                                + "left join "
                                + "(select * from orders where o_orderdate >= \"2023-12-01\" and o_orderdate <= \"2023-12-03\" ) t2 "
                                + "on t1.l_orderkey = o_orderkey;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            PlanCheckContext checkContext = PlanCheckContext.of(
                                    AbstractMaterializedViewRule.SUPPORTED_JOIN_TYPE_SET);
                            Boolean result = rewrittenPlan.child(0).accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext);
                            Assertions.assertTrue(result);
                            Assertions.assertFalse(checkContext.isContainsTopAggregate());
                        });
    }

    @Test
    void testPlanCheckerWithAggregate() {
        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE"
                        + ",PRUNE_EMPTY_PARTITION"
                        + ",ELIMINATE_GROUP_BY_KEY_BY_UNIFORM"
                        + ",ELIMINATE_CONST_JOIN_CONDITION"
                        + ",CONSTANT_PROPAGATION"
        );
        PlanChecker.from(connectContext)
                .checkExplain("SELECT l.L_SHIPDATE AS ship_data_alias, o.O_ORDERDATE, count(*) "
                                + "FROM "
                                + "lineitem as l "
                                + "LEFT JOIN "
                                + "(SELECT abs(O_TOTALPRICE + 10) as c1_abs, O_CUSTKEY, O_ORDERDATE, O_ORDERKEY "
                                + "FROM orders) as o "
                                + "ON l.L_ORDERKEY = o.O_ORDERKEY "
                                + "JOIN "
                                + "(SELECT abs(sqrt(PS_SUPPLYCOST)) as c2_abs, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY "
                                + "FROM partsupp) as ps "
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY "
                                + "GROUP BY l.L_SHIPDATE, o.O_ORDERDATE ",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            PlanCheckContext checkContext = PlanCheckContext.of(
                                    AbstractMaterializedViewRule.SUPPORTED_JOIN_TYPE_SET);
                            Boolean result = rewrittenPlan.child(0).accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext);
                            Assertions.assertTrue(result);
                            Assertions.assertTrue(checkContext.isContainsTopAggregate());
                        });
    }

    @Test
    void testPlanCheckerScanAggregate() {
        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE"
                        + ",PRUNE_EMPTY_PARTITION"
                        + ",ELIMINATE_GROUP_BY_KEY_BY_UNIFORM"
                        + ",ELIMINATE_CONST_JOIN_CONDITION"
                        + ",CONSTANT_PROPAGATION"
        );
        PlanChecker.from(connectContext)
                .checkExplain("select l.L_SHIPDATE, count(*) from lineitem l "
                                + "GROUP BY l.L_SHIPDATE",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            PlanCheckContext checkContext = PlanCheckContext.of(
                                    AbstractMaterializedViewRule.SUPPORTED_JOIN_TYPE_SET);
                            Boolean result = rewrittenPlan.child(0).accept(StructInfo.SCAN_PLAN_PATTERN_CHECKER, checkContext);
                            Assertions.assertFalse(result);
                        });
    }

    @Test
    void testPlanCheckerOnlyScan() {
        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE"
                        + ",PRUNE_EMPTY_PARTITION"
                        + ",ELIMINATE_GROUP_BY_KEY_BY_UNIFORM"
                        + ",ELIMINATE_CONST_JOIN_CONDITION"
                        + ",CONSTANT_PROPAGATION"
        );
        PlanChecker.from(connectContext)
                .checkExplain("select l.L_SHIPDATE from lineitem l ",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            PlanCheckContext checkContext = PlanCheckContext.of(
                                    AbstractMaterializedViewRule.SUPPORTED_JOIN_TYPE_SET);
                            Boolean result = rewrittenPlan.child(0).accept(StructInfo.SCAN_PLAN_PATTERN_CHECKER, checkContext);
                            Assertions.assertTrue(result);
                            Assertions.assertFalse(checkContext.isContainsTopAggregate());
                        });
    }

    @Test
    void testPartitionUnionKeepsGlobalTopN() {
        Plan queryPlan = PlanChecker.from(connectContext)
                .analyze("select id from T1 order by id limit 2 offset 1")
                .rewrite()
                .getPlan().child(0);
        Assertions.assertTrue(queryPlan instanceof LogicalTopN);

        Plan compensatedPlan = TEST_RULE.buildPartitionCompensationPlan(queryPlan, queryPlan, queryPlan);
        Assertions.assertTrue(compensatedPlan instanceof LogicalTopN);
        LogicalTopN<?> globalTopN = (LogicalTopN<?>) compensatedPlan;
        Assertions.assertEquals(2, globalTopN.getLimit());
        Assertions.assertEquals(1, globalTopN.getOffset());
        Assertions.assertTrue(globalTopN.child() instanceof LogicalUnion);
        LogicalUnion union = (LogicalUnion) globalTopN.child();
        Assertions.assertEquals(2, union.children().size());
        Assertions.assertFalse(union.child(0) instanceof LogicalTopN);
        Assertions.assertFalse(union.child(1) instanceof LogicalTopN);
        Assertions.assertEquals(globalTopN.getOrderKeys().get(0).getExpr(), union.getOutput().get(0));
    }

    @Test
    void testPartitionUnionKeepsGlobalLimit() {
        Plan queryPlan = PlanChecker.from(connectContext)
                .analyze("select id from T1 limit 2 offset 1")
                .rewrite()
                .getPlan().child(0);
        Assertions.assertTrue(queryPlan instanceof LogicalLimit);

        Plan compensatedPlan = TEST_RULE.buildPartitionCompensationPlan(queryPlan, queryPlan, queryPlan);
        Assertions.assertTrue(compensatedPlan instanceof LogicalLimit);
        LogicalLimit<?> globalLimit = (LogicalLimit<?>) compensatedPlan;
        Assertions.assertEquals(2, globalLimit.getLimit());
        Assertions.assertEquals(1, globalLimit.getOffset());
        Assertions.assertEquals(LimitPhase.GLOBAL, globalLimit.getPhase());
        Assertions.assertTrue(globalLimit.child() instanceof LogicalUnion);
        LogicalUnion union = (LogicalUnion) globalLimit.child();
        Assertions.assertEquals(2, union.children().size());
        Assertions.assertTrue(union.child(0) instanceof LogicalLimit);
        Assertions.assertTrue(union.child(1) instanceof LogicalLimit);
        Assertions.assertEquals(3, ((LogicalLimit<?>) union.child(0)).getLimit());
        Assertions.assertEquals(0, ((LogicalLimit<?>) union.child(0)).getOffset());
        Assertions.assertEquals(LimitPhase.LOCAL, ((LogicalLimit<?>) union.child(0)).getPhase());
        Assertions.assertEquals(3, ((LogicalLimit<?>) union.child(1)).getLimit());
        Assertions.assertEquals(0, ((LogicalLimit<?>) union.child(1)).getOffset());
        Assertions.assertEquals(LimitPhase.LOCAL, ((LogicalLimit<?>) union.child(1)).getPhase());
    }

    @Test
    void testPartitionUnionKeepsProjectsAboveGlobalTopN() {
        Plan topN = PlanChecker.from(connectContext)
                .analyze("select id from T1 order by id limit 2 offset 1")
                .rewrite()
                .getPlan().child(0);
        LogicalProject<Plan> innerProject = new LogicalProject<>(ImmutableList.of(topN.getOutput().get(0)), topN);
        LogicalProject<Plan> queryPlan = new LogicalProject<>(
                ImmutableList.of(innerProject.getOutput().get(0)), innerProject);

        Plan compensatedPlan = TEST_RULE.buildPartitionCompensationPlan(queryPlan, queryPlan, queryPlan);
        Assertions.assertTrue(compensatedPlan instanceof LogicalProject);
        Assertions.assertEquals(queryPlan.getOutput(), compensatedPlan.getOutput());
        Assertions.assertTrue(compensatedPlan.child(0) instanceof LogicalProject);
        Assertions.assertTrue(compensatedPlan.child(0).child(0) instanceof LogicalTopN);
        Assertions.assertTrue(compensatedPlan.child(0).child(0).child(0) instanceof LogicalUnion);
    }

    @Test
    void testPartitionUnionRejectsAdjustedTopNOffset() {
        Plan queryPlan = PlanChecker.from(connectContext)
                .analyze("select id from T1 order by id limit 2 offset 1")
                .rewrite()
                .getPlan().child(0);
        LogicalTopN<?> queryTopN = (LogicalTopN<?>) queryPlan;

        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(
                queryTopN.withLimitChild(queryTopN.getLimit(), 0, queryTopN.child()), queryPlan, queryPlan));
        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(
                queryPlan, queryTopN.withLimitChild(queryTopN.getLimit(), 0, queryTopN.child()), queryPlan));
    }

    @Test
    void testPartitionUnionRejectsMismatchedTopNShape() {
        Plan queryPlan = PlanChecker.from(connectContext)
                .analyze("select id from T1 order by id limit 2")
                .rewrite()
                .getPlan().child(0);

        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(
                queryPlan.child(0), queryPlan, queryPlan));
    }

    @Test
    void testPartitionUnionRejectsUnexpectedGlobalOperatorInBranches() {
        Plan queryPlan = PlanChecker.from(connectContext)
                .analyze("select id from T1")
                .rewrite()
                .getPlan().child(0);
        Plan planWithGlobalLimit = new LogicalLimit<>(3, 0, LimitPhase.GLOBAL, queryPlan);

        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(
                planWithGlobalLimit, queryPlan, queryPlan));
        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(
                queryPlan, planWithGlobalLimit, queryPlan));
    }

    @Test
    void testPartitionUnionRejectsMultipleGlobalLimits() {
        Plan scan = PlanChecker.from(connectContext)
                .analyze("select id from T1")
                .rewrite()
                .getPlan().child(0);
        Plan innerLimit = new LogicalLimit<>(3, 0, LimitPhase.GLOBAL, scan);
        Plan queryPlan = new LogicalLimit<>(2, 0, LimitPhase.GLOBAL, innerLimit);

        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(queryPlan, queryPlan, queryPlan));
    }

    @Test
    void testPartitionUnionRejectsMixedGlobalOperatorsInBranches() {
        Plan queryPlan = PlanChecker.from(connectContext)
                .analyze("select id from T1 order by id limit 2")
                .rewrite()
                .getPlan().child(0);
        Plan globalLimit = new LogicalLimit<>(3, 0, LimitPhase.GLOBAL, queryPlan.child(0));
        Plan planWithGlobalLimit = queryPlan.withChildren(globalLimit);

        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(globalLimit, queryPlan, queryPlan));
        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(queryPlan, globalLimit, queryPlan));
        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(
                planWithGlobalLimit, queryPlan, queryPlan));
        Assertions.assertNull(TEST_RULE.buildPartitionCompensationPlan(
                queryPlan, planWithGlobalLimit, queryPlan));
    }

    // -------------------------------------------------------------------------
    // from EliminateJoinTest
    // -------------------------------------------------------------------------

    @Test
    void testLOJWithGroupBy() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join (select id from T2 group by id) T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        CascadesContext c3 = createCascadesContext(
                "select * from T1 left outer join (select id as id2 from T2 group by id) T2 "
                        + "on T1.id = T2.id2 ",
                connectContext
        );
        Plan p3 = PlanChecker.from(c3)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        HyperGraph h3 = HyperGraph.builderForMv(p3).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(!HyperGraphComparator.isLogicCompatible(h1, h3,
                constructContext(p1, p2, c1)).isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
    }

    @Test
    void testLOJWithUK() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint uk unique (id)");
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        dropConstraint("alter table T2 drop constraint uk");
    }

    @Test
    void testLOJWithUKAndOtherJoinConjuncts() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint uk_other_join_conjunct unique (id)");
        try {
            CascadesContext c2 = createCascadesContext(
                    "select * from T1 left outer join T2 "
                            + "on T1.id = T2.id and T2.id = 1",
                    connectContext
            );
            Plan p2 = PlanChecker.from(c2)
                    .analyze()
                    .rewrite()
                    .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                    .getAllPlan().get(0).child(0);
            HyperGraph h1 = HyperGraph.builderForMv(p1).build();
            HyperGraph h2 = HyperGraph.builderForMv(p2).build();
            ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
            Assertions.assertFalse(res.isInvalid());
            Assertions.assertTrue(res.getViewExpressions().isEmpty());
        } finally {
            dropConstraint("alter table T2 drop constraint uk_other_join_conjunct");
        }
    }

    @Test
    void testLOJWithUKAndFilterOnEliminatedNode() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select T1.id from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint uk_loj_filter unique (id)");
        try {
            CascadesContext c2 = createCascadesContext(
                    "select T1.id from T1 left outer join T2 "
                            + "on T1.id = T2.id where T2.id is null",
                    connectContext
            );
            Plan p2 = PlanChecker.from(c2)
                    .analyze()
                    .rewrite()
                    .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                    .getAllPlan().get(0).child(0);
            HyperGraph h1 = HyperGraph.builderForMv(p1).build();
            HyperGraph h2 = HyperGraph.builderForMv(p2).build();
            ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
            Assertions.assertTrue(res.isInvalid());
        } finally {
            dropConstraint("alter table T2 drop constraint uk_loj_filter");
        }
    }

    @Test
    void testInnerJoinWithPKFKAndSlotFreeFilterOnEliminatedNode() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk_slot_free_filter primary key (id)");
        addConstraint("alter table T1 add constraint fk_slot_free_filter foreign key (id) references T2(id)");
        try {
            CascadesContext c2 = createCascadesContext(
                    "select * from T1 inner join (select * from T2 where 1 = 0) T2 "
                            + "on T1.id = T2.id",
                    connectContext
            );
            Plan p2 = PlanChecker.from(c2)
                    .analyze()
                    .rewrite()
                    .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                    .getAllPlan().get(0).child(0);
            HyperGraph h1 = HyperGraph.builderForMv(p1).build();
            HyperGraph h2 = HyperGraph.builderForMv(p2).build();
            ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
            Assertions.assertTrue(res.isInvalid());
        } finally {
            dropConstraint("alter table T1 drop constraint fk_slot_free_filter");
            dropConstraint("alter table T2 drop constraint pk_slot_free_filter");
        }
    }

    @Test
    void testLOJWithPKFK() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk primary key (id)");
        addConstraint("alter table T1 add constraint fk foreign key (id) references T2(id)");
        CascadesContext c2 = createCascadesContext(
                "select * from T1 inner join T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        CascadesContext c3 = createCascadesContext(
                "select * from T1 inner join (select id as id2 from T2) T2 "
                        + "on T1.id = T2.id2 ",
                connectContext
        );
        Plan p3 = PlanChecker.from(c3)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        HyperGraph h3 = HyperGraph.builderForMv(p3).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        Assertions.assertTrue(!HyperGraphComparator.isLogicCompatible(h1, h3, constructContext(p1, p2, c1)).isInvalid());
        dropConstraint("alter table T2 drop constraint pk");
    }

    @Test
    void testInnerJoinWithPKFKAndMultiNodeResidualFilter() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk primary key (id)");
        addConstraint("alter table T1 add constraint fk foreign key (id) references T2(id)");
        CascadesContext c2 = createCascadesContext(
                "select * from T1 inner join T2 "
                        + "on T1.id = T2.id where T1.score > T2.score",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(res.isInvalid());
        dropConstraint("alter table T2 drop constraint pk");
    }

    @Disabled
    @Test
    void testLOJWithPKFKAndUK1() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk primary key (id)");
        addConstraint("alter table T1 add constraint fk foreign key (id) references T2(id)");
        addConstraint("alter table T3 add constraint uk unique (id)");
        CascadesContext c2 = createCascadesContext(
                "select * from (select T1.*, T3.id as id3 from T1 left outer join T3 on T1.id = T3.id) T1 inner join T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        dropConstraint("alter table T2 drop constraint pk");
        dropConstraint("alter table T3 drop constraint uk");
    }

    @Test
    void testLOJWithPKFKAndUK2() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        addConstraint("alter table T2 add constraint pk primary key (id)");
        addConstraint("alter table T1 add constraint fk foreign key (id) references T2(id)");
        addConstraint("alter table T3 add constraint uk unique (id)");
        CascadesContext c2 = createCascadesContext(
                "select * from (select T1.*, T2.id as id2 from T1 inner join T2 on T1.id = T2.id) T1 left outer join T3 "
                        + "on T1.id = T3.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertTrue(res.getViewExpressions().isEmpty());
        dropConstraint("alter table T2 drop constraint pk");
        dropConstraint("alter table T3 drop constraint uk");
    }

    // -------------------------------------------------------------------------
    // from HyperGraphAggTest
    // -------------------------------------------------------------------------

    @Test
    void testJoinWithAgg() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        CascadesContext c2 = createCascadesContext(
                "select * from T1 inner join"
                        + "("
                        + "select id from T2 group by id"
                        + ") T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p2).build();
        Assertions.assertEquals("id", Objects.requireNonNull(((StructInfoNode) h1.getNode(1)).getExpressions()).get(0).toSql());
    }

    @Disabled
    @Test
    void testIJWithAgg() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T2 "
                        + "on T1.id = T2.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 inner join"
                        + "("
                        + "select id from T2 group by id"
                        + ") T2 "
                        + "on T1.id = T2.id ",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2));
        Assertions.assertTrue(!res.isInvalid());
        Assertions.assertEquals(2, res.getViewNoNullableSlot().size());
    }

    LogicalCompatibilityContext constructContext(Plan p1, Plan p2) {
        StructInfo st1 = StructInfo.of(p1, p1, null);
        StructInfo st2 = StructInfo.of(p2, p2, null);
        RelationMapping rm = RelationMapping.generate(st1.getRelations(), st2.getRelations(), 8)
                .get(0);
        SlotMapping sm = SlotMapping.generate(rm);
        return LogicalCompatibilityContext.from(rm, sm, st1, st2);
    }

    // -------------------------------------------------------------------------
    // from HyperGraphComparatorTest
    // -------------------------------------------------------------------------

    @Override
    protected String getDisableNereidsRules() {
        return "INFER_PREDICATES,CONSTANT_PROPAGATION,PRUNE_EMPTY_PARTITION";
    }

    @Test
    void testInnerJoinAndLOJ() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join T2 "
                        + "on T1.id = T2.id "
                        + "left outer join T3 on T1.id = T3.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals(2, res.getViewNoNullableSlot().size());
    }

    @Test
    void testIJAndLojAssoc() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T3 "
                        + "on T1.id = T3.id "
                        + "inner join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join T2 "
                        + "on T1.id = T2.id "
                        + "left outer join T3 on T1.id = T3.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals(2, res.getViewNoNullableSlot().size());
    }

    @Test
    void testIJAndLojAssocWithFilter() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T3 "
                        + "on T1.id = T3.id "
                        + "inner join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join "
                        + "(select * from T2 where T2.id = 1) T2 "
                        + "on T1.id = T2.id "
                        + "left outer join T3 on T1.id = T3.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals(2, res.getViewNoNullableSlot().size());
    }

    @Test
    void testIJAndLojAssocWithJoinCond() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T3 "
                        + "on T1.id = T3.id "
                        + "inner join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left join T2 "
                        + "on T1.id = T2.id "
                        + "left join T3 on T1.id = T3.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals(2, res.getViewNoNullableSlot().size());
    }

    @Test
    void testJoinEliminateShouldFail() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T2 "
                        + "on T1.id = T2.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 inner join T2 "
                        + "on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(res.isInvalid());
    }

    // -------------------------------------------------------------------------
    // from NullRejectInferenceTest
    // -------------------------------------------------------------------------

    @Test
    void testTwoHopNullRejectFromInnerJoinConditions() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext queryContext = createCascadesContext(
                "select lineitem.l_orderkey, supplier.s_name, nation.n_name from lineitem "
                        + "inner join supplier on lineitem.l_suppkey = supplier.s_suppkey "
                        + "inner join nation on supplier.s_nationkey = nation.n_nationkey "
                        + "where nation.n_name = 'CHINA'",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        CascadesContext viewContext = createCascadesContext(
                "select lineitem.l_orderkey, supplier.s_name, nation.n_name from lineitem "
                        + "left outer join supplier on lineitem.l_suppkey = supplier.s_suppkey "
                        + "left outer join nation on supplier.s_nationkey = nation.n_nationkey",
                connectContext
        );
        Plan viewPlan = PlanChecker.from(viewContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        StructInfo queryStructInfo = StructInfo.of(queryPlan, queryPlan, queryContext);
        StructInfo viewStructInfo = StructInfo.of(viewPlan, viewPlan, viewContext);
        RelationMapping relationMapping = RelationMapping.generate(
                queryStructInfo.getRelations(), viewStructInfo.getRelations(), 8).get(0);
        SlotMapping queryToView = SlotMapping.generate(relationMapping);
        SlotMapping viewToQuery = queryToView.inverse();
        LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                relationMapping, viewToQuery, queryStructInfo, viewStructInfo);
        ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(
                queryStructInfo, viewStructInfo, compatibilityContext);

        Assertions.assertFalse(comparisonResult.isInvalid());
        Assertions.assertFalse(comparisonResult.getViewNoNullableSlot().isEmpty());

        SplitPredicate compensatePredicates = TEST_RULE.predicatesCompensateForTest(
                queryStructInfo, viewStructInfo, viewToQuery, comparisonResult, queryContext);
        Assertions.assertFalse(compensatePredicates.isInvalid());
        Assertions.assertTrue(compensatePredicates.toList().stream()
                .anyMatch(expression -> isNotNullOnSlot(expression, "s_name")));
    }

    @Test
    void testNullRejectCompensationForInnerJoinFullJoinRewrite() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext queryContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "inner join orders on lineitem.l_orderkey = orders.o_orderkey "
                        + "where orders.o_orderdate = '2023-10-17'",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        CascadesContext viewContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "full outer join orders on lineitem.l_orderkey = orders.o_orderkey",
                connectContext
        );
        Plan viewPlan = PlanChecker.from(viewContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        StructInfo queryStructInfo = StructInfo.of(queryPlan, queryPlan, queryContext);
        StructInfo viewStructInfo = StructInfo.of(viewPlan, viewPlan, viewContext);
        RelationMapping relationMapping = RelationMapping.generate(
                queryStructInfo.getRelations(), viewStructInfo.getRelations(), 8).get(0);
        SlotMapping queryToView = SlotMapping.generate(relationMapping);
        SlotMapping viewToQuery = queryToView.inverse();
        LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                relationMapping, viewToQuery, queryStructInfo, viewStructInfo);
        ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(
                queryStructInfo, viewStructInfo, compatibilityContext);

        Assertions.assertFalse(comparisonResult.isInvalid());
        Assertions.assertFalse(comparisonResult.getViewNoNullableSlot().isEmpty());

        SplitPredicate compensatePredicates = TEST_RULE.predicatesCompensateForTest(
                queryStructInfo, viewStructInfo, viewToQuery, comparisonResult, queryContext);
        Assertions.assertFalse(compensatePredicates.isInvalid());
        Assertions.assertTrue(compensatePredicates.toList().stream()
                .anyMatch(expression -> isNotNullOnSlot(expression, "l_shipdate")));
    }

    @Test
    void testNullRejectCompensationForInnerJoinFullJoinRewriteOnRightSide() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext queryContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "inner join orders on lineitem.l_orderkey = orders.o_orderkey "
                        + "where lineitem.l_shipdate = '2023-10-17'",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        CascadesContext viewContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "full outer join orders on lineitem.l_orderkey = orders.o_orderkey",
                connectContext
        );
        Plan viewPlan = PlanChecker.from(viewContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        StructInfo queryStructInfo = StructInfo.of(queryPlan, queryPlan, queryContext);
        StructInfo viewStructInfo = StructInfo.of(viewPlan, viewPlan, viewContext);
        RelationMapping relationMapping = RelationMapping.generate(
                queryStructInfo.getRelations(), viewStructInfo.getRelations(), 8).get(0);
        SlotMapping queryToView = SlotMapping.generate(relationMapping);
        SlotMapping viewToQuery = queryToView.inverse();
        LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                relationMapping, viewToQuery, queryStructInfo, viewStructInfo);
        ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(
                queryStructInfo, viewStructInfo, compatibilityContext);

        Assertions.assertFalse(comparisonResult.isInvalid());
        Assertions.assertFalse(comparisonResult.getViewNoNullableSlot().isEmpty());

        SplitPredicate compensatePredicates = TEST_RULE.predicatesCompensateForTest(
                queryStructInfo, viewStructInfo, viewToQuery, comparisonResult, queryContext);
        Assertions.assertFalse(compensatePredicates.isInvalid());
        Assertions.assertTrue(compensatePredicates.toList().stream()
                .anyMatch(expression -> isNotNullOnSlot(expression, "o_orderdate")));
    }

    private static boolean isNotNullOnSlot(Expression expression, String slotName) {
        if (!(expression instanceof Not) || ((Not) expression).isGeneratedIsNotNull()
                || !(((Not) expression).child() instanceof IsNull)) {
            return false;
        }
        Expression slot = ((IsNull) ((Not) expression).child()).child();
        return slot instanceof SlotReference && slotName.equals(((SlotReference) slot).getName());
    }

    private static class TestMaterializedViewRule extends AbstractMaterializedViewRule {
        @Override
        public List<Rule> buildRules() {
            return ImmutableList.of();
        }

        private SplitPredicate predicatesCompensateForTest(StructInfo queryStructInfo,
                StructInfo viewStructInfo, SlotMapping viewToQuerySlotMapping,
                ComparisonResult comparisonResult, CascadesContext cascadesContext) {
            return predicatesCompensate(queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                    comparisonResult, cascadesContext);
        }
    }
}
