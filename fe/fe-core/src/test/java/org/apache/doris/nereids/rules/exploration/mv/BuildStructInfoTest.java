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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BuildStructInfoTest extends SqlTestBase {

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
}
