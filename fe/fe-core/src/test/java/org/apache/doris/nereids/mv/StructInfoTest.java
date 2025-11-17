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

package org.apache.doris.nereids.mv;

import org.apache.doris.nereids.rules.exploration.mv.AbstractMaterializedViewJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PredicateCollectorContext;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test the methods in StructInfo.
 */
public class StructInfoTest extends SqlTestBase {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("struct_info_test");
        useDatabase("struct_info_test");
        createTables("CREATE TABLE IF NOT EXISTS orders_arr  (\n"
                + "  o_orderkey       INTEGER NOT NULL,\n"
                + "  o_orderstatus    CHAR(1) NOT NULL,\n"
                + "  o_totalprice     DECIMALV3(15,2) NOT NULL,\n"
                + "  o_custkey        INTEGER NOT NULL,\n"
                + "  o_orderdate      DATE NOT NULL,\n"
                + "  o_orderpriority  CHAR(15) NOT NULL,  \n"
                + "  o_shippriority   INTEGER NOT NULL,\n"
                + "  o_comment        VARCHAR(79) NOT NULL,\n"
                + "  o_array1 ARRAY<int(11)> NULL,\n"
                + "  o_array2 ARRAY<int(11)> NULL\n"
                + ")\n"
                + "DUPLICATE KEY(o_orderkey, o_orderstatus)\n"
                + "DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ");");
        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE"
                        + ",PRUNE_EMPTY_PARTITION"
                        + ",ELIMINATE_GROUP_BY_KEY_BY_UNIFORM"
                        + ",ELIMINATE_CONST_JOIN_CONDITION"
                        + ",CONSTANT_PROPAGATION"
        );
    }

    @Test
    public void testPlanPatternCheckerWindowAboveAgg() {
        PlanChecker.from(connectContext)
                .checkExplain("select \n"
                                + "o_orderkey,\n"
                                + "FIRST_VALUE(o_custkey) OVER (\n"
                                + "        PARTITION BY o_orderdate \n"
                                + "        ORDER BY o_totalprice NULLS LAST\n"
                                + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                                + "    ) AS first_value,\n"
                                + "RANK() OVER (\n"
                                + "        PARTITION BY o_orderdate, o_orderstatus \n"
                                + "        ORDER BY o_totalprice NULLS LAST\n"
                                + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                                + "    ) AS rank_value,\n"
                                + "count(*)    \n"
                                + "from \n"
                                + "orders_arr\n"
                                + "group by o_orderkey,\n"
                                + "o_custkey,\n"
                                + "o_orderdate,\n"
                                + "o_orderstatus,\n"
                                + "o_totalprice;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            PlanCheckContext planCheckContext = new PlanCheckContext(
                                    AbstractMaterializedViewJoinRule.SUPPORTED_JOIN_TYPE_SET);
                            Boolean valid = rewrittenPlan.child(0).accept(
                                    StructInfo.PLAN_PATTERN_CHECKER, planCheckContext);
                            Assertions.assertTrue(valid);
                            Assertions.assertEquals(1, planCheckContext.getTopWindowNum());
                            Assertions.assertTrue(planCheckContext.isContainsTopWindow());
                            Assertions.assertFalse(planCheckContext.isWindowUnderAggregate());
                        });
    }

    @Test
    public void testPlanPatternCheckerWindowUnderAgg() {
        PlanChecker.from(connectContext)
                .checkExplain("select o_orderkey, first_value, count(*)\n"
                                + "from\n"
                                + "(\n"
                                + "select \n"
                                + "o_orderkey, o_orderdate,\n"
                                + "FIRST_VALUE(o_custkey) OVER (\n"
                                + "        PARTITION BY o_orderdate \n"
                                + "        ORDER BY o_totalprice NULLS LAST\n"
                                + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                                + "    ) AS first_value,\n"
                                + "RANK() OVER (\n"
                                + "        PARTITION BY o_orderdate, o_orderstatus \n"
                                + "        ORDER BY o_totalprice NULLS LAST\n"
                                + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                                + "    ) AS rank_value\n"
                                + "from \n"
                                + "orders_arr\n"
                                + ") t\n"
                                + "group by o_orderkey, first_value;",
                        nereidsPlanner -> {

                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            PlanCheckContext planCheckContext = new PlanCheckContext(
                                    AbstractMaterializedViewJoinRule.SUPPORTED_JOIN_TYPE_SET);
                            Boolean valid = rewrittenPlan.child(0).accept(
                                    StructInfo.PLAN_PATTERN_CHECKER, planCheckContext);
                            Assertions.assertFalse(valid);
                            Assertions.assertEquals(0, planCheckContext.getTopWindowNum());
                            Assertions.assertFalse(planCheckContext.isContainsTopWindow());
                            Assertions.assertTrue(planCheckContext.isWindowUnderAggregate());
                        });
    }

    @Test
    public void testCheckWindowTmpRewrittenPlanInValid() {
        PlanChecker.from(connectContext)
                .checkExplain("select o_orderkey, c2\n"
                                + "from (select o_orderkey from orders_arr limit 1) orders_a\n"
                                + "LATERAL VIEW explode_numbers(0) t1 as c2\n"
                                + "order by c2;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan().child(0);
                            Assertions.assertFalse(StructInfo.checkWindowTmpRewrittenPlanIsValid(rewrittenPlan));
                        });
    }

    @Test
    public void testCheckWindowTmpRewrittenPlanIsValid() {
        PlanChecker.from(connectContext)
                .checkExplain("select o_orderkey from orders_arr where o_orderkey > 1",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan().child(0);
                            Assertions.assertTrue(StructInfo.checkWindowTmpRewrittenPlanIsValid(rewrittenPlan));
                        });
    }

    @Test
    public void testPredicateCollectorWithCouldPullUp() {
        PlanChecker.from(connectContext)
                .checkExplain("select \n"
                                + "o_orderkey, o_orderdate,\n"
                                + "FIRST_VALUE(o_custkey) OVER (\n"
                                + "        PARTITION BY o_orderdate \n"
                                + "        ORDER BY o_totalprice NULLS LAST\n"
                                + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                                + "    ) AS first_value\n"
                                + "from \n"
                                + "(\n"
                                + "select * from orders_arr where o_orderdate > '2025-01-01' and o_custkey = 1\n"
                                + ") t;",
                        nereidsPlanner -> {

                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan().child(0);
                            PredicateCollectorContext predicateCollectorContext = new PredicateCollectorContext();
                            rewrittenPlan.accept(StructInfo.PREDICATE_COLLECTOR, predicateCollectorContext);
                            Assertions.assertEquals(1,
                                    predicateCollectorContext.getCouldPullUpPredicates().size());
                            Assertions.assertTrue(
                                    predicateCollectorContext.getCouldPullUpPredicates().stream().allMatch(
                                            expr -> expr.collectFirst(
                                                            node -> node instanceof SlotReference
                                                                    && ((SlotReference) node).getName()
                                                                    .equalsIgnoreCase("o_orderdate"))
                                                    .isPresent()));
                            Assertions.assertEquals(1,
                                    predicateCollectorContext.getCouldNotPullUpPredicates().size());
                            Assertions.assertTrue(
                                    predicateCollectorContext.getCouldNotPullUpPredicates().stream().allMatch(
                                            expr -> expr.collectFirst(
                                                            node -> node instanceof SlotReference
                                                                    && ((SlotReference) node).getName()
                                                                    .equalsIgnoreCase("o_custkey"))
                                                    .isPresent()));
                        });
    }

    @Test
    public void testPredicateCollectorWithCouldNotPullUp() {
        PlanChecker.from(connectContext)
                .checkExplain("select o_orderkey, first_value\n"
                                + "from\n"
                                + "(\n"
                                + "select \n"
                                + "o_orderkey, o_orderdate,\n"
                                + "FIRST_VALUE(o_custkey) OVER (\n"
                                + "        PARTITION BY o_orderdate \n"
                                + "        ORDER BY o_totalprice NULLS LAST\n"
                                + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                                + "    ) AS first_value,\n"
                                + "RANK() OVER (\n"
                                + "        PARTITION BY o_orderdate, o_orderstatus \n"
                                + "        ORDER BY o_totalprice NULLS LAST\n"
                                + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                                + "    ) AS rank_value\n"
                                + "from \n"
                                + "orders_arr\n"
                                + ") t\n"
                                + "where first_value > 0;",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan().child(0);
                            PredicateCollectorContext predicateCollectorContext = new PredicateCollectorContext();
                            rewrittenPlan.accept(StructInfo.PREDICATE_COLLECTOR, predicateCollectorContext);
                            Assertions.assertEquals(1,
                                    predicateCollectorContext.getCouldPullUpPredicates().size());
                            Assertions.assertEquals(0,
                                    predicateCollectorContext.getCouldNotPullUpPredicates().size());
                            Assertions.assertTrue(
                                    predicateCollectorContext.getCouldPullUpPredicates().stream().allMatch(
                                            expr -> expr.collectFirst(
                                                            node -> node instanceof SlotReference
                                                                    && ((SlotReference) node).getName()
                                                                    .equalsIgnoreCase("first_value"))
                                                    .isPresent()));
                        });
    }
}
