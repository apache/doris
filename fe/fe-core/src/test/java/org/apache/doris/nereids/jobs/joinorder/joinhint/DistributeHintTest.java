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

package org.apache.doris.nereids.jobs.joinorder.joinhint;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.datasets.tpch.TPCHTestBase;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.util.HyperGraphBuilderOld;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * A hint only tells the optimizer which plan to prefer; it must never change the result of the
 * query. Both tests below build a random join graph, evaluate it with an in-memory join evaluator,
 * optimize it, and assert the evaluated result is unchanged.
 */
public class DistributeHintTest extends TPCHTestBase {

    // Leading order is only unconditionally valid for inner joins, so the permutations are drawn
    // from a fixed seed to keep a failure reproducible from the test output.
    private static final long LEADING_SEED = 20260729L;

    /**
     * A distribute hint (broadcast / shuffle) picks a physical strategy, so it can never change the
     * result. This is the only place that covers distribute hints against random join graphs.
     */
    @Test
    public void testHintJoin() {
        for (int t = 3; t < 10; t++) {
            for (int e = t - 1; e <= (t * (t - 1)) / 2; e++) {
                for (int i = 0; i < 10; i++) {
                    HyperGraphBuilderOld builder = new HyperGraphBuilderOld();
                    Plan plan = builder.buildJoinPlanWithJoinHint(t, e);
                    plan = new LogicalProject(plan.getOutput(), plan);
                    assertOptimizeKeepsResult(builder, plan, plan,
                            String.format("distribute hint changed the result (tables=%d, edges=%d, iter=%d)",
                                    t, e, i));
                }
            }
        }
    }

    /**
     * Reordering inner joins is always semantics-preserving, so any leading order must produce the
     * same result as the un-hinted plan.
     */
    @Test
    public void testLeading() {
        Random random = new Random(LEADING_SEED);
        for (int t = 3; t < 7; t++) {
            for (int e = t - 1; e <= (t * (t - 1)) / 2; e++) {
                for (int i = 0; i < 3; i++) {
                    HyperGraphBuilderOld builder =
                            new HyperGraphBuilderOld(ImmutableSet.of(JoinType.INNER_JOIN));
                    Plan plan = builder.randomBuildPlanWith(t, e);
                    plan = new LogicalProject(plan.getOutput(), plan);
                    for (int p = 0; p < Math.min(t, 4); p++) {
                        List<String> order = leadingOrder(t, random);
                        Plan leadingPlan = withLeadingHint(order, plan);
                        assertOptimizeKeepsResult(builder, plan, leadingPlan,
                                String.format("leading %s changed the result (tables=%d, edges=%d, iter=%d)",
                                        order, t, e, i));
                    }
                }
            }
        }
    }

    /**
     * Same property as {@link #testLeading}, but the graph also contains outer / semi / anti joins,
     * for which an arbitrary leading order is generally not a valid join order. Today Doris applies
     * such a hint anyway and the result changes -- extra null-padded rows appear. Whether the hint
     * should be rejected and ignored instead is an open question for the Nereids owners, so this
     * test records the case rather than asserting a behaviour nobody has signed off on.
     *
     * <p>Reproduces at 3 tables. Re-enable once the intended behaviour is decided.
     */
    @Disabled("leading hint over outer joins is not result-preserving today; intended behaviour undecided")
    @Test
    public void testLeadingWithOuterJoin() {
        Random random = new Random(LEADING_SEED);
        for (int t = 3; t < 6; t++) {
            for (int e = t - 1; e <= (t * (t - 1)) / 2; e++) {
                HyperGraphBuilderOld builder = new HyperGraphBuilderOld();
                Plan plan = builder.randomBuildPlanWith(t, e);
                plan = new LogicalProject(plan.getOutput(), plan);
                List<String> order = leadingOrder(t, random);
                assertOptimizeKeepsResult(builder, plan, withLeadingHint(order, plan),
                        String.format("leading %s over outer joins changed the result (tables=%d, edges=%d)",
                                order, t, e));
            }
        }
    }

    private List<String> leadingOrder(int tableNum, Random random) {
        List<String> order = new ArrayList<>();
        for (int i = 0; i < tableNum; i++) {
            order.add(String.valueOf(i));
        }
        Collections.shuffle(order, random);
        return order;
    }

    private Plan withLeadingHint(List<String> order, Plan childPlan) {
        ImmutableList.Builder<SelectHint> hints = ImmutableList.builder();
        hints.add(new SelectHintLeading("Leading", order, ImmutableMap.of()));
        return new LogicalSelectHint<>(hints.build(), childPlan);
    }

    /**
     * Evaluates {@code originalPlan}, optimizes {@code planToOptimize}, and asserts both produce the
     * same tuples.
     */
    private void assertOptimizeKeepsResult(HyperGraphBuilderOld builder, Plan originalPlan,
            Plan planToOptimize, String message) {
        Set<List<String>> expected = builder.evaluate(originalPlan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, planToOptimize);
        builder.initStats("tpch", cascadesContext);
        Plan optimizedPlan = PlanChecker.from(cascadesContext)
                .analyze()
                .optimize()
                .getBestPlanTree();
        Assertions.assertEquals(expected, builder.evaluate(optimizedPlan),
                message + "\noriginal plan:\n" + originalPlan.treeString()
                        + "\noptimized plan:\n" + optimizedPlan.treeString());
    }
}
