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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.FilteredRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class TopDownVisitorRewriteJobTest {
    @Test
    void testRuleThatChangesItsNodeOnEveryPassIsStopped() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Plan plan = new LogicalFilter<Plan>(
                ImmutableSet.of(new EqualTo(scan.getOutput().get(0), new IntegerLiteral(0))), scan);
        AtomicInteger applied = new AtomicInteger();
        // One more filter on top on every pass. It stops by itself a little after the bound, so that without
        // the bound this test fails on its assertion and does not fill the heap.
        int stopsAt = TopDownVisitorRewriteJob.MAX_REWRITES_OF_ONE_NODE + 50;
        Rule neverConverges = new OneRewriteRuleFactory() {
            @Override
            public Rule build() {
                return logicalFilter().when(filter -> applied.get() < stopsAt).then(filter -> new LogicalFilter<Plan>(
                        ImmutableSet.of(new EqualTo(filter.getOutput().get(0),
                                new IntegerLiteral(applied.incrementAndGet()))), filter)
                ).toRule(RuleType.TEST_REWRITE);
            }
        }.build();
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                MemoTestUtils.createConnectContext(), plan);
        TopDownVisitorRewriteJob job = new TopDownVisitorRewriteJob(
                new FilteredRules(ImmutableList.of(neverConverges)), p -> true);

        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> job.execute(cascadesContext.getCurrentJobContext()));
        Assertions.assertTrue(e.getMessage().contains(RuleType.TEST_REWRITE.name()), e.getMessage());
        Assertions.assertEquals(TopDownVisitorRewriteJob.MAX_REWRITES_OF_ONE_NODE + 1, applied.get());
    }

    @Test
    void testTheBoundCountsOnePositionNotTheWholePlan() {
        // Two filters, each changed 6000 times: 12000 rewrites of the plan, 6000 at each position.
        int perPosition = 6000;
        Assertions.assertTrue(perPosition < TopDownVisitorRewriteJob.MAX_REWRITES_OF_ONE_NODE);
        Assertions.assertTrue(2 * perPosition > TopDownVisitorRewriteJob.MAX_REWRITES_OF_ONE_NODE);
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Plan below = new LogicalFilter<Plan>(
                ImmutableSet.of(new EqualTo(scan.getOutput().get(0), new IntegerLiteral(0))), scan);
        Plan plan = new LogicalFilter<Plan>(
                ImmutableSet.of(new EqualTo(scan.getOutput().get(0), new IntegerLiteral(0))), below);
        AtomicInteger applied = new AtomicInteger();
        // counts a filter's literal up to perPosition, in place
        Rule countsUp = new OneRewriteRuleFactory() {
            @Override
            public Rule build() {
                return logicalFilter().when(filter -> literalOf(filter) < perPosition).then(filter -> {
                    applied.incrementAndGet();
                    return new LogicalFilter<Plan>(ImmutableSet.of(new EqualTo(filter.getOutput().get(0),
                            new IntegerLiteral(literalOf(filter) + 1))), filter.child());
                }).toRule(RuleType.TEST_REWRITE);
            }
        }.build();
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                MemoTestUtils.createConnectContext(), plan);
        TopDownVisitorRewriteJob job = new TopDownVisitorRewriteJob(
                new FilteredRules(ImmutableList.of(countsUp)), p -> true);

        job.execute(cascadesContext.getCurrentJobContext());
        Assertions.assertEquals(2 * perPosition, applied.get());
    }

    @Test
    void testPlanningTimeoutStopsRuleBetweenPasses() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Plan plan = new LogicalFilter<Plan>(
                ImmutableSet.of(new EqualTo(scan.getOutput().get(0), new IntegerLiteral(0))), scan);
        AtomicInteger applied = new AtomicInteger();
        // one more filter on top on every pass; the first pass alone takes longer than the planning timeout
        Rule neverConverges = new OneRewriteRuleFactory() {
            @Override
            public Rule build() {
                return logicalFilter().then(filter -> {
                    if (applied.incrementAndGet() == 1) {
                        try {
                            Thread.sleep(1100);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return new LogicalFilter<Plan>(ImmutableSet.of(new EqualTo(filter.getOutput().get(0),
                            new IntegerLiteral(applied.get()))), filter);
                }).toRule(RuleType.TEST_REWRITE);
            }
        }.build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().enableNereidsTimeout = true;
        connectContext.getSessionVariable().nereidsTimeoutSecond = 0;
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        // only NereidsPlanner starts this stopwatch; unstarted, the timeout check always reads zero
        if (!cascadesContext.getStatementContext().getStopwatch().isRunning()) {
            cascadesContext.getStatementContext().getStopwatch().start();
        }
        TopDownVisitorRewriteJob job = new TopDownVisitorRewriteJob(
                new FilteredRules(ImmutableList.of(neverConverges)), p -> true);

        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> job.execute(cascadesContext.getCurrentJobContext()));
        // the timeout, read at the start of the second pass, and not the bound after ten thousand of them
        Assertions.assertTrue(e.getMessage().contains("cost too much time"), e.getMessage());
        Assertions.assertEquals(1, applied.get());
    }

    private static int literalOf(LogicalFilter<? extends Plan> filter) {
        EqualTo equalTo = (EqualTo) filter.getConjuncts().iterator().next();
        return ((IntegerLiteral) equalTo.right()).getValue();
    }
}
