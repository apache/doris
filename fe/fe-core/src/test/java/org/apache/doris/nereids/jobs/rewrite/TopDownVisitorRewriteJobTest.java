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
        // one more filter on top on every pass
        Rule neverConverges = new OneRewriteRuleFactory() {
            @Override
            public Rule build() {
                return logicalFilter().then(filter -> new LogicalFilter<Plan>(
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
}
