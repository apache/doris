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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteBottomUpJob.RewriteState;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** BottomUpVisitorRewriteJob */
public class BottomUpVisitorRewriteJob implements RewriteJob {
    private static AtomicInteger batchId = new AtomicInteger(0);
    private final Rules rules;

    public BottomUpVisitorRewriteJob(Rules rules) {
        this.rules = rules;
    }

    @Override
    public void execute(JobContext jobContext) {
        Plan originPlan = jobContext.getCascadesContext().getRewritePlan();
        Optional<Rules> relateRules
                = TopDownVisitorRewriteJob.getRelatedRules(originPlan, rules, jobContext.getCascadesContext());
        if (!relateRules.isPresent()) {
            return;
        }

        Plan root = rewrite(originPlan, jobContext, rules, batchId.incrementAndGet(), false);
        jobContext.getCascadesContext().setRewritePlan(root);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    private static Plan rewrite(Plan plan, JobContext jobContext, Rules rules, int batchId, boolean fastReturn) {
        RewriteState state = PlanTreeRewriteBottomUpJob.getState(plan, batchId);
        if (state == RewriteState.REWRITTEN) {
            return plan;
        }

        while (true) {
            if (fastReturn && rules.getCurrentAndChildrenRules(plan).isEmpty()) {
                return plan;
            }
            ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
            boolean changed = false;
            for (Plan child : plan.children()) {
                Plan rewrite = rewrite(child, jobContext, rules, batchId, true);
                newChildren.add(rewrite);
                changed |= !rewrite.deepEquals(child);
            }
            if (changed) {
                plan = plan.withChildren(newChildren.build());
            }
            Plan rewrittenPlan = doRewrite(plan, jobContext, rules);
            if (!rewrittenPlan.deepEquals(plan)) {
                plan = rewrittenPlan;
            } else {
                PlanTreeRewriteBottomUpJob.setState(rewrittenPlan, RewriteState.REWRITTEN, batchId);
                return rewrittenPlan;
            }
        }
    }

    private static Plan doRewrite(Plan plan, JobContext jobContext, Rules rules) {
        List<Rule> currentRules = rules.getCurrentRules(plan);
        BitSet forbidRules = jobContext.getCascadesContext().getAndCacheDisableRules();
        for (Rule currentRule : currentRules) {
            if (forbidRules.get(currentRule.getRuleType().ordinal()) || !currentRule.getPattern().matchPlanTree(plan)) {
                continue;
            }
            List<Plan> transform = currentRule.transform(plan, jobContext.getCascadesContext());
            if (!transform.isEmpty() && !transform.get(0).deepEquals(plan)) {
                Plan result = transform.get(0);
                currentRule.acceptPlan(result);
                return result;
            }
        }
        return plan;
    }
}
