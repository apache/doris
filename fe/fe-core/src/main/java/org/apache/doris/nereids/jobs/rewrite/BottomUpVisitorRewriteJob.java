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
import org.apache.doris.nereids.PlanProcess;
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
import java.util.function.Predicate;

/** BottomUpVisitorRewriteJob */
public class BottomUpVisitorRewriteJob implements RewriteJob {
    private static AtomicInteger batchId = new AtomicInteger(0);
    private final Rules rules;
    private final Predicate<Plan> isTraverseChildren;

    public BottomUpVisitorRewriteJob(Rules rules, Predicate<Plan> isTraverseChildren) {
        this.rules = rules;
        this.isTraverseChildren = isTraverseChildren;
    }

    public Rules getRules() {
        return rules;
    }

    @Override
    public void execute(JobContext jobContext) {
        Plan originPlan = jobContext.getCascadesContext().getRewritePlan();
        Optional<Rules> relateRules
                = TopDownVisitorRewriteJob.getRelatedRules(originPlan, rules, jobContext.getCascadesContext());
        if (!relateRules.isPresent()) {
            return;
        }

        Plan root = rewrite(
                null, -1, originPlan, jobContext, rules, batchId.incrementAndGet(), false, new ProcessState(originPlan)
        );
        jobContext.getCascadesContext().setRewritePlan(root);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    private Plan rewrite(
            Plan parent, int childIndex, Plan plan, JobContext jobContext, Rules rules,
            int batchId, boolean fastReturn, ProcessState processState) {
        RewriteState state = PlanTreeRewriteBottomUpJob.getState(plan, batchId);
        if (state == RewriteState.REWRITTEN) {
            return plan;
        }
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        boolean showPlanProcess = cascadesContext.showPlanProcess();

        Plan currentPlan = plan;
        while (true) {
            if (fastReturn && rules.getCurrentAndChildrenRules(currentPlan).isEmpty()) {
                return currentPlan;
            }
            if (!isTraverseChildren.test(currentPlan)) {
                return currentPlan;
            }

            checkTimeout(jobContext);

            ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(currentPlan.arity());
            boolean changed = false;

            for (int i = 0; i < currentPlan.children().size(); i++) {
                Plan child = currentPlan.children().get(i);
                Plan rewrite = rewrite(currentPlan, i, child, jobContext, rules, batchId, true, processState);
                newChildren.add(rewrite);
                changed |= !rewrite.deepEquals(child);
            }
            if (changed) {
                currentPlan = currentPlan.withChildren(newChildren.build());
                if (showPlanProcess) {
                    parent = processState.updateChild(parent, childIndex, currentPlan);
                }
            }
            Plan rewrittenPlan = doRewrite(parent, childIndex, currentPlan, jobContext, rules, processState);
            if (!rewrittenPlan.deepEquals(currentPlan)) {
                currentPlan = rewrittenPlan;
                if (showPlanProcess) {
                    parent = processState.updateChild(parent, childIndex, currentPlan);
                }
            } else {
                PlanTreeRewriteBottomUpJob.setState(rewrittenPlan, RewriteState.REWRITTEN, batchId);
                return rewrittenPlan;
            }
        }
    }

    private static Plan doRewrite(
            Plan originParent, int childIndex, Plan plan, JobContext jobContext,
            Rules rules, ProcessState processState) {
        List<Rule> currentRules = rules.getCurrentRules(plan);
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        BitSet forbidRules = cascadesContext.getAndCacheDisableRules();
        for (Rule currentRule : currentRules) {
            if (forbidRules.get(currentRule.getRuleType().ordinal()) || !currentRule.getPattern().matchPlanTree(plan)) {
                continue;
            }
            List<Plan> transform = currentRule.transform(plan, cascadesContext);
            if (!transform.isEmpty() && !transform.get(0).deepEquals(plan)) {
                Plan result = transform.get(0);
                currentRule.acceptPlan(result);

                if (cascadesContext.showPlanProcess()) {
                    String beforeShape = processState.getNewestPlan().treeString(true);
                    String afterShape = processState.updateChildAndGetNewest(originParent, childIndex, result)
                            .treeString(true);
                    cascadesContext.addPlanProcess(
                        new PlanProcess(currentRule.getRuleType().name(), beforeShape, afterShape)
                    );
                }
                // if rewrite success, record the rule type
                cascadesContext.getStatementContext().ruleSetApplied(currentRule.getRuleType());
                return result;
            }
        }
        return plan;
    }
}
