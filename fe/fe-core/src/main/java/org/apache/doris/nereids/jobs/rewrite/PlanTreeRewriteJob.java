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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.minidump.NereidsTracer;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/** PlanTreeRewriteJob */
public abstract class PlanTreeRewriteJob extends Job {
    protected final Predicate<Plan> isTraverseChildren;

    public PlanTreeRewriteJob(JobType type, JobContext context, Predicate<Plan> isTraverseChildren) {
        super(type, context);
        this.isTraverseChildren = Objects.requireNonNull(isTraverseChildren, "isTraverseChildren can not be null");
    }

    protected final RewriteResult rewrite(Plan plan, List<Rule> rules, RewriteJobContext rewriteJobContext) {
        CascadesContext cascadesContext = context.getCascadesContext();
        cascadesContext.setIsRewriteRoot(rewriteJobContext.isRewriteRoot());

        boolean showPlanProcess = cascadesContext.showPlanProcess();
        for (Rule rule : rules) {
            if (disableRules.get(rule.getRuleType().type())) {
                continue;
            }
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, cascadesContext);
                if (newPlans.size() != 1) {
                    throw new AnalysisException("Rewrite rule should generate one plan: " + rule.getRuleType());
                }
                Plan newPlan = newPlans.get(0);
                if (!newPlan.deepEquals(plan)) {
                    // don't remove this comment, it can help us to trace some bug when developing.

                    NereidsTracer.logRewriteEvent(rule.toString(), pattern, plan, newPlan);
                    String traceBefore = null;
                    if (showPlanProcess) {
                        traceBefore = getCurrentPlanTreeString();
                    }
                    rewriteJobContext.result = newPlan;
                    context.setRewritten(true);
                    rule.acceptPlan(newPlan);
                    if (showPlanProcess) {
                        String traceAfter = getCurrentPlanTreeString();
                        PlanProcess planProcess = new PlanProcess(rule.getRuleType().name(), traceBefore, traceAfter);
                        cascadesContext.addPlanProcess(planProcess);
                    }
                    return new RewriteResult(true, newPlan);
                }
            }
        }
        return new RewriteResult(false, plan);
    }

    protected static Plan linkChildren(Plan plan, RewriteJobContext[] childrenContext) {
        List<Plan> children = plan.children();
        // loop unrolling
        switch (children.size()) {
            case 0: {
                return plan;
            }
            case 1: {
                RewriteJobContext child = childrenContext[0];
                Plan firstResult = child == null ? plan.child(0) : child.result;
                return firstResult == null || firstResult == children.get(0)
                        ? plan : plan.withChildren(ImmutableList.of(firstResult));
            }
            case 2: {
                RewriteJobContext left = childrenContext[0];
                Plan firstResult = left == null ? plan.child(0) : left.result;
                RewriteJobContext right = childrenContext[1];
                Plan secondResult = right == null ? plan.child(1) : right.result;
                Plan firstOrigin = children.get(0);
                Plan secondOrigin = children.get(1);
                boolean firstChanged = firstResult != null && firstResult != firstOrigin;
                boolean secondChanged = secondResult != null && secondResult != secondOrigin;
                if (firstChanged || secondChanged) {
                    ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(2);
                    newChildren.add(firstChanged ? firstResult : firstOrigin);
                    newChildren.add(secondChanged ? secondResult : secondOrigin);
                    return plan.withChildren(newChildren.build());
                } else {
                    return plan;
                }
            }
            default: {
                boolean anyChanged = false;
                int i = 0;
                Plan[] newChildren = new Plan[childrenContext.length];
                for (Plan oldChild : children) {
                    Plan result = childrenContext[i].result;
                    boolean changed = result != null && result != oldChild;
                    newChildren[i] = changed ? result : oldChild;
                    anyChanged |= changed;
                    i++;
                }
                return anyChanged ? plan.withChildren(newChildren) : plan;
            }
        }
    }

    private String getCurrentPlanTreeString() {
        return context.getCascadesContext()
                .getCurrentRootRewriteJobContext().get()
                .getNewestPlan()
                .treeString();
    }

    static class RewriteResult {
        final boolean hasNewPlan;
        final Plan plan;

        public RewriteResult(boolean hasNewPlan, Plan plan) {
            this.hasNewPlan = hasNewPlan;
            this.plan = plan;
        }
    }
}
