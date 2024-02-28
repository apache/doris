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
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.minidump.NereidsTracer;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

import java.util.List;

/** PlanTreeRewriteJob */
public abstract class PlanTreeRewriteJob extends Job {

    public PlanTreeRewriteJob(JobType type, JobContext context) {
        super(type, context);
    }

    protected RewriteResult rewrite(Plan plan, List<Rule> rules, RewriteJobContext rewriteJobContext) {
        // boolean traceEnable = isTraceEnable(context);
        boolean isRewriteRoot = rewriteJobContext.isRewriteRoot();
        CascadesContext cascadesContext = context.getCascadesContext();
        cascadesContext.setIsRewriteRoot(isRewriteRoot);

        boolean showPlanProcess = cascadesContext.showPlanProcess();
        for (Rule rule : rules) {
            if (disableRules.contains(rule.getRuleType().type())) {
                continue;
            }
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, cascadesContext);
                Preconditions.checkState(newPlans.size() == 1,
                        "Rewrite rule should generate one plan: " + rule.getRuleType());
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

    protected Plan linkChildrenAndParent(Plan plan, RewriteJobContext rewriteJobContext) {
        Plan newPlan = linkChildren(plan, rewriteJobContext.childrenContext);
        rewriteJobContext.setResult(newPlan);
        return newPlan;
    }

    protected Plan linkChildren(Plan plan, RewriteJobContext[] childrenContext) {
        boolean changed = false;
        Plan[] newChildren = new Plan[childrenContext.length];
        for (int i = 0; i < childrenContext.length; ++i) {
            Plan result = childrenContext[i].result;
            Plan oldChild = plan.child(i);
            if (result != null && result != oldChild) {
                newChildren[i] = result;
                changed = true;
            } else {
                newChildren[i] = oldChild;
            }
        }
        return changed ? plan.withChildren(newChildren) : plan;
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
