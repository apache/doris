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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/** TopDownVisitorRewriteJob */
public class TopDownVisitorRewriteJob implements RewriteJob {
    private final Rules rules;
    private final Predicate<Plan> isTraverseChildren;

    public TopDownVisitorRewriteJob(Rules rules, Predicate<Plan> isTraverseChildren) {
        this.rules = rules;
        this.isTraverseChildren = isTraverseChildren;
    }

    public Rules getRules() {
        return rules;
    }

    @Override
    public void execute(JobContext jobContext) {
        Plan originPlan = jobContext.getCascadesContext().getRewritePlan();
        Optional<Rules> relateRules = getRelatedRules(originPlan, rules, jobContext.getCascadesContext());
        if (!relateRules.isPresent()) {
            return;
        }

        Plan root = rewrite(
                null, -1, originPlan, jobContext, rules, false, new ProcessState(originPlan)
        );
        jobContext.getCascadesContext().setRewritePlan(root);

        jobContext.getCascadesContext().setRewritePlan(root);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    private Plan rewrite(Plan parent, int childIndex, Plan plan, JobContext jobContext, Rules rules, boolean fastReturn,
            ProcessState processState) {
        if (fastReturn && rules.getCurrentAndChildrenRules(plan).isEmpty()) {
            return plan;
        }
        if (!isTraverseChildren.test(plan)) {
            return plan;
        }

        checkTimeout(jobContext);

        plan = doRewrite(parent, childIndex, plan, jobContext, rules, processState);

        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (int i = 0; i < plan.children().size(); i++) {
            Plan child = plan.children().get(i);
            Plan newChild = rewrite(plan, i, child, jobContext, rules, true, processState);
            newChildren.add(newChild);
            changed |= !newChild.deepEquals(child);
        }

        if (changed) {
            plan = plan.withChildren(newChildren.build());
            processState.updateChild(parent, childIndex, plan);
        }

        return plan;
    }

    private static Plan doRewrite(
            Plan originParent, int childIndex, Plan plan, JobContext jobContext,
            Rules rules, ProcessState processState) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        Plan originPlan = plan;
        while (true) {
            List<Rule> currentRules = rules.getCurrentRules(originPlan);
            BitSet forbidRules = cascadesContext.getAndCacheDisableRules();

            boolean changed = false;
            for (Rule currentRule : currentRules) {
                if (forbidRules.get(currentRule.getRuleType().ordinal())
                        || !currentRule.getPattern().matchPlanTree(originPlan)) {
                    continue;
                }
                List<Plan> transform = currentRule.transform(originPlan, cascadesContext);
                if (!transform.isEmpty() && !transform.get(0).deepEquals(originPlan)) {
                    Plan newPlan = transform.get(0);
                    currentRule.acceptPlan(originPlan);
                    if (cascadesContext.showPlanProcess()) {
                        String beforeShape = processState.getNewestPlan().treeString(true);
                        String afterShape = processState.updateChildAndGetNewest(originParent, childIndex, newPlan)
                                .treeString(true);
                        cascadesContext.addPlanProcess(
                                new PlanProcess(currentRule.getRuleType().name(), beforeShape, afterShape)
                        );
                    }
                    originPlan = newPlan;
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                return originPlan;
            }
        }
    }

    /** getRelateRules */
    public static Optional<Rules> getRelatedRules(Plan plan, Rules originRules, CascadesContext context) {
        List<Rule> validRules = originRules.filterValidRules(context);
        if (validRules.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(originRules);
    }
}
