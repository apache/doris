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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;

/** TopDownVisitorRewriteJob */
public class TopDownVisitorRewriteJob implements RewriteJob {
    private final Rules rules;

    public TopDownVisitorRewriteJob(Rules rules) {
        this.rules = rules;
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
        Plan root = rewrite(originPlan, jobContext, rules, false);
        jobContext.getCascadesContext().setRewritePlan(root);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    private static Plan rewrite(Plan plan, JobContext jobContext, Rules rules, boolean fastReturn) {
        if (fastReturn && rules.getCurrentAndChildrenRules(plan).isEmpty()) {
            return plan;
        }

        plan = doRewrite(plan, jobContext, rules);

        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (Plan child : plan.children()) {
            Plan newChild = rewrite(child, jobContext, rules, true);
            newChildren.add(newChild);
            changed |= !newChild.deepEquals(child);
        }

        if (changed) {
            plan = plan.withChildren(newChildren.build());
        }

        return plan;
    }

    private static Plan doRewrite(Plan plan, JobContext jobContext, Rules rules) {
        Plan oldPlan = plan;
        while (true) {
            List<Rule> currentRules = rules.getCurrentRules(oldPlan);
            BitSet forbidRules = jobContext.getCascadesContext().getAndCacheDisableRules();

            boolean changed = false;
            for (Rule currentRule : currentRules) {
                if (forbidRules.get(currentRule.getRuleType().ordinal())
                        || !currentRule.getPattern().matchPlanTree(oldPlan)) {
                    continue;
                }
                List<Plan> transform = currentRule.transform(oldPlan, jobContext.getCascadesContext());
                if (!transform.isEmpty() && !transform.get(0).deepEquals(oldPlan)) {
                    oldPlan = transform.get(0);
                    currentRule.acceptPlan(oldPlan);
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                return oldPlan;
            }
        }
    }

    /** getRelateRules */
    public static Optional<Rules> getRelatedRules(Plan plan, Rules originRules, CascadesContext context) {
        List<Rule> validRules = originRules.filterValidRules(context);
        if (validRules.isEmpty()) {
            return Optional.empty();
        }

        // Rules enableRules = originRules;
        // if (validRules.size() != originRules.getAllRules().size()) {
        //     enableRules = new FilteredRules(validRules);
        // }
        //
        // if (enableRules.getCurrentAndChildrenRules(plan).isEmpty()) {
        //     return Optional.empty();
        // }
        return Optional.of(originRules);
    }
}
