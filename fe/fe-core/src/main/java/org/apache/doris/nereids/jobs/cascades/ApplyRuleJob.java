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

package org.apache.doris.nereids.jobs.cascades;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.pattern.PatternMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.List;

/**
 * Job to apply rule on {@link GroupExpression}.
 */
public class ApplyRuleJob extends Job {
    private final GroupExpression groupExpression;
    private final Rule<Plan> rule;
    private final boolean exploredOnly;

    /**
     * Constructor of ApplyRuleJob.
     *
     * @param groupExpression apply rule on this {@link GroupExpression}
     * @param rule rule to be applied
     * @param context context of optimization
     */
    public ApplyRuleJob(GroupExpression groupExpression, Rule<Plan> rule, PlannerContext context) {
        super(JobType.APPLY_RULE, context);
        this.groupExpression = groupExpression;
        this.rule = rule;
        this.exploredOnly = false;
    }

    @Override
    public void execute() throws AnalysisException {
        if (groupExpression.hasExplored(rule)) {
            return;
        }

        // TODO: need to find all plan reference tree that match this pattern
        PatternMatching patternMatching = new PatternMatching();
        for (Plan<?, ?> plan : patternMatching) {
            if (!rule.check(plan, context)) {
                continue;
            }
            List<Plan> newPlanList = rule.transform(plan, context);
            for (Plan newPlan : newPlanList) {
                GroupExpression newGroupExpression = context.getOptimizerContext().getMemo()
                        .newGroupExpression(newPlan, groupExpression.getParent());
                // TODO need to check return is a new Reference, other wise will be into a dead loop
                if (newPlan instanceof LogicalPlan) {
                    pushTask(new DeriveStatsJob(newGroupExpression, context));
                    if (exploredOnly) {
                        pushTask(new ExplorePlanJob(newGroupExpression, context));
                    }
                    pushTask(new OptimizePlanJob(newGroupExpression, context));
                } else {
                    pushTask(new CostAndEnforcerJob(newGroupExpression, context));
                }
            }
        }
        groupExpression.setExplored(rule);
    }
}
