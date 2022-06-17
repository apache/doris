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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * Bottom up job for rewrite, use pattern match.
 */
public class RewriteBottomUpJob extends Job<Plan> {
    private final Group group;
    private final List<Rule<Plan>> rules;
    private final boolean childrenOptimized;

    public RewriteBottomUpJob(Group group, List<Rule<Plan>> rules, PlannerContext context) {
        this(group, rules, context, false);
    }

    private RewriteBottomUpJob(Group group, List<Rule<Plan>> rules,
            PlannerContext context, boolean childrenOptimized) {
        super(JobType.BOTTOM_UP_REWRITE, context);
        this.group = Objects.requireNonNull(group, "group cannot be null");
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
        this.childrenOptimized = childrenOptimized;
    }

    @Override
    public void execute() throws AnalysisException {
        GroupExpression logicalExpression = group.getLogicalExpression();
        if (!childrenOptimized) {
            for (Group childGroup : logicalExpression.children()) {
                pushTask(new RewriteBottomUpJob(childGroup, rules, context, false));
            }
            pushTask(new RewriteBottomUpJob(group, rules, context, true));
            return;
        }

        List<Rule<Plan>> validRules = getValidRules(logicalExpression, rules);
        for (Rule<Plan> rule : validRules) {
            GroupExpressionMatching groupExpressionMatching
                    = new GroupExpressionMatching(rule.getPattern(), logicalExpression);
            for (Plan before : groupExpressionMatching) {
                List<Plan> afters = rule.transform(before, context);
                Preconditions.checkArgument(afters.size() == 1);
                Plan after = afters.get(0);
                if (after != before) {
                    context.getOptimizerContext().getMemo().copyIn(after, group, rule.isRewrite());
                    pushTask(new RewriteBottomUpJob(group, rules, context, false));
                    return;
                }
            }
            logicalExpression.setApplied(rule);
        }
    }
}
