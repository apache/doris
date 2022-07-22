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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Bottom up job for rewrite, use pattern match.
 */
public class RewriteBottomUpJob extends Job {
    private final Group group;
    private final List<Rule> rules;
    private final boolean childrenOptimized;


    public RewriteBottomUpJob(Group group, JobContext context, List<RuleFactory> factories) {
        this(group, factories.stream()
                .flatMap(factory -> factory.buildRules().stream())
                .collect(Collectors.toList()), context, false);
    }

    public RewriteBottomUpJob(Group group, List<Rule> rules, JobContext context) {
        this(group, rules, context, false);
    }

    private RewriteBottomUpJob(Group group, List<Rule> rules,
            JobContext context, boolean childrenOptimized) {
        super(JobType.BOTTOM_UP_REWRITE, context);
        this.group = Objects.requireNonNull(group, "group cannot be null");
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
        this.childrenOptimized = childrenOptimized;
    }

    @Override
    public void execute() throws AnalysisException {
        GroupExpression logicalExpression = group.getLogicalExpression();
        if (!childrenOptimized) {
            pushTask(new RewriteBottomUpJob(group, rules, context, true));
            for (Group childGroup : logicalExpression.children()) {
                pushTask(new RewriteBottomUpJob(childGroup, rules, context, false));
            }
            return;
        }

        List<Rule> validRules = getValidRules(logicalExpression, rules);
        for (Rule rule : validRules) {
            GroupExpressionMatching groupExpressionMatching
                    = new GroupExpressionMatching(rule.getPattern(), logicalExpression);
            for (Plan before : groupExpressionMatching) {
                List<Plan> afters = rule.transform(before, context.getPlannerContext());
                Preconditions.checkArgument(afters.size() == 1);
                Plan after = afters.get(0);
                if (after != before) {
                    GroupExpression groupExpr = context.getPlannerContext()
                            .getMemo()
                            .copyIn(after, group, rule.isRewrite());
                    groupExpr.setApplied(rule);
                    pushTask(new RewriteBottomUpJob(group, rules, context, false));
                    return;
                }
            }
        }
    }
}
