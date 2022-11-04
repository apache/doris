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

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Top down job for rewrite, use pattern match.
 */
public class RewriteTopDownJob extends Job {
    private final Group group;
    private final List<Rule> rules;

    public RewriteTopDownJob(Group group, JobContext context, List<RuleFactory> factories) {
        this(group, factories.stream()
                .flatMap(factory -> factory.buildRules().stream())
                .collect(Collectors.toList()), context, true);
    }

    public RewriteTopDownJob(Group group, List<Rule> rules, JobContext context) {
        this(group, rules, context, true);
    }

    /**
     * Constructor.
     *
     * @param group root group to be rewritten
     * @param rules rewrite rules
     * @param context planner context
     */
    public RewriteTopDownJob(Group group, List<Rule> rules, JobContext context, boolean once) {
        super(JobType.TOP_DOWN_REWRITE, context, once);
        this.group = Objects.requireNonNull(group, "group cannot be null");
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
    }

    @Override
    public void execute() {
        GroupExpression logicalExpression = group.getLogicalExpression();

        List<Rule> validRules = getValidRules(logicalExpression, rules);
        for (Rule rule : validRules) {
            Preconditions.checkArgument(rule.isRewrite(),
                    "rules must be rewritable in top down job");
            GroupExpressionMatching groupExpressionMatching
                    = new GroupExpressionMatching(rule.getPattern(), logicalExpression);
            // In topdown job, there must be only one matching plan.
            // This `for` loop runs at most once.
            for (Plan before : groupExpressionMatching) {
                Optional<CopyInResult> copyInResult = invokeRewriteRuleWithTrace(rule, before, group);
                if (copyInResult.isPresent() && copyInResult.get().generateNewExpression) {
                    // new group-expr replaced the origin group-expr in `group`,
                    // run this rule against this `group` again.
                    context.setRewritten(true);
                    pushJob(new RewriteTopDownJob(group, rules, context));
                    return;
                }
            }
        }

        for (Group childGroup : group.getLogicalExpression().children()) {
            pushJob(new RewriteTopDownJob(childGroup, rules, context));
        }
    }
}
