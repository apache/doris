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
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.TransformEvent;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Bottom up job for rewrite, use pattern match.
 */
public class RewriteBottomUpJob extends Job {

    private static final EventProducer RULE_TRANSFORM_TRACER = new EventProducer(
            TransformEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(TransformEvent.class, EventChannel.LOG)));

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
    public EventProducer getEventTracer() {
        return RULE_TRANSFORM_TRACER;
    }

    @Override
    public void execute() throws AnalysisException {
        GroupExpression logicalExpression = group.getLogicalExpression();
        if (!childrenOptimized) {
            pushJob(new RewriteBottomUpJob(group, rules, context, true));
            List<Group> children = logicalExpression.children();
            for (int i = children.size() - 1; i >= 0; i--) {
                pushJob(new RewriteBottomUpJob(children.get(i), rules, context, false));
            }
            return;
        }

        countJobExecutionTimesOfGroupExpressions(logicalExpression);
        for (Rule rule : rules) {
            if (rule.isInvalid(disableRules, logicalExpression)) {
                continue;
            }
            GroupExpressionMatching groupExpressionMatching
                    = new GroupExpressionMatching(rule.getPattern(), logicalExpression);
            for (Plan before : groupExpressionMatching) {
                Optional<CopyInResult> copyInResult = invokeRewriteRuleWithTrace(rule, before, group);
                if (!copyInResult.isPresent()) {
                    continue;
                }
                Group correspondingGroup = copyInResult.get().correspondingExpression.getOwnerGroup();
                if (copyInResult.get().generateNewExpression
                        || correspondingGroup != group
                        || logicalExpression.getOwnerGroup() == null) {
                    pushJob(new RewriteBottomUpJob(correspondingGroup, rules, context, false));
                    return;
                }
            }
        }
    }
}
