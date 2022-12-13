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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.TransformEvent;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.List;

/**
 * Job to apply rule on {@link GroupExpression}.
 */
public class ApplyRuleJob extends Job {
    private static final EventProducer APPLY_RULE_TRACER = new EventProducer(TransformEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(TransformEvent.class, EventChannel.LOG)));
    private final GroupExpression groupExpression;
    private final Rule rule;

    /**
     * Constructor of ApplyRuleJob.
     *
     * @param groupExpression apply rule on this {@link GroupExpression}
     * @param rule rule to be applied
     * @param context context of current job
     */
    public ApplyRuleJob(GroupExpression groupExpression, Rule rule, JobContext context) {
        super(JobType.APPLY_RULE, context);
        this.groupExpression = groupExpression;
        this.rule = rule;
    }

    @Override
    public void execute() throws AnalysisException {
        if (groupExpression.hasApplied(rule)) {
            return;
        }
        countJobExecutionTimesOfGroupExpressions(groupExpression);

        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(rule.getPattern(), groupExpression);
        for (Plan plan : groupExpressionMatching) {
            List<Plan> newPlans = rule.transform(plan, context.getCascadesContext());
            for (Plan newPlan : newPlans) {
                CopyInResult result = context.getCascadesContext()
                        .getMemo()
                        .copyIn(newPlan, groupExpression.getOwnerGroup(), false);
                if (!result.generateNewExpression) {
                    continue;
                }
                GroupExpression newGroupExpression = result.correspondingExpression;
                if (newPlan instanceof LogicalPlan) {
                    pushJob(new OptimizeGroupExpressionJob(newGroupExpression, context));
                    pushJob(new DeriveStatsJob(newGroupExpression, context));
                } else {
                    pushJob(new CostAndEnforcerJob(newGroupExpression, context));
                }
                APPLY_RULE_TRACER.log(TransformEvent.of(groupExpression, plan, newPlans, rule.getRuleType()),
                        rule::isRewrite);
            }
        }
        groupExpression.setApplied(rule);
    }
}
