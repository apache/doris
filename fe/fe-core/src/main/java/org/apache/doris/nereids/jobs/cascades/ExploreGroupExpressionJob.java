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

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;

import java.util.Comparator;
import java.util.List;

/**
 * Job to explore {@link GroupExpression} in {@link org.apache.doris.nereids.memo.Memo}.
 */
public class ExploreGroupExpressionJob extends Job {
    private final GroupExpression groupExpression;

    /**
     * Constructor for ExplorePlanJob.
     *
     * @param groupExpression {@link GroupExpression} to be explored
     * @param context context of current job
     */
    public ExploreGroupExpressionJob(GroupExpression groupExpression, JobContext context) {
        super(JobType.EXPLORE_PLAN, context);
        this.groupExpression = groupExpression;
    }

    @Override
    public void execute() {
        // TODO: enable exploration job after we test it
        List<Rule> explorationRules = getRuleSet().getExplorationRules();
        List<Rule> validRules = getValidRules(groupExpression, explorationRules);
        validRules.sort(Comparator.comparingInt(o -> o.getRulePromise().promise()));

        for (Rule rule : validRules) {
            pushJob(new ApplyRuleJob(groupExpression, rule, context));
            for (int i = 0; i < rule.getPattern().children().size(); ++i) {
                Pattern childPattern = rule.getPattern().child(i);
                if (childPattern.arity() > 0 && !childPattern.isGroup()) {
                    Group child = groupExpression.child(i);
                    pushJob(new ExploreGroupJob(child, context));
                }
            }
        }
    }
}
