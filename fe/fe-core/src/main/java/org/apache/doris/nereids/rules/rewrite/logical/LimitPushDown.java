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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rules to push {@link org.apache.doris.nereids.trees.plans.logical.LogicalLimit} down.
 * <p>
 * Limit can't be push down if it has a valid offset info.
 */
public class LimitPushDown implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // limit -> join
                logicalLimit(logicalJoin(any(), any())).whenNot(Limit::hasValidOffset)
                        .then(limit -> limit.withChildren(pushLimitThroughJoin(limit, limit.child())))
                        .toRule(RuleType.PUSH_LIMIT_THROUGH_JOIN),

                // limit -> project -> join
                logicalLimit(logicalProject(logicalJoin(any(), any()))).whenNot(Limit::hasValidOffset)
                        .then(limit -> {
                            LogicalProject<LogicalJoin<Plan, Plan>> project = limit.child();
                            LogicalJoin<Plan, Plan> join = project.child();
                            return limit.withChildren(
                                    project.withChildren(
                                            pushLimitThroughJoin(limit, join)));
                        }).toRule(RuleType.PUSH_LIMIT_THROUGH_PROJECT_JOIN)
        );
    }

    private Plan pushLimitThroughJoin(LogicalLimit<? extends Plan> limit, LogicalJoin<Plan, Plan> join) {
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN:
                return join.withChildren(
                        addLimit(limit, join.left()),
                        join.right()
                );
            case RIGHT_OUTER_JOIN:
                return join.withChildren(
                        join.left(),
                        addLimit(limit, join.right())
                );
            case CROSS_JOIN:
                return join.withChildren(
                        addLimit(limit, join.left()),
                        addLimit(limit, join.right())
                );
            case INNER_JOIN:
                if (join.hasJoinCondition()) {
                    return join;
                } else {
                    return join.withChildren(
                            addLimit(limit, join.left()),
                            addLimit(limit, join.right())
                    );
                }
            default:
                // don't push limit.
                return join;
        }
    }

    private Plan addLimit(LogicalLimit<? extends Plan> pushdownLimit, Plan plan) {
        if (plan instanceof LogicalLimit) {
            // Avoid adding duplicate limits on top of the plan, otherwise would result in dead loop
            // when applying the rule multiple times.
            LogicalLimit<? extends Plan> limit = (LogicalLimit<? extends Plan>) plan;
            // plan is pure limit and limit value > push down limit value
            if (!limit.hasValidOffset() && limit.getLimit() > pushdownLimit.getLimit()) {
                // replace limit.
                return pushdownLimit.withChildren(limit.child());
            } else {
                // return input plan.
                return plan;
            }
        } else if (plan instanceof OneRowRelation) {
            return pushdownLimit.getLimit() > 0 ? plan : new LogicalEmptyRelation((List) plan.getOutput());
        } else if (plan instanceof EmptyRelation) {
            return plan;
        } else {
            return pushdownLimit.withChildren(plan);
        }
    }
}
