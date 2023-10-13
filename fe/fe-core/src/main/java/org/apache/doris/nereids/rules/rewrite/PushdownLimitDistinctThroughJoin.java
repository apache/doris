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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Same with PushdownLimit
 */
public class PushdownLimitDistinctThroughJoin implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // limit -> distinct -> join
                logicalLimit(logicalAggregate(logicalJoin())
                        .when(LogicalAggregate::isDistinct))
                        .then(limit -> {
                            LogicalAggregate<LogicalJoin<Plan, Plan>> agg = limit.child();
                            LogicalJoin<Plan, Plan> join = agg.child();

                            Plan newJoin = pushLimitThroughJoin(limit, join);
                            if (newJoin == null || join.children().equals(newJoin.children())) {
                                return null;
                            }
                            return limit.withChildren(agg.withChildren(newJoin));
                        })
                        .toRule(RuleType.PUSH_LIMIT_DISTINCT_THROUGH_JOIN),

                // limit -> distinct -> project -> join
                logicalLimit(logicalAggregate(logicalProject(logicalJoin()).when(LogicalProject::isAllSlots))
                        .when(LogicalAggregate::isDistinct))
                        .then(limit -> {
                            LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> agg = limit.child();
                            LogicalProject<LogicalJoin<Plan, Plan>> project = agg.child();
                            LogicalJoin<Plan, Plan> join = project.child();

                            Plan newJoin = pushLimitThroughJoin(limit, join);
                            if (newJoin == null || join.children().equals(newJoin.children())) {
                                return null;
                            }
                            return limit.withChildren(agg.withChildren(project.withChildren(newJoin)));
                        }).toRule(RuleType.PUSH_LIMIT_DISTINCT_THROUGH_JOIN)
        );
    }

    private Plan pushLimitThroughJoin(LogicalLimit<?> limit, LogicalJoin<Plan, Plan> join) {
        LogicalAggregate<?> agg = (LogicalAggregate<?>) limit.child();
        List<Slot> groupBySlots = agg.getGroupByExpressions().stream()
                .flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toList());
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN:
                if (join.left().getOutputSet().containsAll(groupBySlots)
                        && join.left().getOutputSet().equals(agg.getOutputSet())) {
                    return join.withChildren(limit.withLimitChild(limit.getLimit() + limit.getOffset(), 0,
                            agg.withChildren(join.left())), join.right());
                }
                return null;
            case RIGHT_OUTER_JOIN:
                if (join.right().getOutputSet().containsAll(groupBySlots)
                        && join.right().getOutputSet().equals(agg.getOutputSet())) {
                    return join.withChildren(join.left(), limit.withLimitChild(limit.getLimit() + limit.getOffset(), 0,
                            agg.withChildren(join.right())));
                }
                return null;
            case CROSS_JOIN:
                if (join.left().getOutputSet().containsAll(groupBySlots)
                        && join.left().getOutputSet().equals(agg.getOutputSet())) {
                    return join.withChildren(limit.withLimitChild(limit.getLimit() + limit.getOffset(), 0,
                            agg.withChildren(join.left())), join.right());
                } else if (join.right().getOutputSet().containsAll(groupBySlots)
                        && join.right().getOutputSet().equals(agg.getOutputSet())) {
                    return join.withChildren(join.left(), limit.withLimitChild(limit.getLimit() + limit.getOffset(), 0,
                            agg.withChildren(join.right())));
                } else {
                    return null;
                }
            default:
                return null;
        }
    }
}
