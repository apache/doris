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
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rules to push {@link org.apache.doris.nereids.trees.plans.logical.LogicalLimit} down.
 * <p>
 * Limit can't be push down if it has a valid offset info.
 */
public class PushdownLimit implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // limit -> join
                logicalLimit(logicalJoin(any(), any())).whenNot(Limit::hasValidOffset)
                        .then(limit -> {
                            Plan newJoin = pushLimitThroughJoin(limit, limit.child());
                            if (newJoin == null || limit.child().children().equals(newJoin.children())) {
                                return null;
                            }
                            return limit.withChildren(newJoin);
                        })
                        .toRule(RuleType.PUSH_LIMIT_THROUGH_JOIN),

                // limit -> project -> join
                logicalLimit(logicalProject(logicalJoin(any(), any()))).whenNot(Limit::hasValidOffset)
                        .then(limit -> {
                            LogicalProject<LogicalJoin<Plan, Plan>> project = limit.child();
                            LogicalJoin<Plan, Plan> join = project.child();
                            Plan newJoin = pushLimitThroughJoin(limit, join);
                            if (newJoin == null || join.children().equals(newJoin.children())) {
                                return null;
                            }
                            return limit.withChildren(project.withChildren(newJoin));
                        }).toRule(RuleType.PUSH_LIMIT_THROUGH_PROJECT_JOIN),

                // limit -> union
                logicalLimit(logicalUnion(multi()).when(union -> union.getQualifier() == Qualifier.ALL))
                        .whenNot(Limit::hasValidOffset)
                        .then(limit -> {
                            LogicalUnion union = limit.child();
                            ImmutableList<Plan> newUnionChildren = union.children()
                                    .stream()
                                    .map(child -> limit.withChildren(child))
                                    .collect(ImmutableList.toImmutableList());
                            if (union.children().equals(newUnionChildren)) {
                                return null;
                            }
                            return limit.withChildren(union.withChildren(newUnionChildren));
                        })
                        .toRule(RuleType.PUSH_LIMIT_THROUGH_UNION),
                // limit -> sort ==> topN
                logicalLimit(logicalSort())
                        .then(limit -> {
                            LogicalSort sort = limit.child();
                            LogicalTopN topN = new LogicalTopN(sort.getOrderKeys(),
                                    limit.getLimit(),
                                    limit.getOffset(),
                                    sort.child(0));
                            return topN;
                        }).toRule(RuleType.PUSH_LIMIT_INTO_SORT),
                logicalLimit(logicalOneRowRelation())
                        .then(limit -> limit.getLimit() > 0 && limit.getOffset() == 0
                                ? limit.child() : new LogicalEmptyRelation(limit.child().getOutput()))
                        .toRule(RuleType.ELIMINATE_LIMIT_ON_ONE_ROW_RELATION),
                logicalLimit(logicalEmptyRelation())
                        .then(UnaryNode::child)
                        .toRule(RuleType.ELIMINATE_LIMIT_ON_EMPTY_RELATION),
                new MergeLimits().build()
        );
    }

    private Plan pushLimitThroughJoin(LogicalLimit<? extends Plan> limit, LogicalJoin<Plan, Plan> join) {
        switch (join.getJoinType()) {
            case LEFT_OUTER_JOIN:
                return join.withChildren(
                        limit.withChildren(join.left()),
                        join.right()
                );
            case RIGHT_OUTER_JOIN:
                return join.withChildren(
                        join.left(),
                        limit.withChildren(join.right())
                );
            case CROSS_JOIN:
                return join.withChildren(
                        limit.withChildren(join.left()),
                        limit.withChildren(join.right())
                );
            default:
                // don't push limit.
                return null;
        }
    }
}
