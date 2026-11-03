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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rule for pushdown project through inner/outer join
 * Just push down project inside join to avoid to push the top of Join-Cluster.
 * <pre>
 *    Project                   Join
 *      |            ──►       /    \
 *     Join               Project  Project
 *    /   \                  |       |
 *   A     B                 A       B
 * </pre>
 */
public class PushDownProjectThroughInnerOuterJoin implements ExplorationRuleFactory {
    public static final PushDownProjectThroughInnerOuterJoin INSTANCE = new PushDownProjectThroughInnerOuterJoin();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalJoin(logicalProject(logicalJoin()), group())
                        .when(j -> j.left().child().getJoinType().isOuterJoin()
                                || j.left().child().getJoinType().isInnerJoin()
                                || j.left().child().getJoinType().isAsofOuterJoin()
                                || j.left().child().getJoinType().isAsofInnerJoin())
                        // Just pushdown project with non-column expr like (t.id + 1)
                        .whenNot(j -> j.left().isAllSlots())
                        .whenNot(j -> j.left().child().hasDistributeHint())
                        .then(topJoin -> {
                            LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topJoin.left();
                            Plan newLeft = ProjectJoinReorderHelper.normalize(project).orElse(null);
                            if (newLeft == null) {
                                return null;
                            }
                            return topJoin.withChildren(newLeft, topJoin.right());
                        }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_LEFT),
                logicalJoin(group(), logicalProject(logicalJoin()))
                        .when(j -> j.right().child().getJoinType().isOuterJoin()
                                || j.right().child().getJoinType().isInnerJoin()
                                || j.right().child().getJoinType().isAsofOuterJoin()
                                || j.right().child().getJoinType().isAsofInnerJoin())
                        // Just pushdown project with non-column expr like (t.id + 1)
                        .whenNot(j -> j.right().isAllSlots())
                        .whenNot(j -> j.right().child().hasDistributeHint())
                        .then(topJoin -> {
                            LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topJoin.right();
                            Plan newRight = ProjectJoinReorderHelper.normalize(project).orElse(null);
                            if (newRight == null) {
                                return null;
                            }
                            return topJoin.withChildren(topJoin.left(), newRight);
                        }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_INNER_OUTER_JOIN_RIGHT)
        );
    }
}
