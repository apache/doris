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
import org.apache.doris.nereids.rules.exploration.CBOUtils;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rule for pushdown project through left-semi/anti join
 * Just push down project inside join to avoid to push the top of Join-Cluster.
 * Note this rule is only used to push down project between join for join ordering.
 * <pre>
 *     Join                     Join
 *      |                        |
 *    Project                   Join
 *      |            ──►       /   \
 *     Join                Project  B
 *    /   \                   |
 *   A     B                  A
 * </pre>
 */
public class PushDownProjectThroughSemiJoin implements ExplorationRuleFactory {
    public static final PushDownProjectThroughSemiJoin INSTANCE = new PushDownProjectThroughSemiJoin();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalJoin(logicalProject(logicalJoin().whenNot(LogicalJoin::isMarkJoin)), group())
                    .when(j -> j.left().child().getJoinType().isLeftSemiOrAntiJoin())
                    // Just pushdown project with non-column expr like (t.id + 1)
                    .whenNot(j -> j.left().isAllSlots())
                    .whenNot(j -> j.left().child().hasDistributeHint())
                    .then(topJoin -> {
                        LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topJoin.left();
                        if (projectBothJoinSide(project)) {
                            return null;
                        }
                        Plan newLeft = pushdownProject(project);
                        return topJoin.withChildren(newLeft, topJoin.right());
                    }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_LEFT),

                logicalJoin(group(), logicalProject(logicalJoin().whenNot(LogicalJoin::isMarkJoin)))
                    .when(j -> j.right().child().getJoinType().isLeftSemiOrAntiJoin())
                    // Just pushdown project with non-column expr like (t.id + 1)
                    .whenNot(j -> j.right().isAllSlots())
                    .whenNot(j -> j.right().child().hasDistributeHint())
                    .then(topJoin -> {
                        LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topJoin.right();
                        if (projectBothJoinSide(project)) {
                            return null;
                        }
                        Plan newRight = pushdownProject(project);
                        return topJoin.withChildren(topJoin.left(), newRight);
                    }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN_RIGHT)
                );
    }

    private boolean projectBothJoinSide(LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project) {
        // if project contains both side of join, it can't be pushed.
        // such as:
        //  Project(l, null as r)
        //  ------ L(l) left anti join R(r)
        LogicalJoin<?, ?> join = project.child();
        Set<Slot> projectOutput = project.getOutputSet();
        boolean containLeft = join.left().getOutput().stream().anyMatch(projectOutput::contains);
        boolean containRight = join.right().getOutput().stream().anyMatch(projectOutput::contains);
        return containRight && containLeft;
    }

    private Plan pushdownProject(LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project) {
        LogicalJoin<GroupPlan, GroupPlan> join = project.child();
        Set<Slot> conditionLeftSlots = CBOUtils.joinChildConditionSlots(join, true);

        List<NamedExpression> newProject = new ArrayList<>(project.getProjects());
        Set<Slot> projectUsedSlots = project.getProjects().stream().map(NamedExpression::toSlot)
                .collect(Collectors.toSet());
        conditionLeftSlots.stream().filter(slot -> !projectUsedSlots.contains(slot))
                .forEach(newProject::add);
        Plan newLeft = new LogicalProject<>(newProject, join.left());

        Plan newJoin = join.withChildren(newLeft, join.right());
        return new LogicalProject<>(ImmutableList.copyOf(project.getOutput()), newJoin);
    }
}
