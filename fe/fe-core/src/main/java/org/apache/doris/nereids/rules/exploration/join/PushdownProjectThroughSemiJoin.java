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
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * rule for pushdown project through left-semi/anti join
 */
public class PushdownProjectThroughSemiJoin extends OneExplorationRuleFactory {
    public static final PushdownProjectThroughSemiJoin INSTANCE = new PushdownProjectThroughSemiJoin();

    /*
     *    Project                   Join
     *      |            ──►       /   \
     *     Join                Project  B
     *    /   \                   |
     *   A     B                  A
     */
    @Override
    public Rule build() {
        return logicalProject(logicalJoin())
            .when(project -> project.child().getJoinType().isLeftSemiOrAntiJoin())
            // Just pushdown project with non-column expr like (t.id + 1)
            .whenNot(LogicalProject::isAllSlots)
            .whenNot(project -> project.child().hasJoinHint())
            .then(project -> {
                LogicalJoin<GroupPlan, GroupPlan> join = project.child();
                Set<Slot> conditionLeftSlots = CBOUtils.joinChildConditionSlots(join, true);

                List<NamedExpression> newProject = new ArrayList<>(project.getProjects());
                Set<Slot> projectUsedSlots = project.getProjects().stream().map(NamedExpression::toSlot)
                        .collect(Collectors.toSet());
                conditionLeftSlots.stream().filter(slot -> !projectUsedSlots.contains(slot)).forEach(newProject::add);
                Plan newLeft = CBOUtils.projectOrSelf(newProject, join.left());

                Plan newJoin = join.withChildrenNoContext(newLeft, join.right());
                return CBOUtils.projectOrSelf(new ArrayList<>(project.getOutput()), newJoin);
            }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_SEMI_JOIN);
    }

    List<NamedExpression> sort(List<NamedExpression> projects, Plan sortPlan) {
        List<ExprId> orderExprIds = sortPlan.getOutput().stream().map(Slot::getExprId).collect(Collectors.toList());
        // map { project input slot expr id -> project output expr }
        Map<ExprId, NamedExpression> map = projects.stream()
                .collect(Collectors.toMap(expr -> expr.getInputSlots().iterator().next().getExprId(), expr -> expr));
        List<NamedExpression> newProjects = new ArrayList<>();
        for (ExprId exprId : orderExprIds) {
            if (map.containsKey(exprId)) {
                newProjects.add(map.get(exprId));
            }
        }
        return newProjects;
    }
}
