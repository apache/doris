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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Prune column for Join-Cluster
 */
@DependsRules({
        MergeProjectPostProcessor.class
})
public class ColumnPruningPostProcessor extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext ctx) {
        project = (PhysicalProject<? extends Plan>) super.visit(project, ctx);
        Plan child = project.child();
        if (project.isAllSlots()) {
            Set<Slot> projects = project.getProjects().stream().map(Slot.class::cast).collect(Collectors.toSet());
            Set<Slot> outputSet = child.getOutputSet();
            if (outputSet.equals(projects)) {
                return child;
            }
        }
        if (child instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<? extends Plan, ? extends Plan> join = (AbstractPhysicalJoin) child;
            Plan left = join.left();
            Plan right = join.right();
            Set<Slot> leftOutput = left.getOutputSet();
            Set<Slot> rightOutput = right.getOutputSet();

            Set<Slot> usedSlots = project.getProjects().stream().flatMap(ne -> ne.getInputSlots().stream())
                    .collect(Collectors.toSet());
            usedSlots.addAll(join.getConditionSlot());

            List<NamedExpression> leftNewProjections = new ArrayList<>();
            List<NamedExpression> rightNewProjections = new ArrayList<>();

            for (Slot usedSlot : usedSlots) {
                if (leftOutput.contains(usedSlot)) {
                    leftNewProjections.add(usedSlot);
                } else if (rightOutput.contains(usedSlot)) {
                    rightNewProjections.add(usedSlot);
                }
            }

            Plan newLeft;
            if (left instanceof PhysicalDistribute) {
                AbstractPhysicalPlan newPlan = new PhysicalProject<>(leftNewProjections,
                        left.getLogicalProperties(), left.child(0)).copyStatsAndGroupIdFrom(
                        (AbstractPhysicalPlan) left.child(0));
                newLeft = leftNewProjections.size() != leftOutput.size() && !leftNewProjections.isEmpty()
                        ? ((AbstractPhysicalPlan) left.withChildren(newPlan)).copyStatsAndGroupIdFrom(
                        (AbstractPhysicalPlan) left) : left;
            } else {
                AbstractPhysicalPlan newPlan = new PhysicalProject<>(leftNewProjections,
                        left.getLogicalProperties(), left).copyStatsAndGroupIdFrom((AbstractPhysicalPlan) left);
                newLeft = leftNewProjections.size() != leftOutput.size() && !leftNewProjections.isEmpty()
                        ? newPlan : left;
            }
            Plan newRight;
            if (right instanceof PhysicalDistribute) {
                AbstractPhysicalPlan newPlan = new PhysicalProject<>(rightNewProjections,
                        right.getLogicalProperties(), right.child(0)).copyStatsAndGroupIdFrom(
                        (AbstractPhysicalPlan) right.child(0));
                newRight = rightNewProjections.size() != rightOutput.size() && !rightNewProjections.isEmpty()
                        ? ((AbstractPhysicalPlan) right.withChildren(newPlan)).copyStatsAndGroupIdFrom(
                        (AbstractPhysicalPlan) right) : right;
            } else {
                AbstractPhysicalPlan newPlan = new PhysicalProject<>(rightNewProjections,
                        right.getLogicalProperties(), right).copyStatsAndGroupIdFrom((AbstractPhysicalPlan) right);
                newRight = rightNewProjections.size() != rightOutput.size() && !rightNewProjections.isEmpty()
                        ? newPlan : right;
            }

            if (newLeft != left || newRight != right) {
                AbstractPhysicalJoin oldJoin = join;
                AbstractPhysicalPlan newJoin = ((AbstractPhysicalJoin) join.withChildren(newLeft,
                        newRight)).copyStatsAndGroupIdFrom(oldJoin);
                return ((AbstractPhysicalPlan) project.withChildren(newJoin)).copyStatsAndGroupIdFrom(project);
            } else {
                return project;
            }
        }
        return project;
    }
}

