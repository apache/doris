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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan
 */
public class PlanReceiver implements AbstractReceiver {
    // limit define the max number of csg-cmp pair in this Receiver
    HashMap<Long, Group> planTable = new HashMap<>();
    int limit;
    int emitCount = 0;

    public PlanReceiver() {
        limit = Integer.MAX_VALUE;
    }

    public PlanReceiver(int limit) {
        this.limit = limit;
    }

    /**
     * Emit a new plan from bottom to top
     *
     * @param left the bitmap of left child tree
     * @param right the bitmap of the right child tree
     * @param edges the join conditions that can be added in this operator
     * @return the left and the right can be connected by the edge
     */
    @Override
    public boolean emitCsgCmp(long left, long right, List<Edge> edges,
            HashMap<Long, NamedExpression> projectExpression) {
        Preconditions.checkArgument(planTable.containsKey(left));
        Preconditions.checkArgument(planTable.containsKey(right));
        emitCount += 1;
        if (emitCount > limit) {
            return false;
        }
        long fullKey = LongBitmap.newBitmapUnion(left, right);
        Group group1 = constructGroup(left, right, edges);
        Group group2 = constructGroup(right, left, edges);
        Group winnerGroup;
        if (group1.getLogicalExpression().getCostByProperties(PhysicalProperties.ANY) < group2.getLogicalExpression()
                .getCostByProperties(PhysicalProperties.ANY)) {
            winnerGroup = group1;
        } else {
            winnerGroup = group2;
        }

        if (!planTable.containsKey(fullKey)
                || planTable.get(fullKey).getLogicalExpression().getCostByProperties(PhysicalProperties.ANY)
                > winnerGroup.getLogicalExpression().getCostByProperties(PhysicalProperties.ANY)) {
            // When we decide to store the new Plan, we need to add the complex project to it.
            winnerGroup = tryAddProject(winnerGroup, projectExpression, fullKey);
            planTable.put(fullKey, winnerGroup);
        }
        return true;

    }

    @Override
    public void addGroup(long bitmap, Group group) {
        planTable.put(bitmap, group);
    }

    @Override
    public boolean contain(long bitmap) {
        return planTable.containsKey(bitmap);
    }

    @Override
    public void reset() {
        planTable.clear();
        emitCount = 0;
    }

    @Override
    public Group getBestPlan(long bitmap) {
        Preconditions.checkArgument(planTable.containsKey(bitmap));
        return planTable.get(bitmap);
    }

    private double getSimpleCost(Group group) {
        if (!group.isJoinGroup()) {
            return group.getStatistics().getRowCount();
        }
        return group.getLogicalExpression().getCostByProperties(PhysicalProperties.ANY);
    }

    private Group tryAddProject(Group group, HashMap<Long, NamedExpression> projectExpression, long fullKey) {
        List<NamedExpression> projects = new ArrayList<>();
        List<Long> removedKey = new ArrayList<>();
        for (Long bitmap : projectExpression.keySet()) {
            if (LongBitmap.isSubset(bitmap, fullKey)) {
                NamedExpression namedExpression = projectExpression.get(bitmap);
                projects.add(namedExpression);
                removedKey.add(bitmap);
            }
        }
        for (Long bitmap : removedKey) {
            projectExpression.remove(bitmap);
        }
        if (projects.size() != 0) {
            LogicalProject logicalProject = new LogicalProject<>(projects,
                    group.getLogicalExpression().getPlan());
            GroupExpression groupExpression = new GroupExpression(logicalProject, Lists.newArrayList(group));
            groupExpression.updateLowestCostTable(PhysicalProperties.ANY,
                    Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.ANY),
                    group.getLogicalExpression().getCostByProperties(PhysicalProperties.ANY));
            Group projectGroup = new Group();
            projectGroup.addGroupExpression(groupExpression);
            StatsCalculator.estimate(groupExpression);
            return projectGroup;
        }
        return group;
    }

    private Group constructGroup(long left, long right, List<Edge> edges) {
        Preconditions.checkArgument(planTable.containsKey(left));
        Preconditions.checkArgument(planTable.containsKey(right));
        Group leftGroup = planTable.get(left);
        Group rightGroup = planTable.get(right);

        double cost = getSimpleCost(leftGroup) + getSimpleCost(rightGroup);
        List<Expression> conditions = new ArrayList<>();
        for (Edge edge : edges) {
            conditions.addAll(edge.getJoin().getExpressions());
        }
        LogicalJoin newJoin = new LogicalJoin(edges.get(0).getJoin().getJoinType(), conditions,
                leftGroup.getLogicalExpression().getPlan(),
                rightGroup.getLogicalExpression().getPlan());

        GroupExpression groupExpression = new GroupExpression(newJoin, Lists.newArrayList(leftGroup, rightGroup));
        Group group = new Group();
        group.addGroupExpression(groupExpression);
        StatsCalculator.estimate(groupExpression);
        cost += group.getStatistics().getRowCount();

        groupExpression.updateLowestCostTable(PhysicalProperties.ANY,
                Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.ANY), cost);
        return group;
    }
}

