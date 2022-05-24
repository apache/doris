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

package org.apache.doris.nereids.memo;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.clearspring.analytics.util.Lists;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Representation for group in cascades optimizer.
 */
public class Group {
    private final GroupId groupId = GroupId.newPlanSetId();

    private final List<GroupExpression> logicalPlanList = Lists.newArrayList();
    private final List<GroupExpression> physicalPlanList = Lists.newArrayList();
    private final LogicalProperties logicalProperties;

    private Map<PhysicalProperties, Pair<Double, GroupExpression>> lowestCostPlans;
    private double costLowerBound = -1;
    private boolean isExplored = false;

    /**
     * Constructor for Group.
     *
     * @param groupExpression first {@link GroupExpression} in this Group
     */
    public Group(GroupExpression groupExpression) {
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            this.logicalPlanList.add(groupExpression);
        } else {
            this.physicalPlanList.add(groupExpression);
        }
        logicalProperties = groupExpression.getParent().getLogicalProperties();
        groupExpression.setParent(this);
    }

    public GroupId getGroupId() {
        return groupId;
    }

    /**
     * Add new {@link GroupExpression} into this group.
     *
     * @param groupExpression {@link GroupExpression} to be added
     * @return added {@link GroupExpression}
     */
    public GroupExpression addGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            logicalPlanList.add(groupExpression);
        } else {
            physicalPlanList.add(groupExpression);
        }
        return groupExpression;
    }

    public double getCostLowerBound() {
        return costLowerBound;
    }

    public void setCostLowerBound(double costLowerBound) {
        this.costLowerBound = costLowerBound;
    }

    public List<GroupExpression> getLogicalPlanList() {
        return logicalPlanList;
    }

    public List<GroupExpression> getPhysicalPlanList() {
        return physicalPlanList;
    }

    public LogicalProperties getLogicalProperties() {
        return logicalProperties;
    }

    public boolean isExplored() {
        return isExplored;
    }

    public void setExplored(boolean explored) {
        isExplored = explored;
    }

    /**
     * Get the lowest cost {@link org.apache.doris.nereids.trees.plans.physical.PhysicalPlan}
     * which meeting the physical property constraints in this Group.
     *
     * @param physicalProperties the physical property constraints
     * @return {@link Optional} of cost and {@link GroupExpression} of physical plan pair.
     */
    public Optional<Pair<Double, GroupExpression>> getLowestCostPlan(PhysicalProperties physicalProperties) {
        if (physicalProperties == null || CollectionUtils.isEmpty(lowestCostPlans)) {
            return Optional.empty();
        }
        return Optional.ofNullable(lowestCostPlans.get(physicalProperties));
    }
}
