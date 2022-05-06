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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;

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

    private final List<PlanReference> logicalPlanList = Lists.newArrayList();
    private final List<PlanReference> physicalPlanList = Lists.newArrayList();
    private final LogicalProperties logicalProperties;

    private Map<PhysicalProperties, Pair<Double, PlanReference>> lowestCostPlans;
    private double costLowerBound = -1;
    private boolean isExplored = false;

    /**
     * Constructor for Group.
     *
     * @param planReference first {@link PlanReference} in this Group
     */
    public Group(PlanReference planReference) {
        if (planReference.getPlan().isLogical()) {
            this.logicalPlanList.add(planReference);
        } else {
            this.physicalPlanList.add(planReference);
        }
        logicalProperties = new LogicalProperties();
        try {
            logicalProperties.setOutput(planReference.getPlan().getOutput());
        } catch (UnboundException e) {
            throw new RuntimeException(e);
        }
        planReference.setParent(this);
    }

    public GroupId getGroupId() {
        return groupId;
    }

    /**
     * Add new {@link PlanReference} into this group.
     *
     * @param planReference {@link PlanReference} to be added
     * @return added {@link PlanReference}
     */
    public PlanReference addPlanReference(PlanReference planReference) {
        if (planReference.getPlan().isLogical()) {
            logicalPlanList.add(planReference);
        } else {
            physicalPlanList.add(planReference);
        }
        return planReference;
    }

    public double getCostLowerBound() {
        return costLowerBound;
    }

    public void setCostLowerBound(double costLowerBound) {
        this.costLowerBound = costLowerBound;
    }

    public List<PlanReference> getLogicalPlanList() {
        return logicalPlanList;
    }

    public List<PlanReference> getPhysicalPlanList() {
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
     * @return {@link Optional} of cost and {@link PlanReference} of physical plan pair.
     */
    public Optional<Pair<Double, PlanReference>> getLowestCostPlan(PhysicalProperties physicalProperties) {
        if (physicalProperties == null || CollectionUtils.isEmpty(lowestCostPlans)) {
            return Optional.empty();
        }
        return Optional.ofNullable(lowestCostPlans.get(physicalProperties));
    }
}
