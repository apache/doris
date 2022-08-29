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


package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.GroupExpression;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * When parent node request some properties but children don't have.
 * Enforce add missing properties for child.
 */
public class EnforceMissingPropertiesHelper {

    private JobContext context;
    private GroupExpression groupExpression;
    private double curTotalCost;

    public EnforceMissingPropertiesHelper(JobContext context, GroupExpression groupExpression,
            double curTotalCost) {
        this.context = context;
        this.groupExpression = groupExpression;
        this.curTotalCost = curTotalCost;
    }

    public double getCurTotalCost() {
        return curTotalCost;
    }

    /**
     * Enforce missing property.
     */
    public PhysicalProperties enforceProperty(PhysicalProperties output, PhysicalProperties request) {
        boolean isSatistyOrder = output.getOrderSpec().satisfy(request.getOrderSpec());
        boolean isSatistyDistribution = output.getDistributionSpec().satisfy(request.getDistributionSpec());

        if (!isSatistyDistribution && !isSatistyOrder) {
            return enforceSortAndDistribution(output, request);
        } else if (isSatistyDistribution && isSatistyOrder) {
            Preconditions.checkState(false, "can't reach here.");
            return null;
        } else if (!isSatistyDistribution) {
            if (!request.getOrderSpec().getOrderKeys().isEmpty()) {
                // After redistribute data , original order request may be wrong.
                return enforceDistributionButMeetSort(output, request);
            }
            return enforceDistribution(output);
        } else {
            // Order don't satisfy.
            return enforceSort(output);
        }
    }

    /**
     * When requestProperty include sort, enforce distribution may break the original sort.
     * <p>
     * But if we additonal enforce sort, it may cause infinite loop.
     * <p>
     * hackly, use {[empty order], Any} to eliminate the original property.
     */
    private PhysicalProperties enforceDistributionButMeetSort(PhysicalProperties output, PhysicalProperties request) {
        groupExpression.getOwnerGroup()
                .replaceBestPlan(output, PhysicalProperties.ANY, groupExpression.getCost(output));
        return enforceSortAndDistribution(output, request);
    }

    private PhysicalProperties enforceSort(PhysicalProperties oldOutputProperty) {
        // keep consistent in DistributionSpec with the oldOutputProperty
        PhysicalProperties newOutputProperty = new PhysicalProperties(
                oldOutputProperty.getDistributionSpec(),
                context.getRequiredProperties().getOrderSpec());
        GroupExpression enforcer =
                context.getRequiredProperties().getOrderSpec().addEnforcer(groupExpression.getOwnerGroup());

        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceDistribution(PhysicalProperties oldOutputProperty) {
        PhysicalProperties newOutputProperty = new PhysicalProperties(
                context.getRequiredProperties().getDistributionSpec(),
                oldOutputProperty.getOrderSpec());
        GroupExpression enforcer =
                context.getRequiredProperties().getDistributionSpec().addEnforcer(groupExpression.getOwnerGroup());

        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceSortAndDistribution(PhysicalProperties outputProperty,
            PhysicalProperties requiredProperty) {
        PhysicalProperties enforcedProperty;
        if (requiredProperty.getDistributionSpec().equals(new DistributionSpecGather())) {
            enforcedProperty = enforceSort(outputProperty);
            enforcedProperty = enforceDistribution(enforcedProperty);
        } else {
            enforcedProperty = enforceDistribution(outputProperty);
            enforcedProperty = enforceSort(enforcedProperty);
        }

        return enforcedProperty;
    }

    /**
     * Add enforcer plan, update cost, update property of enforcer, and setBestPlan
     */
    private void addEnforcerUpdateCost(GroupExpression enforcer,
            PhysicalProperties oldOutputProperty,
            PhysicalProperties newOutputProperty) {
        context.getPlannerContext().getMemo().addEnforcerPlan(enforcer, groupExpression.getOwnerGroup());
        curTotalCost += CostCalculator.calculateCost(enforcer);

        if (enforcer.updateLowestCostTable(newOutputProperty, Lists.newArrayList(oldOutputProperty), curTotalCost)) {
            enforcer.putOutputPropertiesMap(newOutputProperty, newOutputProperty);
        }
        groupExpression.getOwnerGroup().setBestPlan(enforcer, curTotalCost, newOutputProperty);
    }
}
