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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.GroupExpression;

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

    /**
     * Enforce missing property.
     */
    public Pair<PhysicalProperties, Double> enforceProperty(PhysicalProperties output, PhysicalProperties required) {
        boolean isMeetOrder = output.getOrderSpec().meet(required.getOrderSpec());
        boolean isMeetDistribution = output.getDistributionSpec().satisfy(required.getDistributionSpec());

        if (!isMeetDistribution && !isMeetOrder) {
            // Both Distribution and Order don't satisfy.
            return new Pair<>(enforceSortAndDistribution(output, required), curTotalCost);
        } else if (isMeetDistribution && isMeetOrder) {
            // Both satisfy.
            // TODO: can't reach here.
            return new Pair<>(null, curTotalCost);
        } else if (!isMeetDistribution) {
            // Distribution don't satisfy.
            if (required.getOrderSpec().getOrderKeys().isEmpty()) {
                return new Pair<>(enforceDistribution(output), curTotalCost);
            } else {
                // TODO
                // It's wrong that SortSpec is empty.
                // After redistribute data , original order requirement may be wrong. Need enforce "SortNode" here.
                // PhysicalProperties newProperty =
                //         new PhysicalProperties(new DistributionSpec(), new OrderSpec(Lists.newArrayList()));
                // groupExpression.getParent().
                // return enforceSortAndDistribution(newProperty, required);
                return new Pair<>(enforceDistribution(output), curTotalCost);
            }
        } else {
            // Order don't satisfy.
            return new Pair<>(enforceSort(output), curTotalCost);
        }
    }

    private PhysicalProperties enforceSort(PhysicalProperties oldOutputProperty) {
        // clone
        PhysicalProperties newOutputProperty = new PhysicalProperties(
                oldOutputProperty.getDistributionSpec(),
                context.getRequiredProperties().getOrderSpec());
        GroupExpression enforcer =
                context.getRequiredProperties().getOrderSpec().addEnforcer(groupExpression.getOwnerGroup());

        updateCostWithEnforcer(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceDistribution(PhysicalProperties oldOutputProperty) {
        PhysicalProperties newOutputProperty = new PhysicalProperties(
                context.getRequiredProperties().getDistributionSpec(),
                oldOutputProperty.getOrderSpec());
        GroupExpression enforcer =
                context.getRequiredProperties().getDistributionSpec().addEnforcer(groupExpression.getOwnerGroup());

        updateCostWithEnforcer(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private void updateCostWithEnforcer(GroupExpression enforcer,
            PhysicalProperties oldOutputProperty,
            PhysicalProperties newOutputProperty) {
        context.getPlannerContext().getMemo().addEnforcerPlan(enforcer, groupExpression.getOwnerGroup());
        curTotalCost += CostCalculator.calculateCost(enforcer);

        if (enforcer.updateLowestCostTable(newOutputProperty, Lists.newArrayList(oldOutputProperty), curTotalCost)) {
            enforcer.putOutputPropertiesMap(newOutputProperty, newOutputProperty);
        }
        groupExpression.getOwnerGroup().setBestPlan(enforcer, curTotalCost, newOutputProperty);
    }

    private PhysicalProperties enforceSortAndDistribution(PhysicalProperties outputProperty,
            PhysicalProperties requiredProperty) {
        PhysicalProperties enforcedProperty;
        if (requiredProperty.getDistributionSpec()
                .equals(new DistributionSpecGather())) {
            enforcedProperty = enforceSort(outputProperty);
            enforcedProperty = enforceDistribution(enforcedProperty);
        } else {
            enforcedProperty = enforceDistribution(outputProperty);
            enforcedProperty = enforceSort(enforcedProperty);
        }

        return enforcedProperty;
    }
}
