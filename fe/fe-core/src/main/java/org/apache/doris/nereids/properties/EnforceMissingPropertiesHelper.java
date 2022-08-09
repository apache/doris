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

import com.google.common.collect.Lists;

/**
 * When parent node request some properties but children don't have.
 * Enforce add missing properties for child.
 */
public class EnforceMissingPropertiesHelper {

    private final JobContext context;
    private final GroupExpression groupExpression;
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
        boolean isSatisfyOrder = output.getOrderSpec().satisfy(request.getOrderSpec());
        boolean isSatisfyDistribution = output.getDistributionSpec().satisfy(request.getDistributionSpec());

        if (!isSatisfyDistribution && !isSatisfyOrder) {
            return enforceSortAndDistribution(output, request);
        } else if (isSatisfyDistribution && isSatisfyOrder) {
            return output;
        } else if (!isSatisfyDistribution) {
            if (!request.getOrderSpec().getOrderKeys().isEmpty()) {
                // After redistribute data , original order request may be wrong.
                return enforceDistributionButMeetSort(output, request);
            }
            return enforceDistribution(output);
        } else {
            // Order don't satisfy.
            return enforceLocalSort(output);
        }
    }

    /**
     * When requestProperty include sort, enforce distribution may break the original sort.
     * <p>
     * But if we add enforce sort, it may cause infinite loop.
     * <p>
     * trick, use {[empty order], Any} to eliminate the original property.
     */
    private PhysicalProperties enforceDistributionButMeetSort(PhysicalProperties output, PhysicalProperties request) {
        groupExpression.getOwnerGroup()
                .replaceBestPlan(output, PhysicalProperties.ANY, groupExpression.getCost(output));
        return enforceSortAndDistribution(output, request);
    }

    private PhysicalProperties enforceGlobalSort(PhysicalProperties oldOutputProperty) {
        // keep consistent in DistributionSpec with the oldOutputProperty
        PhysicalProperties newOutputProperty = new PhysicalProperties(context.getRequiredProperties().getOrderSpec());
        GroupExpression enforcer =
                context.getRequiredProperties().getOrderSpec().addGlobalEnforcer(groupExpression.getOwnerGroup());

        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceLocalSort(PhysicalProperties oldOutputProperty) {
        // keep consistent in DistributionSpec with the oldOutputProperty
        PhysicalProperties newOutputProperty = new PhysicalProperties(
                oldOutputProperty.getDistributionSpec(), context.getRequiredProperties().getOrderSpec());
        GroupExpression enforcer =
                context.getRequiredProperties().getOrderSpec().addLocalEnforcer(groupExpression.getOwnerGroup());

        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceDistribution(PhysicalProperties oldOutputProperty) {
        PhysicalProperties newOutputProperty = new PhysicalProperties(
                context.getRequiredProperties().getDistributionSpec());
        GroupExpression enforcer =
                context.getRequiredProperties().getDistributionSpec().addEnforcer(groupExpression.getOwnerGroup());

        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceSortAndDistribution(PhysicalProperties outputProperty,
            PhysicalProperties requiredProperty) {
        PhysicalProperties enforcedProperty;
        if (requiredProperty.getDistributionSpec().equals(new DistributionSpecGather())) {
            enforcedProperty = enforceGlobalSort(outputProperty);
        } else {
            enforcedProperty = enforceDistribution(outputProperty);
            enforcedProperty = enforceLocalSort(enforcedProperty);
        }

        return enforcedProperty;
    }

    /**
     * Add enforcer plan, update cost, update property of enforcer, and setBestPlan
     */
    private void addEnforcerUpdateCost(GroupExpression enforcer,
            PhysicalProperties oldOutputProperty,
            PhysicalProperties newOutputProperty) {
        context.getCascadesContext().getMemo().addEnforcerPlan(enforcer, groupExpression.getOwnerGroup());
        curTotalCost += CostCalculator.calculateCost(enforcer);

        if (enforcer.updateLowestCostTable(newOutputProperty, Lists.newArrayList(oldOutputProperty), curTotalCost)) {
            enforcer.putOutputPropertiesMap(newOutputProperty, newOutputProperty);
        }
        groupExpression.getOwnerGroup().setBestPlan(enforcer, curTotalCost, newOutputProperty);
    }
}
