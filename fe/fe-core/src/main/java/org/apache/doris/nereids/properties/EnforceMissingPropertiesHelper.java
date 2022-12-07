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
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.EnforcerEvent;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import com.google.common.collect.Lists;

/**
 * When parent node request some properties but children don't have.
 * Enforce add missing properties for child.
 */
public class EnforceMissingPropertiesHelper {
    private static final EventProducer ENFORCER_TRACER = new EventProducer(EnforcerEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(EnforcerEvent.class, EventChannel.LOG)));
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
    public PhysicalProperties enforceProperty(PhysicalProperties output, PhysicalProperties required) {
        boolean isSatisfyOrder = output.getOrderSpec().satisfy(required.getOrderSpec());
        boolean isSatisfyDistribution = output.getDistributionSpec().satisfy(required.getDistributionSpec());
        if (isSatisfyDistribution && isSatisfyOrder) {
            return output;
        }

        if (!isSatisfyDistribution && !isSatisfyOrder) {
            return enforceSortAndDistribution(output, required);
        }
        if (!isSatisfyOrder) {
            return enforceLocalSort(output, required);
        }
        if (!required.getOrderSpec().getOrderKeys().isEmpty()) {
            // After redistribute data , original order required may be wrong.
            return enforceDistributionButMeetSort(output, required);
        }
        return enforceDistribution(output, required);
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
                .replaceBestPlan(output, PhysicalProperties.ANY, groupExpression.getCostByProperties(output));
        return enforceSortAndDistribution(output, request);
    }

    private PhysicalProperties enforceGlobalSort(PhysicalProperties oldOutputProperty, PhysicalProperties required) {
        // keep consistent in DistributionSpec with the oldOutputProperty
        PhysicalProperties newOutputProperty = new PhysicalProperties(required.getOrderSpec());
        GroupExpression enforcer = required.getOrderSpec().addGlobalQuickSortEnforcer(groupExpression.getOwnerGroup());

        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceLocalSort(PhysicalProperties oldOutputProperty, PhysicalProperties required) {
        // keep consistent in DistributionSpec with the oldOutputProperty
        PhysicalProperties newOutputProperty = new PhysicalProperties(
                oldOutputProperty.getDistributionSpec(), required.getOrderSpec());
        GroupExpression enforcer = required.getOrderSpec().addLocalQuickSortEnforcer(groupExpression.getOwnerGroup());

        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalProperties enforceDistribution(PhysicalProperties oldOutputProperty, PhysicalProperties required) {
        DistributionSpec outputDistributionSpec;
        DistributionSpec requiredDistributionSpec = required.getDistributionSpec();
        if (requiredDistributionSpec instanceof DistributionSpecHash) {
            DistributionSpecHash requiredDistributionSpecHash = (DistributionSpecHash) requiredDistributionSpec;
            outputDistributionSpec = requiredDistributionSpecHash.withShuffleType(ShuffleType.ENFORCED);
        } else {
            outputDistributionSpec = requiredDistributionSpec;
        }

        PhysicalProperties newOutputProperty = new PhysicalProperties(outputDistributionSpec);
        GroupExpression enforcer = outputDistributionSpec.addEnforcer(groupExpression.getOwnerGroup());
        addEnforcerUpdateCost(enforcer, oldOutputProperty, newOutputProperty);
        return newOutputProperty;
    }

    private PhysicalProperties enforceSortAndDistribution(PhysicalProperties outputProperty,
            PhysicalProperties requiredProperty) {
        PhysicalProperties enforcedProperty;
        if (requiredProperty.getDistributionSpec().equals(new DistributionSpecGather())) {
            enforcedProperty = enforceGlobalSort(outputProperty, requiredProperty);
        } else {
            enforcedProperty = enforceDistribution(outputProperty, requiredProperty);
            enforcedProperty = enforceLocalSort(enforcedProperty, requiredProperty);
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
        ENFORCER_TRACER.log(EnforcerEvent.of(groupExpression, ((PhysicalPlan) enforcer.getPlan()),
                oldOutputProperty, newOutputProperty));
        curTotalCost += CostCalculator.calculateCost(enforcer);

        if (enforcer.updateLowestCostTable(newOutputProperty,
                Lists.newArrayList(oldOutputProperty), curTotalCost)) {
            enforcer.putOutputPropertiesMap(newOutputProperty, newOutputProperty);
        }
        groupExpression.getOwnerGroup().setBestPlan(enforcer, curTotalCost, newOutputProperty);
    }
}
