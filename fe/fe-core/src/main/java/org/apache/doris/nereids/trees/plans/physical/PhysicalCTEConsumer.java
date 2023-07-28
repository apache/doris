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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical CTE consumer.
 */
public class PhysicalCTEConsumer extends PhysicalRelation {

    private final CTEId cteId;
    private final Map<Slot, Slot> producerToConsumerSlotMap;
    private final Map<Slot, Slot> consumerToProducerSlotMap;

    /**
     * Constructor
     */
    public PhysicalCTEConsumer(RelationId relationId, CTEId cteId, Map<Slot, Slot> consumerToProducerSlotMap,
            Map<Slot, Slot> producerToConsumerSlotMap, LogicalProperties logicalProperties) {
        this(relationId, cteId, consumerToProducerSlotMap, producerToConsumerSlotMap,
                Optional.empty(), logicalProperties);
    }

    /**
     * Constructor
     */
    public PhysicalCTEConsumer(RelationId relationId, CTEId cteId,
            Map<Slot, Slot> consumerToProducerSlotMap, Map<Slot, Slot> producerToConsumerSlotMap,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        this(relationId, cteId, consumerToProducerSlotMap, producerToConsumerSlotMap,
                groupExpression, logicalProperties, null, null);
    }

    /**
     * Constructor
     */
    public PhysicalCTEConsumer(RelationId relationId, CTEId cteId, Map<Slot, Slot> consumerToProducerSlotMap,
            Map<Slot, Slot> producerToConsumerSlotMap, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties, Statistics statistics) {
        super(relationId, PlanType.PHYSICAL_CTE_CONSUMER, groupExpression,
                logicalProperties, physicalProperties, statistics);
        this.cteId = cteId;
        this.consumerToProducerSlotMap = ImmutableMap.copyOf(Objects.requireNonNull(
                consumerToProducerSlotMap, "consumerToProducerSlotMap should not null"));
        this.producerToConsumerSlotMap = ImmutableMap.copyOf(Objects.requireNonNull(
                producerToConsumerSlotMap, "consumerToProducerSlotMap should not null"));
    }

    public CTEId getCteId() {
        return cteId;
    }

    public Map<Slot, Slot> getProducerToConsumerSlotMap() {
        return producerToConsumerSlotMap;
    }

    public Slot getProducerSlot(Slot consumerSlot) {
        Slot slot = consumerToProducerSlotMap.get(consumerSlot);
        Preconditions.checkArgument(slot != null, String.format(
                "Required producer slot for %s doesn't exist", consumerSlot));
        return slot;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalCTEConsumer[" + id.asInt() + "]",
                "cteId", cteId);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEConsumer(this, context);
    }

    @Override
    public PhysicalCTEConsumer withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalCTEConsumer(relationId, cteId,
                consumerToProducerSlotMap, producerToConsumerSlotMap,
                groupExpression, getLogicalProperties());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalCTEConsumer(relationId, cteId,
                consumerToProducerSlotMap, producerToConsumerSlotMap,
                groupExpression, logicalProperties.get());
    }

    @Override
    public PhysicalCTEConsumer withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalCTEConsumer(relationId, cteId,
                consumerToProducerSlotMap, producerToConsumerSlotMap,
                groupExpression, getLogicalProperties(), physicalProperties, statistics);
    }

    @Override
    public String shapeInfo() {
        return Utils.toSqlString("PhysicalCteConsumer",
                "cteId", cteId);
    }

    @Override
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
                                         AbstractPhysicalJoin builderNode,
                                         Expression src, Expression probeExpr,
                                         TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        // push down rf on cte sender
        // TODO: refactor pushing down into cte internal here
        return super.pushDownRuntimeFilter(context, generator, builderNode,
                src, probeExpr, type, buildSideNdv, exprOrder);
    }
}
