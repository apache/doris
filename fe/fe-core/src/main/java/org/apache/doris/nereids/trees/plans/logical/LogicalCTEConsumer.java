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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalCTEConsumer
 */
//TODO: find cte producer and propagate its functional dependencies
public class LogicalCTEConsumer extends LogicalRelation implements BlockFuncDepsPropagation {

    private final String name;
    private final CTEId cteId;
    private final Map<Slot, Slot> consumerToProducerOutputMap;
    private final Map<Slot, Slot> producerToConsumerOutputMap;

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(RelationId relationId, CTEId cteId, String name,
            Map<Slot, Slot> consumerToProducerOutputMap, Map<Slot, Slot> producerToConsumerOutputMap) {
        super(relationId, PlanType.LOGICAL_CTE_CONSUMER, Optional.empty(), Optional.empty());
        this.cteId = Objects.requireNonNull(cteId, "cteId should not null");
        this.name = Objects.requireNonNull(name, "name should not null");
        this.consumerToProducerOutputMap = Objects.requireNonNull(consumerToProducerOutputMap,
                "consumerToProducerOutputMap should not null");
        this.producerToConsumerOutputMap = Objects.requireNonNull(producerToConsumerOutputMap,
                "producerToConsumerOutputMap should not null");
    }

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(RelationId relationId, CTEId cteId, String name, LogicalPlan producerPlan) {
        super(relationId, PlanType.LOGICAL_CTE_CONSUMER, Optional.empty(), Optional.empty());
        this.cteId = Objects.requireNonNull(cteId, "cteId should not null");
        this.name = Objects.requireNonNull(name, "name should not null");
        this.consumerToProducerOutputMap = new LinkedHashMap<>();
        this.producerToConsumerOutputMap = new LinkedHashMap<>();
        initOutputMaps(producerPlan);
    }

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(RelationId relationId, CTEId cteId, String name,
            Map<Slot, Slot> consumerToProducerOutputMap, Map<Slot, Slot> producerToConsumerOutputMap,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, PlanType.LOGICAL_CTE_CONSUMER, groupExpression, logicalProperties);
        this.cteId = Objects.requireNonNull(cteId, "cteId should not null");
        this.name = Objects.requireNonNull(name, "name should not null");
        this.consumerToProducerOutputMap = Objects.requireNonNull(consumerToProducerOutputMap,
                "consumerToProducerOutputMap should not null");
        this.producerToConsumerOutputMap = Objects.requireNonNull(producerToConsumerOutputMap,
                "producerToConsumerOutputMap should not null");
    }

    private void initOutputMaps(LogicalPlan childPlan) {
        List<Slot> producerOutput = childPlan.getOutput();
        for (Slot producerOutputSlot : producerOutput) {
            SlotReference slotRef =
                    producerOutputSlot instanceof SlotReference ? (SlotReference) producerOutputSlot : null;
            Slot consumerSlot = new SlotReference(StatementScopeIdGenerator.newExprId(),
                    producerOutputSlot.getName(), producerOutputSlot.getDataType(),
                    producerOutputSlot.nullable(), ImmutableList.of(name),
                    slotRef != null ? (slotRef.getColumn().isPresent() ? slotRef.getColumn().get() : null) : null,
                    slotRef != null ? Optional.of(slotRef.getInternalName()) : Optional.empty());
            producerToConsumerOutputMap.put(producerOutputSlot, consumerSlot);
            consumerToProducerOutputMap.put(consumerSlot, producerOutputSlot);
        }
    }

    public Map<Slot, Slot> getConsumerToProducerOutputMap() {
        return consumerToProducerOutputMap;
    }

    public Map<Slot, Slot> getProducerToConsumerOutputMap() {
        return producerToConsumerOutputMap;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEConsumer(this, context);
    }

    public Plan withTwoMaps(Map<Slot, Slot> consumerToProducerOutputMap, Map<Slot, Slot> producerToConsumerOutputMap) {
        return new LogicalCTEConsumer(relationId, cteId, name,
                consumerToProducerOutputMap, producerToConsumerOutputMap,
                Optional.empty(), Optional.empty());
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTEConsumer(relationId, cteId, name,
                consumerToProducerOutputMap, producerToConsumerOutputMap,
                groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalCTEConsumer(relationId, cteId, name,
                consumerToProducerOutputMap, producerToConsumerOutputMap,
                groupExpression, logicalProperties);
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.copyOf(producerToConsumerOutputMap.values());
    }

    public CTEId getCteId() {
        return cteId;
    }

    public String getName() {
        return name;
    }

    public Slot getProducerSlot(Slot consumerSlot) {
        Slot slot = consumerToProducerOutputMap.get(consumerSlot);
        Preconditions.checkArgument(slot != null, String.format("Required producer"
                + "slot for :%s doesn't exist", consumerSlot));
        return slot;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCteConsumer[" + id.asInt() + "]",
                "cteId", cteId,
                "relationId", relationId,
                "name", name);
    }
}
