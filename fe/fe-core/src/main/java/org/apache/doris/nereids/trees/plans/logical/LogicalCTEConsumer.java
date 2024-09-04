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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalCTEConsumer
 */
//TODO: find cte producer and propagate its functional dependencies
public class LogicalCTEConsumer extends LogicalRelation implements BlockFuncDepsPropagation, OutputPrunable {

    private final String name;
    private final CTEId cteId;
    private final Map<Slot, Slot> consumerToProducerOutputMap;
    private final Multimap<Slot, Slot> producerToConsumerOutputMap;

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(RelationId relationId, CTEId cteId, String name,
            Map<Slot, Slot> consumerToProducerOutputMap, Multimap<Slot, Slot> producerToConsumerOutputMap) {
        this(relationId, cteId, name, consumerToProducerOutputMap, producerToConsumerOutputMap,
                Optional.empty(), Optional.empty());
    }

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(RelationId relationId, CTEId cteId, String name, LogicalPlan producerPlan) {
        super(relationId, PlanType.LOGICAL_CTE_CONSUMER, Optional.empty(), Optional.empty());
        this.cteId = Objects.requireNonNull(cteId, "cteId should not null");
        this.name = Objects.requireNonNull(name, "name should not null");
        ImmutableMap.Builder<Slot, Slot> cToPBuilder = ImmutableMap.builder();
        ImmutableMultimap.Builder<Slot, Slot> pToCBuilder = ImmutableMultimap.builder();
        List<Slot> producerOutput = producerPlan.getOutput();
        for (Slot producerOutputSlot : producerOutput) {
            Slot consumerSlot = generateConsumerSlot(this.name, producerOutputSlot);
            cToPBuilder.put(consumerSlot, producerOutputSlot);
            pToCBuilder.put(producerOutputSlot, consumerSlot);
        }
        consumerToProducerOutputMap = cToPBuilder.build();
        producerToConsumerOutputMap = pToCBuilder.build();
    }

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(RelationId relationId, CTEId cteId, String name,
            Map<Slot, Slot> consumerToProducerOutputMap, Multimap<Slot, Slot> producerToConsumerOutputMap,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, PlanType.LOGICAL_CTE_CONSUMER, groupExpression, logicalProperties);
        this.cteId = Objects.requireNonNull(cteId, "cteId should not null");
        this.name = Objects.requireNonNull(name, "name should not null");
        this.consumerToProducerOutputMap = ImmutableMap.copyOf(Objects.requireNonNull(consumerToProducerOutputMap,
                "consumerToProducerOutputMap should not null"));
        this.producerToConsumerOutputMap = ImmutableMultimap.copyOf(Objects.requireNonNull(producerToConsumerOutputMap,
                "producerToConsumerOutputMap should not null"));
    }

    /**
     * generate a consumer slot mapping from producer slot.
     */
    public static SlotReference generateConsumerSlot(String cteName, Slot producerOutputSlot) {
        SlotReference slotRef =
                producerOutputSlot instanceof SlotReference ? (SlotReference) producerOutputSlot : null;
        return new SlotReference(StatementScopeIdGenerator.newExprId(),
                producerOutputSlot.getName(), producerOutputSlot.getDataType(),
                producerOutputSlot.nullable(), ImmutableList.of(cteName),
                slotRef != null ? (slotRef.getTable().isPresent() ? slotRef.getTable().get() : null) : null,
                slotRef != null ? (slotRef.getColumn().isPresent() ? slotRef.getColumn().get() : null) : null,
                slotRef != null ? Optional.of(slotRef.getInternalName()) : Optional.empty());
    }

    public Map<Slot, Slot> getConsumerToProducerOutputMap() {
        return consumerToProducerOutputMap;
    }

    public Multimap<Slot, Slot> getProducerToConsumerOutputMap() {
        return producerToConsumerOutputMap;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEConsumer(this, context);
    }

    public Plan withTwoMaps(Map<Slot, Slot> consumerToProducerOutputMap,
            Multimap<Slot, Slot> producerToConsumerOutputMap) {
        return new LogicalCTEConsumer(relationId, cteId, name,
                consumerToProducerOutputMap, producerToConsumerOutputMap);
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
    public LogicalCTEConsumer withRelationId(RelationId relationId) {
        throw new RuntimeException("should not call LogicalCTEConsumer's withRelationId method");
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.copyOf(producerToConsumerOutputMap.values());
    }

    @Override
    public Plan pruneOutputs(List<NamedExpression> prunedOutputs) {
        Map<Slot, Slot> consumerToProducerOutputMap = new LinkedHashMap<>(this.consumerToProducerOutputMap.size());
        Multimap<Slot, Slot> producerToConsumerOutputMap = LinkedHashMultimap.create(
                this.consumerToProducerOutputMap.size(), this.consumerToProducerOutputMap.size());
        for (Entry<Slot, Slot> consumerToProducerSlot : this.consumerToProducerOutputMap.entrySet()) {
            if (prunedOutputs.contains(consumerToProducerSlot.getKey())) {
                consumerToProducerOutputMap.put(consumerToProducerSlot.getKey(), consumerToProducerSlot.getValue());
                producerToConsumerOutputMap.put(consumerToProducerSlot.getValue(), consumerToProducerSlot.getKey());
            }
        }
        return withTwoMaps(consumerToProducerOutputMap, producerToConsumerOutputMap);
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return (List) this.getOutput();
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
