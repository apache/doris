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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * LogicalCTEConsumer
 */
public class LogicalCTEConsumer extends LogicalLeaf {

    private final CTEId cteId;

    private final Map<Slot, Slot> consumerToProducerOutputMap = new LinkedHashMap<>();

    private final Map<Slot, Slot> producerToConsumerOutputMap = new LinkedHashMap<>();

    private final int consumerId;

    private final String name;

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, LogicalPlan childPlan, CTEId cteId, String name) {
        super(PlanType.LOGICAL_CTE_RELATION, groupExpression, logicalProperties);
        this.cteId = cteId;
        this.name = name;
        initProducerToConsumerOutputMap(childPlan);
        for (Map.Entry<Slot, Slot> entry : producerToConsumerOutputMap.entrySet()) {
            this.consumerToProducerOutputMap.put(entry.getValue(), entry.getKey());
        }
        this.consumerId = StatementScopeIdGenerator.newCTEId().asInt();
    }

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CTEId cteId,
            Map<Slot, Slot> consumerToProducerOutputMap,
            Map<Slot, Slot> producerToConsumerOutputMap, int consumerId, String name) {
        super(PlanType.LOGICAL_CTE_RELATION, groupExpression, logicalProperties);
        this.cteId = cteId;
        this.consumerToProducerOutputMap.putAll(consumerToProducerOutputMap);
        this.producerToConsumerOutputMap.putAll(producerToConsumerOutputMap);
        this.consumerId = consumerId;
        this.name = name;
    }

    private void initProducerToConsumerOutputMap(LogicalPlan childPlan) {
        List<Slot> producerOutput = childPlan.getOutput();
        for (Slot producerOutputSlot : producerOutput) {
            Slot consumerSlot = new SlotReference(producerOutputSlot.getName(),
                    producerOutputSlot.getDataType(), producerOutputSlot.nullable(), ImmutableList.of(name));
            producerToConsumerOutputMap.put(producerOutputSlot, consumerSlot);
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

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTEConsumer(groupExpression, Optional.of(getLogicalProperties()), cteId,
                consumerToProducerOutputMap,
                producerToConsumerOutputMap,
                consumerId, name);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCTEConsumer(groupExpression, logicalProperties, cteId,
                consumerToProducerOutputMap,
                producerToConsumerOutputMap,
                consumerId, name);
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.copyOf(producerToConsumerOutputMap.values());
    }

    public CTEId getCteId() {
        return cteId;
    }

    @Override
    public int hashCode() {
        return consumerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return this.consumerId == ((LogicalCTEConsumer) o).consumerId;
    }

    public int getConsumerId() {
        return consumerId;
    }

    public String getName() {
        return name;
    }

    public Slot findProducerSlot(Slot consumerSlot) {
        Slot slot = consumerToProducerOutputMap.get(consumerSlot);
        Preconditions.checkArgument(slot != null, String.format("Required producer"
                + "slot for :%s doesn't exist", consumerSlot));
        return slot;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCteConsumer[" + id.asInt() + "]",
                "cteId", cteId,
                "consumerId", consumerId);
    }
}
