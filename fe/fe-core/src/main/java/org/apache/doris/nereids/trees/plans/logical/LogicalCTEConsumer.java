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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * LogicalCTEConsumer
 */
public class LogicalCTEConsumer extends LogicalLeaf {

    private final LogicalPlan childPlan;

    private final int cteId;
    private final List<Expression> predicates;

    private final List<Expression> projections;

    private final Map<Slot, Slot> consumerToProducerOutputMap = new HashMap<>();

    private final Map<Slot, Slot> producerToConsumerOutputMap = new HashMap<>();

    private final int consumerId;

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, LogicalPlan childPlan, int cteId) {
        super(PlanType.LOGICAL_CTE_RELATION, groupExpression, logicalProperties);
        this.childPlan = childPlan;
        this.cteId = cteId;
        initProducerToConsumerOutputMap(childPlan);
        initConsumerToProducerOutputMap(childPlan);
        this.consumerId = System.identityHashCode(this);
        this.predicates = Collections.emptyList();
        this.projections = Collections.emptyList();

    }

    /**
     * Logical CTE consumer.
     */
    public LogicalCTEConsumer(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, LogicalPlan childPlan, int cteId,
            Map<Slot, Slot> producerToConsumerOutputMap, int consumerId,
                              List<Expression> predicates, List<Expression> projections) {
        super(PlanType.LOGICAL_CTE_RELATION, groupExpression, logicalProperties);
        this.childPlan = childPlan;
        this.cteId = cteId;
        this.producerToConsumerOutputMap.putAll(producerToConsumerOutputMap);
        this.consumerId = consumerId;
        this.predicates = predicates;
        this.projections = projections;
    }

    private void initProducerToConsumerOutputMap(LogicalPlan childPlan) {
        List<Slot> producerOutput = childPlan.getOutput();
        for (Slot producerOutputSlot : producerOutput) {
            Slot consumerSlot = new SlotReference(producerOutputSlot.getName(),
                    producerOutputSlot.getDataType(), producerOutputSlot.nullable(), producerOutputSlot.getQualifier());
            producerToConsumerOutputMap.put(producerOutputSlot, consumerSlot);
        }
    }

    private void initConsumerToProducerOutputMap(LogicalPlan childPlan) {
        List<Slot> producerOutput = childPlan.getOutput();
        for (Slot producerOutputSlot : producerOutput) {
            Slot consumerSlot = new SlotReference(producerOutputSlot.getName(),
                    producerOutputSlot.getDataType(), producerOutputSlot.nullable(), producerOutputSlot.getQualifier());
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
        return visitor.visit(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    public List<Expression> getPredicates() {
        return predicates;
    }

    public List<Expression> getProjects() {
        return projections;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTEConsumer(groupExpression, Optional.of(getLogicalProperties()), childPlan, cteId,
                producerToConsumerOutputMap,
                consumerId, predicates, projections);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCTEConsumer(groupExpression, logicalProperties, childPlan, cteId, producerToConsumerOutputMap,
                consumerId, predicates, projections);
    }

    @Override
    public List<Slot> computeOutput() {
        return new ArrayList<>(producerToConsumerOutputMap.values());
    }

    public int getCteId() {
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

    public Slot findProducerSlot(Slot consumerSlot) {
        Slot slot = consumerToProducerOutputMap.get(consumerSlot);
        Preconditions.checkArgument(slot != null, String.format("Required producer"
                + "slot for :%s doesn't exist", consumerSlot));
        return slot;
    }

    @Override
    public String toString() {
        return String.format("LOGICAL_CTE_CONSUMER#%d", cteId);
    }
}
