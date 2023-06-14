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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.planner.HashJoinNode;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is used to reduce shuffle cost by finishing the shuffle upon CTEConsumer when sending producer's result set to
 * CTE consumer.
 */
public class CTEPostProcessor extends PlanPostProcessor {

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        return plan.accept(this, ctx);
    }

    @Override
    public Plan visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, CascadesContext context) {
        distribute = distribute.withChildren(ImmutableList.of(distribute.child().accept(this, context)));
        Plan cur = distribute;
        do {
            cur = cur.child(0);
        } while (!(cur instanceof HashJoinNode
                || cur instanceof PhysicalDistribute || cur instanceof LeafPlan) && cur instanceof UnaryPlan);
        if (!(cur instanceof PhysicalCTEConsumer)) {
            return distribute;
        }
        DistributionSpec distributionSpec = distribute.getDistributionSpec();
        PhysicalCTEConsumer cteConsumer = (PhysicalCTEConsumer) cur;

        if (distributionSpec instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) distributionSpec;
            List<ExprId> exprIds = distributionSpecHash.getOrderedShuffledColumns();
            Plan plan = distribute.child();
            List<Slot> slots = plan.getOutput();
            List<ExprId> replicatedSlots = new ArrayList<>();
            for (ExprId exprId : exprIds) {
                for (Slot slot : slots) {
                    if (slot.getExprId().equals(exprId)) {
                        Slot cteSlot = context.getAliasSlotToCTESlot().get(slot);
                        if (cteSlot == null) {
                            if (context.getCteSlotToCTEId().containsKey(slot)) {
                                cteSlot = slot;
                            } else {
                                return distribute;
                            }
                        }
                        Slot producerSlot = context.getCteIdToConsumerToProducerOutputMap().get(cteConsumer.getCteId())
                                .get(cteSlot);
                        replicatedSlots.add(producerSlot.getExprId());
                    }
                }
            }
            context.addDistributionForCTE(cteConsumer.getConsumerId(), new DistributionSpecHash(replicatedSlots,
                    distributionSpecHash.getShuffleType(), distributionSpecHash.getTableId(),
                    distributionSpecHash.getSelectedIndexId(), distributionSpecHash.getPartitionIds(),
                    distributionSpecHash.getEquivalenceExprIds(), distributionSpecHash.getExprIdToEquivalenceSet()));
        } else {
            context.addDistributionForCTE(cteConsumer.getConsumerId(), distributionSpec);
        }
        return distribute.child();
    }

    @Override
    public Plan visitPhysicalCTEConsumer(PhysicalCTEConsumer cteConsumer, CascadesContext context) {
        CTEId cteId = cteConsumer.getCteId();
        Map<Slot, Slot> consumerToProducerOutputMap =
                context.getCteIdToConsumerToProducerOutputMap().computeIfAbsent(cteId, k -> new HashMap<>());
        consumerToProducerOutputMap.putAll(cteConsumer.getConsumerToProducerOutputMap());
        for (Slot slot : cteConsumer.getOutput()) {
            context.getCteSlotToCTEId().put(slot, cteId);
        }
        return cteConsumer;
    }

    @Override
    public Plan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project = project.withChildren(ImmutableList.of(project.child().accept(this, context)));
        for (NamedExpression p : project.getProjects()) {
            if (p instanceof Alias) {
                Alias alias = (Alias) p;
                if (alias.child() instanceof SlotReference) {
                    SlotReference slotReference = (SlotReference) alias.child();
                    // Value might be null.
                    context.getAliasSlotToCTESlot().put(alias.toSlot(),
                            context.getAliasSlotToCTESlot().remove(slotReference));
                }
            }
        }
        return project;
    }
}
