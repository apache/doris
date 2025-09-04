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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Physical Union.
 */
public class PhysicalUnion extends PhysicalSetOperation implements Union {

    // in doris, we use union node to present one row relation
    private final List<List<NamedExpression>> constantExprsList;

    /** PhysicalUnion */
    public PhysicalUnion(Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(PlanType.PHYSICAL_UNION, qualifier, outputs, childrenOutputs, logicalProperties, children);
        this.constantExprsList = ImmutableList.copyOf(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    /** PhysicalUnion */
    public PhysicalUnion(Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(PlanType.PHYSICAL_UNION, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, children);
        this.constantExprsList = ImmutableList.copyOf(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    /** PhysicalUnion */
    public PhysicalUnion(Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs, List<List<NamedExpression>> constantExprsList,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, List<Plan> inputs) {
        super(PlanType.PHYSICAL_UNION, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, physicalProperties, statistics, inputs);
        this.constantExprsList = ImmutableList.copyOf(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public List<List<NamedExpression>> getConstantExprsList() {
        return constantExprsList;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalUnion(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalUnion" + "[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics,
                "qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs,
                "constantExprsList", constantExprsList);
    }

    @Override
    public String shapeInfo() {
        ConnectContext context = ConnectContext.get();
        if (context != null
                && context.getSessionVariable().getDetailShapePlanNodesSet().contains(getClass().getSimpleName())) {
            StringBuilder builder = new StringBuilder();
            builder.append(getClass().getSimpleName());
            builder.append("(constantExprsList=");
            builder.append(constantExprsList.stream()
                    .map(exprs -> exprs.stream().map(Expression::shapeInfo)
                            .collect(Collectors.joining(", ", "[", "]")))
                    .collect(Collectors.joining(", ", "[", "]")));
            builder.append(")");
            return builder.toString();
        } else {
            return super.shapeInfo();
        }
    }

    @Override
    public PhysicalUnion withChildren(List<Plan> children) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList, groupExpression,
                getLogicalProperties(), children);
    }

    @Override
    public PhysicalUnion withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                groupExpression, getLogicalProperties(), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                groupExpression, logicalProperties.get(), children);
    }

    @Override
    public PhysicalUnion withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, children);
    }

    @Override
    public PhysicalUnion resetLogicalProperties() {
        return new PhysicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList,
                Optional.empty(), null, physicalProperties, statistics, children);
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        if (qualifier == Qualifier.DISTINCT) {
            builder.addUniqueSlot(ImmutableSet.copyOf(getOutput()));
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        // don't propagate uniform slots
    }

    private List<BitSet> mapSlotToIndex(Plan plan, List<Set<Slot>> equalSlotsList) {
        Map<Slot, Integer> slotToIndex = new HashMap<>();
        for (int i = 0; i < plan.getOutput().size(); i++) {
            slotToIndex.put(plan.getOutput().get(i), i);
        }
        List<BitSet> equalSlotIndicesList = new ArrayList<>();
        for (Set<Slot> equalSlots : equalSlotsList) {
            BitSet equalSlotIndices = new BitSet();
            for (Slot slot : equalSlots) {
                if (slotToIndex.containsKey(slot)) {
                    equalSlotIndices.set(slotToIndex.get(slot));
                }
            }
            if (equalSlotIndices.cardinality() > 1) {
                equalSlotIndicesList.add(equalSlotIndices);
            }
        }
        return equalSlotIndicesList;
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        if (children.isEmpty()) {
            return;
        }

        // Get the list of equal slot sets and their corresponding index mappings for the first child
        List<Set<Slot>> childEqualSlotsList = child(0).getLogicalProperties()
                .getTrait().calAllEqualSet();
        List<BitSet> childEqualSlotsIndicesList = mapSlotToIndex(child(0), childEqualSlotsList);
        List<BitSet> unionEqualSlotIndicesList = new ArrayList<>(childEqualSlotsIndicesList);

        // Traverse all children and find the equal sets that exist in all children
        for (int i = 1; i < children.size(); i++) {
            Plan child = children.get(i);

            // Get the equal slot sets for the current child
            childEqualSlotsList = child.getLogicalProperties().getTrait().calAllEqualSet();

            // Map slots to indices for the current child
            childEqualSlotsIndicesList = mapSlotToIndex(child, childEqualSlotsList);

            // Only keep the equal pairs that exist in all children of the union
            // This is done by calculating the intersection of all children's equal slot indices
            for (BitSet unionEqualSlotIndices : unionEqualSlotIndicesList) {
                BitSet intersect = new BitSet();
                for (BitSet childEqualSlotIndices : childEqualSlotsIndicesList) {
                    if (unionEqualSlotIndices.intersects(childEqualSlotIndices)) {
                        intersect = childEqualSlotIndices;
                        break;
                    }
                }
                unionEqualSlotIndices.and(intersect);
            }
        }

        // Build the functional dependencies for the output slots
        List<Slot> outputList = getOutput();
        for (BitSet equalSlotIndices : unionEqualSlotIndicesList) {
            if (equalSlotIndices.cardinality() <= 1) {
                continue;
            }
            int first = equalSlotIndices.nextSetBit(0);
            int next = equalSlotIndices.nextSetBit(first + 1);
            while (next > 0) {
                builder.addEqualPair(outputList.get(first), outputList.get(next));
                next = equalSlotIndices.nextSetBit(next + 1);
            }
        }
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        // don't generate
    }
}
