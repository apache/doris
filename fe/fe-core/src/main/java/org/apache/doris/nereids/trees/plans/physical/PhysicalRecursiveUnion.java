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
import org.apache.doris.nereids.trees.plans.algebra.RecursiveCte;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
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

/**
 * PhysicalRecursiveUnion is basically like PhysicalUnion, so most methods are same or similar as PhysicalUnion
 */
public class PhysicalRecursiveUnion<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends PhysicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements RecursiveCte {
    private final String cteName;
    private final List<NamedExpression> outputs;
    private final List<List<SlotReference>> regularChildrenOutputs;
    private final SetOperation.Qualifier qualifier;

    /** PhysicalRecursiveCte */
    public PhysicalRecursiveUnion(String cteName, SetOperation.Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(cteName, qualifier, outputs, childrenOutputs, Optional.empty(), logicalProperties, leftChild, rightChild);
    }

    /** PhysicalRecursiveCte */
    public PhysicalRecursiveUnion(String cteName, SetOperation.Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(cteName, qualifier, outputs, childrenOutputs, groupExpression, logicalProperties,
                PhysicalProperties.ANY, null, leftChild, rightChild);
    }

    /** PhysicalRecursiveCte */
    public PhysicalRecursiveUnion(String cteName, SetOperation.Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_RECURSIVE_CTE, groupExpression, logicalProperties, physicalProperties,
                statistics, leftChild, rightChild);
        this.cteName = cteName;
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(childrenOutputs);
    }

    @Override
    public boolean isUnionAll() {
        return qualifier == SetOperation.Qualifier.ALL;
    }

    public String getCteName() {
        return cteName;
    }

    @Override
    public List<SlotReference> getRegularChildOutput(int i) {
        return regularChildrenOutputs.get(i);
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputs;
    }

    @Override
    public List<Slot> computeOutput() {
        return outputs.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<List<SlotReference>> getRegularChildrenOutputs() {
        return regularChildrenOutputs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalRecursiveUnion that = (PhysicalRecursiveUnion) o;
        return cteName.equals(that.cteName) && qualifier == that.qualifier && Objects.equals(outputs, that.outputs)
                && Objects.equals(regularChildrenOutputs, that.regularChildrenOutputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteName, qualifier, outputs, regularChildrenOutputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalRecursiveUnion(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return regularChildrenOutputs.stream().flatMap(List::stream).collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalRecursiveCte" + "[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics,
                "cteName", cteName,
                "Qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs);
    }

    @Override
    public String shapeInfo() {
        ConnectContext context = ConnectContext.get();
        if (context != null
                && context.getSessionVariable().getDetailShapePlanNodesSet().contains(getClass().getSimpleName())) {
            StringBuilder builder = new StringBuilder();
            builder.append(getClass().getSimpleName());
            builder.append(")");
            return builder.toString();
        } else {
            return super.shapeInfo();
        }
    }

    @Override
    public PhysicalRecursiveUnion<Plan, Plan> withChildren(List<Plan> children) {
        return new PhysicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs, groupExpression,
                getLogicalProperties(), children.get(0), children.get(1));
    }

    @Override
    public PhysicalRecursiveUnion<Plan, Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs,
                groupExpression, getLogicalProperties(), left(), right());
    }

    @Override
    public PhysicalRecursiveUnion<Plan, Plan> withGroupExprLogicalPropChildren(
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs,
                groupExpression, logicalProperties.get(), left(), right());
    }

    @Override
    public PhysicalRecursiveUnion<Plan, Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, left(), right());
    }

    @Override
    public PhysicalRecursiveUnion<Plan, Plan> resetLogicalProperties() {
        return new PhysicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs,
                Optional.empty(), null, physicalProperties, statistics, left(), right());
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        if (qualifier == SetOperation.Qualifier.DISTINCT) {
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
