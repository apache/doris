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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.RecursiveCte;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
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
 * LogicalRecursiveUnion is basically like LogicalUnion, so most methods are same or similar as LogicalUnion
 */
public class LogicalRecursiveUnion<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements RecursiveCte {
    private final String cteName;
    private final List<NamedExpression> outputs;
    private final List<List<SlotReference>> regularChildrenOutputs;
    private final SetOperation.Qualifier qualifier;

    /** LogicalRecursiveUnion */
    public LogicalRecursiveUnion(String cteName, SetOperation.Qualifier qualifier, List<Plan> children) {
        this(cteName, qualifier, ImmutableList.of(), ImmutableList.of(), children);
    }

    /** LogicalRecursiveUnion */
    public LogicalRecursiveUnion(String cteName, SetOperation.Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs, List<Plan> children) {
        this(cteName, qualifier, outputs, childrenOutputs, Optional.empty(),
                Optional.empty(),
                children);
    }

    /** LogicalRecursiveUnion */
    public LogicalRecursiveUnion(String cteName, SetOperation.Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        super(PlanType.LOGICAL_RECURSIVE_CTE, groupExpression, logicalProperties, children);
        this.cteName = cteName;
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(childrenOutputs);
    }

    @Override
    public LEFT_CHILD_TYPE left() {
        return (LEFT_CHILD_TYPE) child(0);
    }

    @Override
    public RIGHT_CHILD_TYPE right() {
        return (RIGHT_CHILD_TYPE) child(1);
    }

    @Override
    public boolean isUnionAll() {
        return qualifier == SetOperation.Qualifier.ALL;
    }

    public String getCteName() {
        return cteName;
    }

    public SetOperation.Qualifier getQualifier() {
        return qualifier;
    }

    @Override
    public List<SlotReference> getRegularChildOutput(int i) {
        return regularChildrenOutputs.get(i);
    }

    @Override
    public List<List<SlotReference>> getRegularChildrenOutputs() {
        return regularChildrenOutputs;
    }

    public List<List<NamedExpression>> collectChildrenProjections() {
        return castCommonDataTypeOutputs();
    }

    private List<List<NamedExpression>> castCommonDataTypeOutputs() {
        int childOutputSize = child(0).getOutput().size();
        ImmutableList.Builder<NamedExpression> newLeftOutputs = ImmutableList.builderWithExpectedSize(
                childOutputSize);
        ImmutableList.Builder<NamedExpression> newRightOutputs = ImmutableList.builderWithExpectedSize(
                childOutputSize);
        // Ensure that the output types of the left and right children are consistent and expand upward.
        for (int i = 0; i < childOutputSize; ++i) {
            Slot left = child(0).getOutput().get(i);
            Slot right = child(1).getOutput().get(i);
            DataType compatibleType;
            try {
                compatibleType = LogicalSetOperation.getAssignmentCompatibleType(left.getDataType(),
                        right.getDataType());
            } catch (Exception e) {
                throw new AnalysisException(
                        "Can not find compatible type for " + left + " and " + right + ", " + e.getMessage());
            }
            Expression newLeft = TypeCoercionUtils.castIfNotSameTypeStrict(left, compatibleType);
            Expression newRight = TypeCoercionUtils.castIfNotSameTypeStrict(right, compatibleType);
            if (newLeft instanceof Cast) {
                newLeft = new Alias(newLeft, left.getName());
            }
            if (newRight instanceof Cast) {
                newRight = new Alias(newRight, right.getName());
            }
            newLeftOutputs.add((NamedExpression) newLeft);
            newRightOutputs.add((NamedExpression) newRight);
        }

        return ImmutableList.of(newLeftOutputs.build(), newRightOutputs.build());
    }

    /**
     * Generate new output for Recursive Cte.
     */
    public List<NamedExpression> buildNewOutputs() {
        List<Slot> slots = resetNullableForLeftOutputs();
        ImmutableList.Builder<NamedExpression> newOutputs = ImmutableList.builderWithExpectedSize(slots.size());

        for (int i = 0; i < slots.size(); i++) {
            Slot slot = slots.get(i);
            ExprId exprId = i < outputs.size() ? outputs.get(i).getExprId() : StatementScopeIdGenerator.newExprId();
            newOutputs.add(
                    new SlotReference(exprId, slot.toSql(), slot.getDataType(), slot.nullable(), ImmutableList.of()));
        }
        return newOutputs.build();
    }

    // If the right child is nullable, need to ensure that the left child is also nullable
    private List<Slot> resetNullableForLeftOutputs() {
        int rightChildOutputSize = child(1).getOutput().size();
        ImmutableList.Builder<Slot> resetNullableForLeftOutputs = ImmutableList
                .builderWithExpectedSize(rightChildOutputSize);
        for (int i = 0; i < rightChildOutputSize; ++i) {
            if (child(1).getOutput().get(i).nullable() && !child(0).getOutput().get(i).nullable()) {
                resetNullableForLeftOutputs.add(child(0).getOutput().get(i).withNullable(true));
            } else {
                resetNullableForLeftOutputs.add(child(0).getOutput().get(i));
            }
        }
        return resetNullableForLeftOutputs.build();
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalRecursiveUnion",
                "cteName", cteName,
                "Qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs,
                "stats", statistics);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalRecursiveUnion that = (LogicalRecursiveUnion) o;
        return cteName.equals(that.cteName) && qualifier == that.qualifier && Objects.equals(outputs, that.outputs)
                && Objects.equals(regularChildrenOutputs, that.regularChildrenOutputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteName, qualifier, outputs, regularChildrenOutputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRecursiveUnion(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return regularChildrenOutputs.stream().flatMap(List::stream).collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<Slot> computeOutput() {
        return outputs.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public LogicalRecursiveUnion<Plan, Plan> withChildren(List<Plan> children) {
        return new LogicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs, children);
    }

    public LogicalRecursiveUnion<Plan, Plan> withChildrenAndTheirOutputs(List<Plan> children,
            List<List<SlotReference>> childrenOutputs) {
        Preconditions.checkArgument(children.size() == childrenOutputs.size(),
                "children size %s is not equals with children outputs size %s",
                children.size(), childrenOutputs.size());
        return new LogicalRecursiveUnion<>(cteName, qualifier, outputs, childrenOutputs, children);
    }

    @Override
    public LogicalRecursiveUnion<Plan, Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs,
                groupExpression, Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalRecursiveUnion<>(cteName, qualifier, outputs, regularChildrenOutputs,
                groupExpression, logicalProperties, children);
    }

    public LogicalRecursiveUnion<Plan, Plan> withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalRecursiveUnion<>(cteName, qualifier, newOutputs, regularChildrenOutputs,
                Optional.empty(), Optional.empty(), children);
    }

    public LogicalRecursiveUnion<Plan, Plan> withNewOutputsAndChildren(List<NamedExpression> newOutputs,
            List<Plan> children,
            List<List<SlotReference>> childrenOutputs) {
        return new LogicalRecursiveUnion<>(cteName, qualifier, newOutputs, childrenOutputs,
                Optional.empty(), Optional.empty(), children);
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputs;
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        if (qualifier == SetOperation.Qualifier.DISTINCT) {
            builder.addUniqueSlot(ImmutableSet.copyOf(getOutput()));
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        final Optional<ExpressionRewriteContext> context = ConnectContext.get() == null ? Optional.empty()
                : Optional.of(new ExpressionRewriteContext(this, CascadesContext.initContext(
                        ConnectContext.get().getStatementContext(), this, PhysicalProperties.ANY)));
        for (int i = 0; i < getOutputs().size(); i++) {
            Optional<Literal> value = Optional.empty();
            for (int childIdx = 0; childIdx < children.size(); childIdx++) {
                List<? extends Slot> originOutputs = regularChildrenOutputs.get(childIdx);
                Slot slot = originOutputs.get(i);
                Optional<Expression> childValue = child(childIdx).getLogicalProperties()
                        .getTrait().getUniformValue(slot);
                if (childValue == null || !childValue.isPresent() || !childValue.get().isConstant()) {
                    value = Optional.empty();
                    break;
                }
                Optional<Literal> constExprOpt = ExpressionUtils.checkConstantExpr(childValue.get(), context);
                if (!constExprOpt.isPresent()) {
                    value = Optional.empty();
                    break;
                }
                if (!value.isPresent()) {
                    value = constExprOpt;
                } else if (!value.equals(constExprOpt)) {
                    value = Optional.empty();
                    break;
                }
            }
            if (value.isPresent()) {
                builder.addUniformSlotAndLiteral(getOutputs().get(i).toSlot(), value.get());
            }
        }
    }

    @Override
    public boolean hasUnboundExpression() {
        return outputs.isEmpty();
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
}
