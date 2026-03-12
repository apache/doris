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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.RecursiveCte;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalRecursiveCte is basically like LogicalUnion
 */
public class LogicalRecursiveCte extends AbstractLogicalPlan implements RecursiveCte, OutputPrunable {
    private final String cteName;
    private final List<NamedExpression> outputs;
    private final List<List<SlotReference>> regularChildrenOutputs;
    private final boolean isUnionAll;

    /** LogicalRecursiveCte */
    public LogicalRecursiveCte(String cteName, boolean isUnionAll, List<Plan> children) {
        this(cteName, isUnionAll, ImmutableList.of(), ImmutableList.of(), children);
    }

    /** LogicalRecursiveCte */
    public LogicalRecursiveCte(String cteName, boolean isUnionAll, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs, List<Plan> children) {
        this(cteName, isUnionAll, outputs, childrenOutputs, Optional.empty(),
                Optional.empty(),
                children);
    }

    /** LogicalRecursiveCte */
    public LogicalRecursiveCte(String cteName, boolean isUnionAll, List<NamedExpression> outputs,
            List<List<SlotReference>> childrenOutputs,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        super(PlanType.LOGICAL_RECURSIVE_CTE, groupExpression, logicalProperties, children);
        this.cteName = cteName;
        this.isUnionAll = isUnionAll;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(childrenOutputs);
    }

    @Override
    public boolean isUnionAll() {
        return isUnionAll;
    }

    public String getCteName() {
        return cteName;
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
                childOutputSize
        );
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
                    new SlotReference(exprId, slot.toSql(), slot.getDataType(), slot.nullable(), ImmutableList.of())
            );
        }
        return newOutputs.build();
    }

    // If the right child is nullable, need to ensure that the left child is also nullable
    private List<Slot> resetNullableForLeftOutputs() {
        int rightChildOutputSize = child(1).getOutput().size();
        ImmutableList.Builder<Slot> resetNullableForLeftOutputs
                = ImmutableList.builderWithExpectedSize(rightChildOutputSize);
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
        return Utils.toSqlStringSkipNull("LogicalRecursiveCte",
                "cteName", cteName,
                "isUnionAll", isUnionAll,
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
        LogicalRecursiveCte that = (LogicalRecursiveCte) o;
        return cteName.equals(that.cteName) && isUnionAll == that.isUnionAll && Objects.equals(outputs, that.outputs)
                && Objects.equals(regularChildrenOutputs, that.regularChildrenOutputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteName, isUnionAll, outputs, regularChildrenOutputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRecursiveCte(this, context);
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
    public LogicalRecursiveCte withChildren(List<Plan> children) {
        return new LogicalRecursiveCte(cteName, isUnionAll, outputs, regularChildrenOutputs, children);
    }

    public LogicalRecursiveCte withChildrenAndTheirOutputs(List<Plan> children,
            List<List<SlotReference>> childrenOutputs) {
        Preconditions.checkArgument(children.size() == childrenOutputs.size(),
                "children size %s is not equals with children outputs size %s",
                children.size(), childrenOutputs.size());
        return new LogicalRecursiveCte(cteName, isUnionAll, outputs, childrenOutputs, children);
    }

    @Override
    public LogicalRecursiveCte withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalRecursiveCte(cteName, isUnionAll, outputs, regularChildrenOutputs,
                groupExpression, Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalRecursiveCte(cteName, isUnionAll, outputs, regularChildrenOutputs,
                groupExpression, logicalProperties, children);
    }

    public LogicalRecursiveCte withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalRecursiveCte(cteName, isUnionAll, newOutputs, regularChildrenOutputs,
                Optional.empty(), Optional.empty(), children);
    }

    public LogicalRecursiveCte withNewOutputsAndChildren(List<NamedExpression> newOutputs,
                                                         List<Plan> children,
                                                         List<List<SlotReference>> childrenOutputs) {
        return new LogicalRecursiveCte(cteName, isUnionAll, newOutputs, childrenOutputs,
                Optional.empty(), Optional.empty(), children);
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputs;
    }

    @Override
    public LogicalRecursiveCte pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withNewOutputs(prunedOutputs);
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
    }

    @Override
    public boolean hasUnboundExpression() {
        return outputs.isEmpty();
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        // don't generate
    }
}
