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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical Union.
 */
public class LogicalUnion extends LogicalSetOperation implements Union, OutputPrunable {

    // in doris, we use union node to present one row relation
    private final List<List<NamedExpression>> constantExprsList;
    // When there is an agg on the union and there is a filter on the agg,
    // it is necessary to keep the filter on the agg and push the filter down to each child of the union.
    private final boolean hasPushedFilter;

    /** LogicalUnion */
    public LogicalUnion(Qualifier qualifier, List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, children);
        this.hasPushedFilter = false;
        this.constantExprsList = ImmutableList.of();
    }

    /** LogicalUnion */
    public LogicalUnion(Qualifier qualifier, List<List<NamedExpression>> constantExprsList, List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, children);
        this.hasPushedFilter = false;
        this.constantExprsList = constantExprsList;
    }

    /** LogicalUnion */
    public LogicalUnion(Qualifier qualifier, List<NamedExpression> outputs, List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList, boolean hasPushedFilter, List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, outputs, childrenOutputs, children);
        this.hasPushedFilter = hasPushedFilter;
        this.constantExprsList = Utils.fastToImmutableList(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    /** LogicalUnion */
    public LogicalUnion(Qualifier qualifier, List<NamedExpression> outputs, List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList, boolean hasPushedFilter,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, children);
        this.hasPushedFilter = hasPushedFilter;
        this.constantExprsList = Utils.fastToImmutableList(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public boolean hasPushedFilter() {
        return hasPushedFilter;
    }

    public List<List<NamedExpression>> getConstantExprsList() {
        return constantExprsList;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return constantExprsList.stream().flatMap(List::stream).collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalUnion",
                "qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs,
                "constantExprsList", constantExprsList,
                "hasPushedFilter", hasPushedFilter,
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
        LogicalUnion that = (LogicalUnion) o;
        return super.equals(that) && hasPushedFilter == that.hasPushedFilter
                && Objects.equals(constantExprsList, that.constantExprsList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hasPushedFilter, constantExprsList);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalUnion(this, context);
    }

    @Override
    public LogicalUnion withChildren(List<Plan> children) {
        return new LogicalUnion(qualifier, outputs, regularChildrenOutputs,
                constantExprsList, hasPushedFilter, children);
    }

    @Override
    public LogicalSetOperation withChildrenAndTheirOutputs(List<Plan> children,
            List<List<SlotReference>> childrenOutputs) {
        Preconditions.checkArgument(children.size() == childrenOutputs.size(),
                "children size %s is not equals with children outputs size %s",
                children.size(), childrenOutputs.size());
        return new LogicalUnion(qualifier, outputs, childrenOutputs, constantExprsList, hasPushedFilter, children);
    }

    @Override
    public LogicalUnion withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList, hasPushedFilter,
                groupExpression, Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList, hasPushedFilter,
                groupExpression, logicalProperties, children);
    }

    @Override
    public LogicalUnion withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalUnion(qualifier, newOutputs, regularChildrenOutputs, constantExprsList,
                hasPushedFilter, Optional.empty(), Optional.empty(), children);
    }

    public LogicalUnion withNewOutputsAndConstExprsList(List<NamedExpression> newOutputs,
            List<List<NamedExpression>> constantExprsList) {
        return new LogicalUnion(qualifier, newOutputs, regularChildrenOutputs, constantExprsList,
                hasPushedFilter, Optional.empty(), Optional.empty(), children);
    }

    public LogicalUnion withChildrenAndConstExprsList(List<Plan> children,
            List<List<SlotReference>> childrenOutputs, List<List<NamedExpression>> constantExprsList) {
        return new LogicalUnion(qualifier, outputs, childrenOutputs, constantExprsList, hasPushedFilter, children);
    }

    public LogicalUnion withNewOutputsChildrenAndConstExprsList(List<NamedExpression> newOutputs, List<Plan> children,
                                                                List<List<SlotReference>> childrenOutputs,
                                                                List<List<NamedExpression>> constantExprsList) {
        return new LogicalUnion(qualifier, newOutputs, childrenOutputs, constantExprsList,
                hasPushedFilter, Optional.empty(), Optional.empty(), children);
    }

    public LogicalUnion withAllQualifier() {
        return new LogicalUnion(Qualifier.ALL, outputs, regularChildrenOutputs, constantExprsList, hasPushedFilter,
                Optional.empty(), Optional.empty(), children);
    }

    @Override
    public LogicalUnion pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withNewOutputs(prunedOutputs);
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        if (qualifier == Qualifier.DISTINCT) {
            builder.addUniqueSlot(ImmutableSet.copyOf(getOutput()));
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        final Optional<ExpressionRewriteContext> context = ConnectContext.get() == null ? Optional.empty()
                : Optional.of(new ExpressionRewriteContext(CascadesContext.initContext(
                        ConnectContext.get().getStatementContext(), this, PhysicalProperties.ANY)));
        for (int i = 0; i < getOutputs().size(); i++) {
            Optional<Literal> value = Optional.empty();
            if (!constantExprsList.isEmpty()) {
                value = ExpressionUtils.checkConstantExpr(constantExprsList.get(0).get(i), context);
                if (!value.isPresent()) {
                    continue;
                }
                final int fi = i;
                Literal literal = value.get();
                if (constantExprsList.stream()
                        .map(exprs -> ExpressionUtils.checkConstantExpr(exprs.get(fi), context))
                        .anyMatch(val -> !val.isPresent() || !val.get().equals(literal))) {
                    continue;
                }
            }
            for (int childIdx = 0; childIdx < children.size(); childIdx++) {
                // TODO: use originOutputs = child(childIdx).getOutput() ?
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
        if (!constantExprsList.isEmpty() && children.isEmpty()) {
            return false;
        }
        return super.hasUnboundExpression();
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

    /** castCommonDataTypeAndNullableByConstants */
    public static Pair<List<List<NamedExpression>>, List<Boolean>> castCommonDataTypeAndNullableByConstants(
            List<List<NamedExpression>> constantExprsList) {
        int columnCount = constantExprsList.isEmpty() ? 0 : constantExprsList.get(0).size();
        Pair<List<DataType>, List<Boolean>> commonInfo
                = computeCommonDataTypeAndNullable(constantExprsList, columnCount);
        List<List<NamedExpression>> castedRows = castToCommonType(constantExprsList, commonInfo.key(), columnCount);
        List<Boolean> nullables = commonInfo.second;
        return Pair.of(castedRows, nullables);
    }

    private static Pair<List<DataType>, List<Boolean>> computeCommonDataTypeAndNullable(
            List<List<NamedExpression>> constantExprsList, int columnCount) {
        List<Boolean> nullables = Lists.newArrayListWithCapacity(columnCount);
        List<DataType> commonDataTypes = Lists.newArrayListWithCapacity(columnCount);
        List<NamedExpression> firstRow = constantExprsList.get(0);
        for (int columnId = 0; columnId < columnCount; columnId++) {
            Expression constant = firstRow.get(columnId).child(0);
            Pair<DataType, Boolean> commonDataTypeAndNullable
                    = computeCommonDataTypeAndNullable(constant, columnId, constantExprsList);
            commonDataTypes.add(commonDataTypeAndNullable.first);
            nullables.add(commonDataTypeAndNullable.second);
        }
        return Pair.of(commonDataTypes, nullables);
    }

    private static Pair<DataType, Boolean> computeCommonDataTypeAndNullable(
            Expression firstRowExpr, int columnId, List<List<NamedExpression>> constantExprsList) {
        DataType commonDataType = firstRowExpr.getDataType();
        boolean nullable = firstRowExpr.nullable();
        for (int rowId = 1; rowId < constantExprsList.size(); rowId++) {
            NamedExpression namedExpression = constantExprsList.get(rowId).get(columnId);
            Expression otherConstant = namedExpression.child(0);
            nullable |= otherConstant.nullable();
            DataType otherDataType = otherConstant.getDataType();
            commonDataType = getAssignmentCompatibleType(commonDataType, otherDataType);
        }
        return Pair.of(commonDataType, nullable);
    }

    private static List<List<NamedExpression>> castToCommonType(
            List<List<NamedExpression>> rows, List<DataType> commonDataTypes, int columnCount) {
        ImmutableList.Builder<List<NamedExpression>> castedConstants
                = ImmutableList.builderWithExpectedSize(rows.size());
        for (List<NamedExpression> row : rows) {
            castedConstants.add(castToCommonType(row, commonDataTypes));
        }
        return castedConstants.build();
    }

    private static List<NamedExpression> castToCommonType(List<NamedExpression> row, List<DataType> commonTypes) {
        ImmutableList.Builder<NamedExpression> castedRow = ImmutableList.builderWithExpectedSize(row.size());
        boolean changed = false;
        for (int columnId = 0; columnId < row.size(); columnId++) {
            NamedExpression constantAlias = row.get(columnId);
            Expression constant = constantAlias.child(0);
            DataType commonType = commonTypes.get(columnId);
            if (commonType.equals(constant.getDataType())) {
                castedRow.add(constantAlias);
            } else {
                changed = true;
                Expression expression
                        = TypeCoercionUtils.castIfNotSameTypeStrict(constant, commonType);
                castedRow.add((NamedExpression) constantAlias.withChildren(expression));
            }
        }
        return changed ? castedRow.build() : row;
    }
}
