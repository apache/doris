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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical SetOperation.
 * The type can have any number of children.
 * After parse, there will only be two children.
 * But after rewriting rules such as merging of the same nodes and elimination of oneRowRelation,
 * there will be multiple or no children.
 * <p>
 * eg: select k1, k2 from t1 union select 1, 2 union select d1, d2 from t2;
 */
public abstract class LogicalSetOperation extends AbstractLogicalPlan implements SetOperation, OutputSavePoint {

    // eg value: qualifier:DISTINCT
    protected final Qualifier qualifier;
    // The newly created output column, used to display the output.
    // eg value: outputs:[k1, k2]
    protected final List<NamedExpression> outputs;
    protected final List<List<SlotReference>> regularChildrenOutputs;

    public LogicalSetOperation(PlanType planType, Qualifier qualifier, List<Plan> children) {
        super(planType, children);
        this.qualifier = qualifier;
        this.outputs = ImmutableList.of();
        this.regularChildrenOutputs = ImmutableList.of();
    }

    public LogicalSetOperation(PlanType planType, Qualifier qualifier,
            List<NamedExpression> outputs, List<List<SlotReference>> regularChildrenOutputs, List<Plan> children) {
        super(planType, children);
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(regularChildrenOutputs);
    }

    public LogicalSetOperation(PlanType planType, Qualifier qualifier, List<NamedExpression> outputs,
            List<List<SlotReference>> regularChildrenOutputs,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        super(planType, groupExpression, logicalProperties, children.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(regularChildrenOutputs);
    }

    public List<List<SlotReference>> getRegularChildrenOutputs() {
        return regularChildrenOutputs;
    }

    @Override
    public boolean hasUnboundExpression() {
        return outputs.isEmpty();
    }

    @Override
    public List<Slot> computeOutput() {
        return outputs.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    public List<List<NamedExpression>> collectChildrenProjections() {
        return castCommonDataTypeOutputs();
    }

    /**
     * Generate new output for SetOperation.
     */
    public List<NamedExpression> buildNewOutputs() {
        ImmutableList.Builder<NamedExpression> newOutputs = new Builder<>();
        for (Slot slot : resetNullableForLeftOutputs()) {
            newOutputs.add(new SlotReference(slot.toSql(), slot.getDataType(), slot.nullable()));
        }
        return newOutputs.build();
    }

    // If the right child is nullable, need to ensure that the left child is also nullable
    private List<Slot> resetNullableForLeftOutputs() {
        Preconditions.checkState(children.size() == 2);
        List<Slot> resetNullableForLeftOutputs = new ArrayList<>();
        for (int i = 0; i < child(1).getOutput().size(); ++i) {
            if (child(1).getOutput().get(i).nullable() && !child(0).getOutput().get(i).nullable()) {
                resetNullableForLeftOutputs.add(child(0).getOutput().get(i).withNullable(true));
            } else {
                resetNullableForLeftOutputs.add(child(0).getOutput().get(i));
            }
        }
        return ImmutableList.copyOf(resetNullableForLeftOutputs);
    }

    private List<List<NamedExpression>> castCommonDataTypeOutputs() {
        List<NamedExpression> newLeftOutputs = new ArrayList<>();
        List<NamedExpression> newRightOutputs = new ArrayList<>();
        // Ensure that the output types of the left and right children are consistent and expand upward.
        for (int i = 0; i < child(0).getOutput().size(); ++i) {
            Slot left = child(0).getOutput().get(i);
            Slot right = child(1).getOutput().get(i);
            DataType compatibleType = getAssignmentCompatibleType(left.getDataType(), right.getDataType());
            Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, compatibleType);
            Expression newRight = TypeCoercionUtils.castIfNotSameType(right, compatibleType);
            if (newLeft instanceof Cast) {
                newLeft = new Alias(newLeft, left.getName());
            }
            if (newRight instanceof Cast) {
                newRight = new Alias(newRight, right.getName());
            }
            newLeftOutputs.add((NamedExpression) newLeft);
            newRightOutputs.add((NamedExpression) newRight);
        }

        List<List<NamedExpression>> resultExpressions = new ArrayList<>();
        resultExpressions.add(newLeftOutputs);
        resultExpressions.add(newRightOutputs);
        return ImmutableList.copyOf(resultExpressions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSetOperation that = (LogicalSetOperation) o;
        return qualifier == that.qualifier && Objects.equals(outputs, that.outputs)
                && Objects.equals(regularChildrenOutputs, that.regularChildrenOutputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier, outputs, regularChildrenOutputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSetOperation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return regularChildrenOutputs.stream().flatMap(List::stream).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Qualifier getQualifier() {
        return qualifier;
    }

    @Override
    public List<SlotReference> getRegularChildOutput(int i) {
        return regularChildrenOutputs.get(i);
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputs;
    }

    public abstract LogicalSetOperation withChildrenAndTheirOutputs(
            List<Plan> children, List<List<SlotReference>> childrenOutputs);

    public abstract LogicalSetOperation withNewOutputs(List<NamedExpression> newOutputs);

    @Override
    public int getArity() {
        return children.size();
    }

    private DataType getAssignmentCompatibleType(DataType left, DataType right) {
        if (left.isNullType()) {
            return right;
        }
        if (right.isNullType()) {
            return left;
        }
        if (left.equals(right)) {
            return left;
        }
        if (left instanceof ArrayType && right instanceof ArrayType) {
            return ArrayType.of(getAssignmentCompatibleType(
                    ((ArrayType) left).getItemType(), ((ArrayType) right).getItemType()));
        }
        if (left instanceof MapType && right instanceof MapType) {
            return MapType.of(
                    getAssignmentCompatibleType(((MapType) left).getKeyType(), ((MapType) right).getKeyType()),
                    getAssignmentCompatibleType(((MapType) left).getValueType(), ((MapType) right).getValueType()));
        }
        if (left instanceof StructType && right instanceof StructType) {
            List<StructField> leftFields = ((StructType) left).getFields();
            List<StructField> rightFields = ((StructType) right).getFields();
            if (leftFields.size() != rightFields.size()) {
                throw new AnalysisException(
                        "could not get common type for two different struct type " + left + ", " + right);
            }
            ImmutableList.Builder<StructField> commonFields = ImmutableList.builder();
            for (int i = 0; i < leftFields.size(); i++) {
                boolean nullable = leftFields.get(i).isNullable() || rightFields.get(i).isNullable();
                DataType commonType = getAssignmentCompatibleType(
                        leftFields.get(i).getDataType(), rightFields.get(i).getDataType());
                StructField commonField = leftFields.get(i).withDataTypeAndNullable(commonType, nullable);
                commonFields.add(commonField);
            }
            return new StructType(commonFields.build());
        }
        return DataType.fromCatalogType(Type.getAssignmentCompatibleType(
                left.toCatalogDataType(),
                right.toCatalogDataType(),
                false,
                SessionVariable.getEnableDecimal256()));
    }
}
