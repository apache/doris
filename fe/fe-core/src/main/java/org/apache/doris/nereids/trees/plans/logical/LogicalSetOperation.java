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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;

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
 *
 * eg: select k1, k2 from t1 union select 1, 2 union select d1, d2 from t2;
 */
public abstract class LogicalSetOperation extends AbstractLogicalPlan implements SetOperation {

    // eg value: qualifier:DISTINCT
    protected final Qualifier qualifier;

    // The newly created output column, used to display the output.
    // eg value: outputs:[k1, k2]
    protected final List<NamedExpression> outputs;

    public LogicalSetOperation(PlanType planType, Qualifier qualifier, List<Plan> inputs) {
        super(planType, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.of();
    }

    public LogicalSetOperation(PlanType planType, Qualifier qualifier,
                               List<NamedExpression> outputs,
                               List<Plan> inputs) {
        super(planType, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public LogicalSetOperation(PlanType planType, Qualifier qualifier, List<NamedExpression> outputs,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Plan> inputs) {
        super(planType, groupExpression, logicalProperties, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    @Override
    public List<Slot> computeOutput() {
        return outputs.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    public List<List<Expression>> collectCastExpressions() {
        return castCommonDataTypeOutputs(resetNullableForLeftOutputs());
    }

    /**
     * Generate new output for SetOperation.
     */
    public List<NamedExpression> buildNewOutputs(List<Expression> leftCastExpressions) {
        ImmutableList.Builder<NamedExpression> newOutputs = new Builder<>();
        for (Expression expression : leftCastExpressions) {
            if (expression instanceof Cast) {
                newOutputs.add(new SlotReference(
                        ((Cast) expression).child().toSql(), expression.getDataType(),
                        ((Cast) expression).child().nullable()));
            } else if (expression instanceof Slot) {
                newOutputs.add(new SlotReference(
                        expression.toSql(), expression.getDataType(), expression.nullable()));
            }
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

    private List<List<Expression>> castCommonDataTypeOutputs(List<Slot> resetNullableForLeftOutputs) {
        List<Expression> newLeftOutputs = new ArrayList<>();
        List<Expression> newRightOutpus = new ArrayList<>();
        // Ensure that the output types of the left and right children are consistent and expand upward.
        for (int i = 0; i < resetNullableForLeftOutputs.size(); ++i) {
            boolean hasPushed = false;
            Slot left = resetNullableForLeftOutputs.get(i);
            Slot right = child(1).getOutput().get(i);
            if (TypeCoercionUtils.canHandleTypeCoercion(left.getDataType(), right.getDataType())) {
                Optional<DataType> tightestCommonType =
                        TypeCoercionUtils.findTightestCommonType(null, left.getDataType(), right.getDataType());
                if (tightestCommonType.isPresent()) {
                    Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, tightestCommonType.get());
                    Expression newRight = TypeCoercionUtils.castIfNotSameType(right, tightestCommonType.get());
                    newLeftOutputs.add(newLeft);
                    newRightOutpus.add(newRight);
                    hasPushed = true;
                }
            }

            if (!hasPushed) {
                newLeftOutputs.add(left);
                newRightOutpus.add(right);
            }
        }

        List<List<Expression>> resultExpressions = new ArrayList<>();
        resultExpressions.add(newLeftOutputs);
        resultExpressions.add(newRightOutpus);
        return ImmutableList.copyOf(resultExpressions);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalSetOperation",
                "qualifier", qualifier,
                "outputs", outputs);
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
        return Objects.equals(qualifier, that.qualifier)
                && Objects.equals(outputs, that.outputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier, outputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSetOperation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Qualifier getQualifier() {
        return qualifier;
    }

    @Override
    public List<Slot> getFirstOutput() {
        return child(0).getOutput();
    }

    @Override
    public List<Slot> getChildOutput(int i) {
        return child(i).getOutput();
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputs;
    }

    public abstract Plan withNewOutputs(List<NamedExpression> newOutputs);

    @Override
    public int getArity() {
        return children.size();
    }

    public abstract Plan withNewChildren(List<Plan> children);
}
