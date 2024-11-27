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
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical qualify plan.
 */
public class LogicalQualify<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Filter {

    private final Set<Expression> conjuncts;

    public LogicalQualify(Set<Expression> conjuncts, CHILD_TYPE child) {
        this(conjuncts, Optional.empty(), Optional.empty(), child);
    }

    private LogicalQualify(Set<Expression> conjuncts, Optional<GroupExpression> groupExpression,
                           Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_QUALIFY, groupExpression, logicalProperties, child);
        this.conjuncts = ImmutableSet.copyOf(Objects.requireNonNull(conjuncts, "conjuncts can not be null"));
    }

    @Override
    public Set<Expression> getConjuncts() {
        return conjuncts;
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalQualify<>(conjuncts, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalQualify<>(conjuncts, groupExpression, logicalProperties, children.get(0));
    }

    public LogicalQualify<Plan> withConjuncts(Set<Expression> conjuncts) {
        return new LogicalQualify<>(conjuncts, Optional.empty(), Optional.of(getLogicalProperties()), child());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalQualify[" + id.asInt() + "]",
                "predicates", getPredicate()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalQualify that = (LogicalQualify) o;
        return conjuncts.equals(that.conjuncts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conjuncts);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalQualify(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.copyOf(conjuncts);
    }

    @Override
    public LogicalQualify<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalQualify<>(conjuncts, children.get(0));
    }

    @Override
    public void computeUnique(Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeUniform(Builder builder) {
        for (Expression e : getConjuncts()) {
            Set<Slot> uniformSlots = ExpressionUtils.extractUniformSlot(e);
            for (Slot slot : uniformSlots) {
                builder.addUniformSlot(slot);
            }
        }
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeEqualSet(Builder builder) {
        builder.addEqualSet(child().getLogicalProperties().getTrait());
        for (Expression expression : getConjuncts()) {
            Optional<Pair<Slot, Slot>> equalSlot = ExpressionUtils.extractEqualSlot(expression);
            equalSlot.ifPresent(slotSlotPair -> builder.addEqualPair(slotSlotPair.first, slotSlotPair.second));
        }
    }

    @Override
    public void computeFd(Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
