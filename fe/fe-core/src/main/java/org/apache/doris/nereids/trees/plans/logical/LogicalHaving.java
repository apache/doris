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
import org.apache.doris.nereids.properties.FdItem;
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
 * Logical Having plan
 *
 * @param <CHILD_TYPE> Types which inherit from {@link Plan}
 */
public class LogicalHaving<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Filter {

    private final Set<Expression> conjuncts;

    public LogicalHaving(Set<Expression> conjuncts, CHILD_TYPE child) {
        this(conjuncts, Optional.empty(), Optional.empty(), child);
    }

    private LogicalHaving(Set<Expression> conjuncts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_HAVING, groupExpression, logicalProperties, child);
        this.conjuncts = ImmutableSet.copyOf(Objects.requireNonNull(conjuncts, "conjuncts can not be null"));
    }

    @Override
    public Set<Expression> getConjuncts() {
        return conjuncts;
    }

    public List<Expression> getExpressions() {
        return ImmutableList.copyOf(conjuncts);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalHaving<>(conjuncts, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalHaving(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalHaving<>(conjuncts, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalHaving<>(conjuncts, groupExpression, logicalProperties, children.get(0));
    }

    public Plan withExpressions(Set<Expression> expressions) {
        return new LogicalHaving<Plan>(expressions, Optional.empty(),
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(conjuncts);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LogicalHaving) || getClass() != object.getClass()) {
            return false;
        }
        LogicalHaving other = (LogicalHaving) object;
        return conjuncts.equals(other.conjuncts);
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
    public ImmutableSet<FdItem> computeFdItems() {
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();

        ImmutableSet<FdItem> childItems = child().getLogicalProperties().getTrait().getFdItems();
        builder.addAll(childItems);

        return builder.build();
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

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalHaving", "predicates", getPredicate());
    }
}
