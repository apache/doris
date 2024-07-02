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
import org.apache.doris.nereids.trees.plans.commands.info.SplitColumnInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.Range;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a logical dynamic split node within a query plan.
 * <p>
 * This class extends LogicalUnary and implements the Filter interface, designed to facilitate
 * dynamic splitting operations within a query execution plan. Dynamic splitting enables
 * runtime adjustments to the execution strategy of a query plan based on specific conditions,
 * enhancing query performance or adapting to data variations.
 * <p>
 */
public class LogicalDynamicSplit<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Filter {

    private Range range;

    private SplitColumnInfo splitColumnInfo;

    private AtomicBoolean replaced;

    public LogicalDynamicSplit(SplitColumnInfo splitColumnInfo, Range range, AtomicBoolean replaced, CHILD_TYPE child) {

        this(splitColumnInfo, range, replaced, Optional.empty(), Optional.empty(), child);
    }

    public LogicalDynamicSplit(SplitColumnInfo splitColumnInfo, Range range, AtomicBoolean replaced,
                               Optional<GroupExpression> groupExpression,
                               Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_BATCH_FILTER, groupExpression, logicalProperties, child);
        this.range = range;
        this.splitColumnInfo = splitColumnInfo;
        this.replaced = replaced;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalDynamicSplit[" + id.asInt() + "]",
                "splitColumnInfo", splitColumnInfo, "range", range
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
        LogicalDynamicSplit that = (LogicalDynamicSplit) o;
        return range.equals(that.range)
                && splitColumnInfo.equals(that.splitColumnInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitColumnInfo, range);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDynamicSplit(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ArrayList<>();
    }

    @Override
    public LogicalDynamicSplit<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalDynamicSplit<>(splitColumnInfo, range, replaced, children.get(0));
    }

    @Override
    public LogicalDynamicSplit<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalDynamicSplit<>(splitColumnInfo, range, replaced,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalDynamicSplit<>(splitColumnInfo, range,
                replaced, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems() {
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();

        ImmutableSet<FdItem> childItems = child().getLogicalProperties().getTrait().getFdItems();
        builder.addAll(childItems);

        return builder.build();
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

    @Override
    public List<Slot> computeOutput() {
        return new ArrayList<>();
    }

    @Override
    public Set<Expression> getConjuncts() {
        return new HashSet<>();
    }

    public Range getRange() {
        return range;
    }

    public SplitColumnInfo getSplitColumnInfo() {
        return splitColumnInfo;
    }

    public AtomicBoolean isReplaced() {
        return replaced;
    }

    public void setReplaced(Boolean replaced) {
        this.replaced.set(replaced);
    }

}
