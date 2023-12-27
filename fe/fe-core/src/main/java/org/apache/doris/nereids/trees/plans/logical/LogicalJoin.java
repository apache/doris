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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.properties.FunctionalDependencies.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.ImmutableEquivalenceSet;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Logical join plan.
 */
public class LogicalJoin<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements Join {

    private final JoinType joinType;
    private final List<Expression> otherJoinConjuncts;
    private final List<Expression> hashJoinConjuncts;

    // When the predicate condition contains subqueries and disjunctions, the join will be marked as MarkJoin.
    private final Optional<MarkJoinSlotReference> markJoinSlotReference;

    // Use for top-to-down join reorder
    private final JoinReorderContext joinReorderContext = new JoinReorderContext();
    // Table bitmap for tables below this join
    private long bitmap = LongBitmap.newBitmap();

    private JoinHint hint;

    public LogicalJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, JoinHint.NONE,
                Optional.empty(), Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, ExpressionUtils.EMPTY_CONDITION, JoinHint.NONE, Optional.empty(),
                Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts,
            JoinHint hint, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, Optional.empty(), Optional.empty(),
                Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts,
                otherJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(
            long bitmap,
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts,
                otherJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), Optional.empty(), leftChild, rightChild);
        this.bitmap = LongBitmap.or(this.bitmap, bitmap);
    }

    public LogicalJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            List<Plan> children) {
        this(joinType, hashJoinConjuncts,
                otherJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), Optional.empty(), children);
    }

    private LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts,
            JoinHint hint, Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Plan> children, JoinReorderContext joinReorderContext) {
        // Just use in withXXX method. Don't need check/copyOf()
        super(PlanType.LOGICAL_JOIN, groupExpression, logicalProperties, children);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = hashJoinConjuncts;
        this.otherJoinConjuncts = otherJoinConjuncts;
        this.hint = Objects.requireNonNull(hint, "hint can not be null");
        this.joinReorderContext.copyFrom(joinReorderContext);
        this.markJoinSlotReference = markJoinSlotReference;
    }

    private LogicalJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.LOGICAL_JOIN, groupExpression, logicalProperties, leftChild, rightChild);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = ImmutableList.copyOf(hashJoinConjuncts);
        this.otherJoinConjuncts = ImmutableList.copyOf(otherJoinConjuncts);
        this.hint = Objects.requireNonNull(hint, "hint can not be null");
        this.markJoinSlotReference = markJoinSlotReference;
    }

    private LogicalJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        super(PlanType.LOGICAL_JOIN, groupExpression, logicalProperties, children);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = ImmutableList.copyOf(hashJoinConjuncts);
        this.otherJoinConjuncts = ImmutableList.copyOf(otherJoinConjuncts);
        this.hint = Objects.requireNonNull(hint, "hint can not be null");
        this.markJoinSlotReference = markJoinSlotReference;
    }

    public LogicalJoin<? extends Plan, ? extends Plan> swap() {
        return withTypeChildren(getJoinType().swap(),
                right(), left());
    }

    public List<Expression> getOtherJoinConjuncts() {
        return otherJoinConjuncts;
    }

    public List<Expression> getHashJoinConjuncts() {
        return hashJoinConjuncts;
    }

    public Set<Slot> getConditionSlot() {
        return Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream()).collect(ImmutableSet.toImmutableSet());
    }

    public Set<ExprId> getConditionExprId() {
        return Stream.concat(getHashJoinConjuncts().stream(), getOtherJoinConjuncts().stream())
                .flatMap(expr -> expr.getInputSlotExprIds().stream()).collect(Collectors.toSet());
    }

    public Set<Slot> getLeftConditionSlot() {
        Set<Slot> leftOutputSet = this.left().getOutputSet();
        return Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream())
                .filter(leftOutputSet::contains)
                .collect(ImmutableSet.toImmutableSet());
    }

    public Optional<Expression> getOnClauseCondition() {
        return ExpressionUtils.optionalAnd(hashJoinConjuncts, otherJoinConjuncts);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public JoinHint getHint() {
        return hint;
    }

    public void setHint(JoinHint hint) {
        this.hint = hint;
    }

    public boolean isMarkJoin() {
        return markJoinSlotReference.isPresent();
    }

    public JoinReorderContext getJoinReorderContext() {
        return joinReorderContext;
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(JoinUtils.getJoinOutput(joinType, left(), right()))
                .addAll(isMarkJoin()
                        ? ImmutableList.of(markJoinSlotReference.get()) : ImmutableList.of())
                .build();
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList(
                "type", joinType,
                "markJoinSlotReference", markJoinSlotReference,
                "hashJoinConjuncts", hashJoinConjuncts,
                "otherJoinConjuncts", otherJoinConjuncts);
        if (hint != JoinHint.NONE) {
            args.add("hint");
            args.add(hint);
        }
        return Utils.toSqlString("LogicalJoin[" + id.asInt() + "]", args.toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalJoin<?, ?> that = (LogicalJoin<?, ?>) o;
        return joinType == that.joinType
                && hint == that.hint
                && hashJoinConjuncts.equals(that.hashJoinConjuncts)
                && otherJoinConjuncts.equals(that.otherJoinConjuncts)
                && Objects.equals(markJoinSlotReference, that.markJoinSlotReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJoin(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(hashJoinConjuncts)
                .addAll(otherJoinConjuncts)
                .build();
    }

    public Optional<MarkJoinSlotReference> getMarkJoinSlotReference() {
        return markJoinSlotReference;
    }

    public long getBitmap() {
        return bitmap;
    }

    public void setBitmap(long bitmap) {
        this.bitmap = bitmap;
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
    public LogicalJoin<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), Optional.empty(), children, joinReorderContext);
    }

    @Override
    public LogicalJoin<Plan, Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, Optional.of(getLogicalProperties()), children, joinReorderContext);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, logicalProperties, children, joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withChildrenNoContext(Plan left, Plan right) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, left, right);
    }

    public LogicalJoin<Plan, Plan> withJoinConjuncts(
            List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                hint, markJoinSlotReference, children);
    }

    public LogicalJoin<Plan, Plan> withHashJoinConjunctsAndChildren(
            List<Expression> hashJoinConjuncts, Plan left, Plan right) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, left, right);
    }

    public LogicalJoin<Plan, Plan> withConjunctsChildren(List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, Plan left, Plan right) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference, left,
                right);
    }

    public LogicalJoin<Plan, Plan> withJoinType(JoinType joinType) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, children);
    }

    public LogicalJoin<Plan, Plan> withTypeChildren(JoinType joinType, Plan left, Plan right) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, left, right);
    }

    public LogicalJoin<Plan, Plan> withOtherJoinConjuncts(List<Expression> otherJoinConjuncts) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint,
                markJoinSlotReference, children);
    }

    private @Nullable Pair<Set<Slot>, Set<Slot>> extractHashKeys() {
        Set<Slot> leftKeys = new HashSet<>();
        Set<Slot> rightKeys = new HashSet<>();
        for (Expression expression : hashJoinConjuncts) {
            // Note we don't support null-safe predicate right now, because we just check uniqueness for join keys
            if (!(expression instanceof EqualTo
                    && ((EqualTo) expression).left() instanceof Slot
                    && ((EqualTo) expression).right() instanceof Slot)) {
                return null;
            }
            Slot leftKey = (Slot) ((EqualTo) expression).left();
            Slot rightKey = (Slot) ((EqualTo) expression).right();
            if (left().getOutputSet().contains(leftKey)) {
                leftKeys.add(leftKey);
                rightKeys.add(rightKey);
            } else {
                leftKeys.add(rightKey);
                rightKeys.add(leftKey);
            }
        }
        return Pair.of(leftKeys, rightKeys);
    }

    @Override
    public FunctionalDependencies computeFuncDeps(Supplier<List<Slot>> outputSupplier) {
        //1. NALAJ and FOJ block functional dependencies
        if (joinType.isNullAwareLeftAntiJoin() || joinType.isFullOuterJoin()) {
            return FunctionalDependencies.EMPTY_FUNC_DEPS;
        }

        // left/right semi/anti join propagate left/right functional dependencies
        if (joinType.isLeftAntiJoin() || joinType.isLefSemiJoin()) {
            return left().getLogicalProperties().getFunctionalDependencies();
        }
        if (joinType.isRightSemiJoin() || joinType.isRightAntiJoin()) {
            return right().getLogicalProperties().getFunctionalDependencies();
        }

        // if there is non-equal join conditions, block functional dependencies
        if (!otherJoinConjuncts.isEmpty()) {
            return FunctionalDependencies.EMPTY_FUNC_DEPS;
        }

        Pair<Set<Slot>, Set<Slot>> keys = extractHashKeys();
        if (keys == null) {
            return FunctionalDependencies.EMPTY_FUNC_DEPS;
        }

        // Note here we only check whether the left is unique.
        // So the hash condition can't be null-safe
        // TODO: consider Null-safe hash condition when left and rigth is not nullable
        boolean isLeftUnique = left().getLogicalProperties()
                .getFunctionalDependencies().isUnique(keys.first);
        boolean isRightUnique = right().getLogicalProperties()
                .getFunctionalDependencies().isUnique(keys.second);
        Builder fdBuilder = new Builder();
        if (joinType.isInnerJoin()) {
            // inner join propagate uniforms slots
            // And if the hash keys is unique, inner join can propagate all functional dependencies
            if (isLeftUnique && isRightUnique) {
                fdBuilder.addFunctionalDependencies(left().getLogicalProperties().getFunctionalDependencies());
                fdBuilder.addFunctionalDependencies(right().getLogicalProperties().getFunctionalDependencies());
            } else {
                fdBuilder.addUniformSlot(left().getLogicalProperties().getFunctionalDependencies());
                fdBuilder.addUniformSlot(right().getLogicalProperties().getFunctionalDependencies());
            }
        }

        // left/right outer join propagate left/right uniforms slots
        // And if the right/left hash keys is unique,
        // join can propagate left/right functional dependencies
        if (joinType.isLeftOuterJoin()) {
            if (isRightUnique) {
                return left().getLogicalProperties().getFunctionalDependencies();
            }
            fdBuilder.addUniformSlot(left().getLogicalProperties().getFunctionalDependencies());
        }
        if (joinType.isRightOuterJoin()) {
            if (isLeftUnique) {
                return left().getLogicalProperties().getFunctionalDependencies();
            }
            fdBuilder.addUniformSlot(left().getLogicalProperties().getFunctionalDependencies());
        }
        return fdBuilder.build();
    }

    /**
     * get Equal slot from join
     */
    public ImmutableEquivalenceSet<Slot> getEqualSlots() {
        // TODO: Use fd in the future
        if (!joinType.isInnerJoin() && !joinType.isSemiJoin()) {
            return ImmutableEquivalenceSet.of();
        }
        ImmutableEquivalenceSet.Builder<Slot> builder = new ImmutableEquivalenceSet.Builder<>();
        hashJoinConjuncts.stream()
                .filter(e -> e instanceof EqualPredicate
                        && e.child(0) instanceof Slot
                        && e.child(1) instanceof Slot)
                .forEach(e ->
                        builder.addEqualPair((Slot) e.child(0), (Slot) e.child(1)));
        return builder.build();
    }

    @Override
    public JSONObject toJson() {
        JSONObject logicalJoin = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("JoinType", joinType.toString());
        properties.put("HashJoinConjuncts", hashJoinConjuncts.toString());
        properties.put("OtherJoinConjuncts", otherJoinConjuncts.toString());
        properties.put("JoinHint", hint.toString());
        properties.put("MarkJoinSlotReference", markJoinSlotReference.toString());
        logicalJoin.put("Properties", properties);
        return logicalJoin;
    }
}
