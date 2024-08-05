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

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.json.JSONObject;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract class for all physical join node.
 */
public abstract class AbstractPhysicalJoin<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan>
        extends PhysicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements Join {

    protected final JoinType joinType;
    protected final List<Expression> hashJoinConjuncts;
    protected final List<Expression> otherJoinConjuncts;
    protected final List<Expression> markJoinConjuncts;
    protected final DistributeHint hint;
    protected final Optional<MarkJoinSlotReference> markJoinSlotReference;
    protected final List<RuntimeFilter> runtimeFilters = Lists.newArrayList();

    // use for translate only
    protected final List<Expression> filterConjuncts = Lists.newArrayList();
    protected boolean shouldTranslateOutput = true;

    /**
     * Constructor of PhysicalJoin.
     */
    public AbstractPhysicalJoin(
            PlanType type,
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(type, joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION,
                hint, markJoinSlotReference, groupExpression, logicalProperties, null, null,
                leftChild, rightChild);
    }

    /**
     * Constructor of PhysicalJoin.
     */
    public AbstractPhysicalJoin(
            PlanType type,
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(type, joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION,
                hint, markJoinSlotReference, groupExpression, logicalProperties, physicalProperties,
                statistics, leftChild, rightChild);
    }

    protected AbstractPhysicalJoin(
            PlanType type,
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            List<Expression> markJoinConjuncts,
            DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(type, groupExpression, logicalProperties, physicalProperties, statistics, leftChild, rightChild);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = ImmutableList.copyOf(hashJoinConjuncts);
        this.otherJoinConjuncts = ImmutableList.copyOf(otherJoinConjuncts);
        this.markJoinConjuncts = ImmutableList.copyOf(markJoinConjuncts);
        this.hint = hint;
        this.markJoinSlotReference = markJoinSlotReference;
    }

    public List<Expression> getHashJoinConjuncts() {
        return hashJoinConjuncts;
    }

    public boolean isShouldTranslateOutput() {
        return shouldTranslateOutput;
    }

    public void setShouldTranslateOutput(boolean shouldTranslateOutput) {
        this.shouldTranslateOutput = shouldTranslateOutput;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public List<Expression> getOtherJoinConjuncts() {
        return otherJoinConjuncts;
    }

    public boolean isMarkJoin() {
        return markJoinSlotReference.isPresent();
    }

    public List<Expression> getMarkJoinConjuncts() {
        return markJoinConjuncts;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new Builder<Expression>()
                .addAll(hashJoinConjuncts)
                .addAll(otherJoinConjuncts)
                .addAll(markJoinConjuncts).build();
    }

    // TODO:
    // 1. consider the order of conjucts in otherJoinConjuncts and hashJoinConditions
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractPhysicalJoin<?, ?> that = (AbstractPhysicalJoin<?, ?>) o;
        return joinType == that.joinType
                && hashJoinConjuncts.equals(that.hashJoinConjuncts)
                && otherJoinConjuncts.equals(that.otherJoinConjuncts)
                && markJoinConjuncts.equals(that.markJoinConjuncts)
                && hint.equals(that.hint)
                && Objects.equals(markJoinSlotReference, that.markJoinSlotReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts, markJoinSlotReference);
    }

    /**
     * hashJoinConjuncts and otherJoinConjuncts
     *
     * @return the combination of hashJoinConjuncts and otherJoinConjuncts
     */
    public Optional<Expression> getOnClauseCondition() {
        // TODO this function is called by AggScalarSubQueryToWindowFunction and InferPredicates
        //  we assume they can handle mark join correctly
        Optional<Expression> normalJoinConjuncts =
                ExpressionUtils.optionalAnd(hashJoinConjuncts, otherJoinConjuncts);
        return normalJoinConjuncts.isPresent()
                ? ExpressionUtils.optionalAnd(ImmutableList.of(normalJoinConjuncts.get()),
                markJoinConjuncts)
                : ExpressionUtils.optionalAnd(markJoinConjuncts);
    }

    @Override
    public DistributeHint getDistributeHint() {
        return hint;
    }

    public List<Expression> getFilterConjuncts() {
        return filterConjuncts;
    }

    public Optional<MarkJoinSlotReference> getMarkJoinSlotReference() {
        return markJoinSlotReference;
    }

    public void addFilterConjuncts(Collection<Expression> conjuncts) {
        filterConjuncts.addAll(conjuncts);
    }

    @Override
    public JSONObject toJson() {
        JSONObject physicalJoin = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("JoinType", joinType.toString());
        properties.put("HashJoinConjuncts", hashJoinConjuncts.toString());
        properties.put("OtherJoinConjuncts", otherJoinConjuncts.toString());
        properties.put("MarkJoinConjuncts", markJoinConjuncts.toString());
        properties.put("JoinHint", hint.toString());
        properties.put("MarkJoinSlotReference", markJoinSlotReference.toString());
        physicalJoin.put("Properties", properties);
        return physicalJoin;
    }

    public void addRuntimeFilter(RuntimeFilter rf) {
        runtimeFilters.add(rf);
    }

    public List<RuntimeFilter> getRuntimeFilters() {
        return runtimeFilters;
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(JoinUtils.getJoinOutput(joinType, left(), right()))
                .addAll(isMarkJoin()
                        ? ImmutableList.of(markJoinSlotReference.get()) : ImmutableList.of())
                .build();
    }

    /**
     * getConditionSlot
     */
    public Set<Slot> getConditionSlot() {
        return Stream.concat(Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream()),
                markJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream()).collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList("type", joinType,
                "stats", statistics,
                "hashCondition", hashJoinConjuncts,
                "otherCondition", otherJoinConjuncts,
                "markCondition", markJoinConjuncts);
        if (markJoinSlotReference.isPresent()) {
            args.add("isMarkJoin");
            args.add("true");
        }
        if (markJoinSlotReference.isPresent()) {
            args.add("MarkJoinSlotReference");
            args.add(markJoinSlotReference.get());
        }
        if (hint.distributeType != DistributeType.NONE) {
            args.add("hint");
            args.add(hint.getExplainString());
        }
        if (!runtimeFilters.isEmpty()) {
            args.add("runtimeFilters");
            args.add(runtimeFilters.stream().map(rf -> rf.toString() + " ").collect(Collectors.toList()));
        }
        return Utils.toSqlString(this.getClass().getSimpleName() + "[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                args.toArray());
    }

    /**
     * true if this is a broadcast join
     */
    public boolean isBroadCastJoin() {
        if (child(1) instanceof PhysicalDistribute) {
            DistributionSpec distSpec = ((PhysicalDistribute) child(1)).getDistributionSpec();
            if (distSpec instanceof DistributionSpecReplicated) {
                return true;
            }
        }
        return false;
    }

    protected Join.ShuffleType shuffleType() {
        if (left() instanceof PhysicalDistribute) {
            if (right() instanceof PhysicalDistribute) {
                return ShuffleType.shuffle;
            } else {
                return ShuffleType.shuffleBucket;
            }
        }
        if (right() instanceof PhysicalDistribute) {
            PhysicalDistribute buildDist = (PhysicalDistribute) right();
            if (buildDist.getDistributionSpec() == DistributionSpecReplicated.INSTANCE) {
                return ShuffleType.broadcast;
            } else if (buildDist.getDistributionSpec() instanceof DistributionSpecHash) {
                return ShuffleType.bucketShuffle;
            }
            return ShuffleType.unknown;
        }
        return ShuffleType.colocated;
    }
}
