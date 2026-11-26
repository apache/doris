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

package org.apache.doris.mtmv.ivm.agg;

import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountAtMax;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountAtMin;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxTopK;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinTopK;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopKFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Template for one IVM-supported aggregate function family.
 *
 * <p>Subclasses describe the parts that vary by function:
 * <ol>
 *   <li>Which original aggregate function it accepts.</li>
 *   <li>Which hidden state columns normalize must add.</li>
 *   <li>Which delta aggregate columns are produced.</li>
 *   <li>How old MV state and delta slots are merged into final output expressions.</li>
 * </ol>
 */
public abstract class IvmAggFunctionProcessor {
    /** Returns true when this processor owns the original aggregate function from the MV definition. */
    abstract boolean supportsOriginalFunction(AggregateFunction function);

    /** Stable function kind stored in {@link IvmAggTarget} after normalize, used to find this processor later. */
    abstract IvmAggFunctionKind handledFunctionKind();

    /**
     * Common normalize implementation: visible alias stays visible, and processor-declared state keys become hidden
     * aliases. An existing aggregate column is reused when the required hidden state {@code (aggClass, argExpr)}
     * is already present in {@code aggColumnPool}; otherwise a new hidden column is created, registered in the
     * pool, and appended to {@code hiddenAggOutputs}.
     */
    IvmAggTargetSpec buildTargetSpec(int ordinal, AggregateFunction function, Alias visibleAlias,
            Map<IvmAggColumnKey, Slot> aggColumnPool, List<NamedExpression> hiddenAggOutputs,
            List<NamedExpression> internalAggOutputs, long minMaxTopK) {
        Set<IvmAggStateKey> internalKeys = new LinkedHashSet<>(internalStateKeys(function, minMaxTopK));
        List<IvmAggStateKey> aggregateKeys = new ArrayList<>(hiddenStateKeys(function, minMaxTopK));
        aggregateKeys.addAll(internalKeys);
        ImmutableMap.Builder<IvmAggStateKey, Alias> hiddenAliases = ImmutableMap.builder();
        for (IvmAggStateKey stateKey : aggregateKeys) {
            AggregateFunction stateFunc = hiddenStateAggregate(stateKey, function, minMaxTopK);
            IvmAggColumnKey key = IvmAggColumnKey.of(
                    IvmAggFunctionKind.valueOf(stateKey.name()), stateFunc.child(0));
            Slot existingSlot = aggColumnPool.get(key);
            if (existingSlot != null) {
                hiddenAliases.put(stateKey, new Alias(existingSlot, existingSlot.getName()));
                continue;
            }
            String colName = IvmUtil.ivmAggHiddenColumnName(ordinal, stateKey.name());
            Alias newAlias = new Alias(stateFunc, colName);
            hiddenAliases.put(stateKey, newAlias);
            aggColumnPool.put(key, newAlias.toSlot());
            // Internal outputs go after every persisted state: the delta apply rebuilds the
            // aggregate output positionally, and skipping them only stays aligned at the tail.
            (internalKeys.contains(stateKey) ? internalAggOutputs : hiddenAggOutputs).add(newAlias);
        }
        // Derived expressions may read every aggregate output, internal ones included.
        ImmutableMap<IvmAggStateKey, Alias> stateAliases = hiddenAliases.build();
        ImmutableMap.Builder<IvmAggStateKey, Alias> derivedAliases = ImmutableMap.builder();
        for (IvmAggStateKey stateKey : derivedStateKeys(function, minMaxTopK)) {
            String colName = IvmUtil.ivmAggHiddenColumnName(ordinal, stateKey.name());
            derivedAliases.put(stateKey, new Alias(
                    derivedStateExpression(stateKey, function, stateAliases, visibleAlias, minMaxTopK),
                    colName));
        }
        return new IvmAggTargetSpec(ordinal, handledFunctionKind(), visibleAlias,
                stateAliases, internalKeys, derivedAliases.build(), targetArguments(function), minMaxTopK);
    }

    /** Appends the processor-owned delta aggregate outputs for this target. */
    abstract void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot, List<NamedExpression> outputs,
            IvmAggExpressionBuilder ctx);

    /**
     * Appends processor-owned derived (non-aggregate) delta columns on top of the delta aggregate.
     *
     * <p>Some processors pack every change row into one aggregate column and then derive the polarity
     * columns (for example ARRAY_AGG's insert/delete views) with scalar functions over that packed
     * aggregate output. Such derived columns cannot be aggregate outputs of the same node, so this
     * hook appends them to the delta top project's output list, where {@code deltaAggOutputByName}
     * exposes the delta aggregate's own output slots.
     */
    void appendDeltaTopProjectOutputs(IvmAggTarget target, Map<String, Slot> deltaAggOutputByName,
            List<NamedExpression> topOutputs, IvmAggExpressionBuilder ctx) {
    }

    /** Resolves delta output slots and registers the slots that apply expressions will read later. */
    void mapApplyDeltaSlots(IvmAggTarget target, Map<String, Slot> outputByName,
            Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, Slot deltaGroupCountSlot, IvmAggExpressionBuilder ctx) {
        for (IvmAggFunctionKind slotKind : applyDeltaSlotKinds(target)) {
            applyDeltaSlots.put(ctx.deltaSlotRef(target, slotKind),
                    resolveDeltaOutputSlot(target, slotKind, outputByName, deltaGroupCountSlot, ctx));
        }
    }

    /** Adds processor-owned delta output names where NULL has the same merge meaning as numeric zero. */
    void collectZeroDefaultDeltaOutputNames(IvmAggTarget target, boolean scalarAgg,
            Set<String> outputNames, IvmAggExpressionBuilder ctx) {
        for (IvmAggFunctionKind slotKind : zeroDefaultSlotKinds(target, scalarAgg)) {
            outputNames.add(ctx.deltaColumnName(target, slotKind));
        }
    }

    /** Appends final visible and hidden-state expressions into {@link IvmAggApplyContext}. */
    abstract void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext ctx);

    /** Hidden state columns this function needs in the normalized MV schema. */
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function, long minMaxTopK) {
        return ImmutableList.of();
    }

    /**
     * Aggregate outputs this function needs only to derive another state column. They are part of
     * the normalized aggregate's output list, but the project above it computes the derived state
     * from them and does not project them, so they never become MV columns.
     */
    protected List<IvmAggStateKey> internalStateKeys(AggregateFunction function, long minMaxTopK) {
        return ImmutableList.of();
    }

    /**
     * Hidden state columns computed by the project above the normalized aggregate from the
     * {@link #hiddenStateKeys} and {@link #internalStateKeys} columns. They have no aggregate of
     * their own, so they are not part of the aggregate output list.
     */
    protected List<IvmAggStateKey> derivedStateKeys(AggregateFunction function, long minMaxTopK) {
        return ImmutableList.of();
    }

    /**
     * Builds the expression of one {@link #derivedStateKeys} state from the aggregate output
     * {@code stateAliases} (which includes {@link #internalStateKeys}) and the visible aggregate
     * alias, in the project above the normalized aggregate.
     */
    protected Expression derivedStateExpression(IvmAggStateKey stateKey, AggregateFunction function,
            Map<IvmAggStateKey, Alias> stateAliases, Alias visibleAlias, long minMaxTopK) {
        throw new IllegalArgumentException("Unsupported derived IVM aggregate state: " + stateKey);
    }

    /**
     * Returns true when the visible MV column carries this target's own aggregate value state.
     *
     * <p>Apply of such a target merges the old value read from the visible column (SUM, COUNT(expr),
     * MIN/MAX, COLLECT_LIST/ARRAY_AGG, BITMAP_UNION). Returns false when the old value is derived
     * from hidden state instead (AVG's SUM/COUNT, BITMAP_UNION_COUNT's bitmap) or from the group
     * count (COUNT(*)), which are hidden-named columns and always survive to the MV schema.
     *
     * <p>The visible column is not guaranteed to survive: an upper expression may consume it without
     * projecting it (for example {@code SELECT SUM(v) * 100}). Normalize keeps every slot apply reads
     * alive by materializing a hidden carrier column for such targets
     * ({@link IvmAggTarget#getValueStateColumnName()}).
     */
    boolean visibleColumnHoldsValueState(IvmAggTarget target) {
        return true;
    }

    /** Original aggregate arguments stored in {@link IvmAggTarget}; COUNT(*) stores an empty list. */
    protected List<Expression> targetArguments(AggregateFunction function) {
        return ImmutableList.of(function.child(0));
    }

    /**
     * Aggregate function used to materialize one hidden aggregate state during normalize.
     *
     * <p>{@code minMaxTopK} is the MV's resolved MIN/MAX capacity; only the MIN_TOPK/MAX_TOPK states
     * read it, the other states ignore it.
     */
    protected AggregateFunction hiddenStateAggregate(IvmAggStateKey stateKey, AggregateFunction function,
            long minMaxTopK) {
        return switch (stateKey) {
            case COUNT -> new Count(function.child(0));
            case SUM -> new Sum(function.child(0));
            case BITMAP_UNION -> new BitmapUnion(function.child(0));
            case MIN_TOPK -> new MinTopK(function.child(0), new BigIntLiteral(minMaxTopK));
            case MAX_TOPK -> new MaxTopK(function.child(0), new BigIntLiteral(minMaxTopK));
            case MIN_COUNT -> new CountAtMin(function.child(0));
            case MAX_COUNT -> new CountAtMax(function.child(0));
            case MIN_TOPK_BOUND, MAX_TOPK_BOUND -> throw new IllegalArgumentException(
                    "Derived IVM aggregate state has no materializing aggregate: " + stateKey);
        };
    }

    /**
     * The one-pair top-K array of a MIN/MAX state with capacity one, in the same
     * {@code ARRAY<STRUCT<value, count>>} shape the top-K aggregate returns: one pair when the group
     * has a non-NULL value, the empty array otherwise.
     */
    protected static Expression singlePairArray(Expression value, Expression count) {
        DataType valueType = TopKFunction.normalizeValueType(value.getDataType());
        ArrayType stateType = TopKFunction.resultType(valueType);
        Expression pair = TypeCoercionUtils.castIfNotMatchType(
                new CreateNamedStruct(
                        new StringLiteral("value"),
                        TypeCoercionUtils.castIfNotMatchType(value, valueType),
                        new StringLiteral("count"), count),
                stateType.getItemType());
        return new If(new GreaterThan(count, new BigIntLiteral(0L)), new Array(pair),
                new ArrayLiteral(ImmutableList.of(), stateType));
    }

    /** Logical delta slots this processor exposes to the apply stage. */
    protected List<IvmAggFunctionKind> applyDeltaSlotKinds(IvmAggTarget target) {
        return ImmutableList.of();
    }

    /** Resolves one processor-owned delta slot from the delta top project's output slots. */
    protected Slot resolveDeltaOutputSlot(IvmAggTarget target, IvmAggFunctionKind slotKind,
            Map<String, Slot> outputByName, Slot deltaGroupCountSlot, IvmAggExpressionBuilder ctx) {
        String columnName = ctx.deltaColumnName(target, slotKind);
        Slot slot = outputByName.get(columnName);
        if (slot == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM agg delta rewrite failed to resolve delta output slot: "
                    + columnName + " for target " + target);
        }
        return slot;
    }

    /** Delta slots where NULL has the same merge meaning as numeric zero. */
    protected List<IvmAggFunctionKind> zeroDefaultSlotKinds(IvmAggTarget target, boolean scalarAgg) {
        return ImmutableList.of();
    }
}
