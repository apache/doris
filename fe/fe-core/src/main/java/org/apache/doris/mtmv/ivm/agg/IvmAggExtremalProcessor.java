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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxTopK;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinTopK;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopKFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFilter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySlice;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySortedMerge;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Shared delta/apply logic for MIN/MAX.
 *
 * <p>The MV keeps a top-K state per group in two hidden columns: {@code MIN_TOPK} (or
 * {@code MAX_TOPK}) holds {@code ARRAY<STRUCT<value, count>>}, and {@code MIN_TOPK_BOUND} holds the
 * trusted range bound {@code b} ({@code NULL} means complete). The delta produces the insert-side
 * and delete-side top-K arrays of the batch; apply merges them with the pre-image array through
 * generic array functions and writes both hidden columns back. Only values within
 * {@code min(pre-image bound, both delta bounds)} are trusted; when that range is used up the
 * assert triggers the existing complete-refresh fallback ({@code MIN_MAX_BOUNDARY_HIT}).
 *
 * <p>The delta arrays carry no truncation flag: a side whose array holds exactly {@code K} entries
 * is conservatively treated as truncated, and its last entry is the side's bound.
 *
 * <p>{@code K = 0} is the extreme-only mode used for append-only plans: the state array holds at
 * most the single {@code (extreme, count)} pair. A non-empty delete-side array means a non-NULL
 * value was retracted, which breaks the premise the mode relies on, so apply falls back; otherwise
 * the new pair is the better of the pre-image and inserted extremes, summing the counts when the
 * values agree.
 *
 * <p>{@code K <= 1} always leaves a single pair at most, so that state is never materialized by a
 * top-K aggregate: the aggregate emits the visible MIN/MAX column plus an internal
 * {@code COUNT_AT_MIN/MAX}, and the project above it derives the pair array from them. The persisted
 * columns are then the same as for {@code K >= 2}, and apply is unchanged.
 */
abstract class IvmAggExtremalProcessor extends IvmAggFunctionProcessor {
    private static final String INSERT_VALUE_SLOT = "EXTREME_INS_VALUE";
    private static final String INSERT_COUNT_SLOT = "EXTREME_INS_COUNT";
    private static final String DELETE_VALUE_SLOT = "EXTREME_DEL_VALUE";
    private static final String DELETE_COUNT_SLOT = "EXTREME_DEL_COUNT";

    private final IvmAggFunctionKind functionKind;
    private final IvmAggStateKey topKStateKey;
    private final IvmAggStateKey boundStateKey;
    private final IvmAggStateKey countStateKey;
    private final String deleteSlotName;

    IvmAggExtremalProcessor(IvmAggFunctionKind functionKind, IvmAggStateKey topKStateKey,
            IvmAggStateKey boundStateKey, String deleteSlotName) {
        this.functionKind = functionKind;
        this.topKStateKey = topKStateKey;
        this.boundStateKey = boundStateKey;
        this.countStateKey = topKStateKey == IvmAggStateKey.MIN_TOPK
                ? IvmAggStateKey.MIN_COUNT : IvmAggStateKey.MAX_COUNT;
        this.deleteSlotName = deleteSlotName;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return functionKind;
    }

    @Override
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function, long minMaxTopK) {
        return minMaxTopK <= 1
                ? ImmutableList.of(IvmAggStateKey.COUNT)
                : ImmutableList.of(IvmAggStateKey.COUNT, topKStateKey);
    }

    /**
     * With {@code K <= 1} the top-K array is at most one (value, count) pair, so it is derived from
     * the visible MIN/MAX column and COUNT_AT_MIN/MAX instead of materializing the top-K state.
     */
    @Override
    protected List<IvmAggStateKey> internalStateKeys(AggregateFunction function, long minMaxTopK) {
        return minMaxTopK <= 1 ? ImmutableList.of(countStateKey) : ImmutableList.of();
    }

    @Override
    protected List<IvmAggStateKey> derivedStateKeys(AggregateFunction function, long minMaxTopK) {
        return minMaxTopK <= 1
                ? ImmutableList.of(topKStateKey, boundStateKey)
                : ImmutableList.of(boundStateKey);
    }

    @Override
    protected Expression derivedStateExpression(IvmAggStateKey stateKey, AggregateFunction function,
            Map<IvmAggStateKey, Alias> stateAliases, Alias visibleAlias, long minMaxTopK) {
        if (minMaxTopK <= 1) {
            Slot count = stateAliases.get(countStateKey).toSlot();
            if (stateKey == topKStateKey) {
                return singlePairArray(visibleAlias.toSlot(), count);
            }
            DataType valueType = TopKFunction.normalizeValueType(visibleAlias.toSlot().getDataType());
            if (minMaxTopK == 0) {
                // The extreme-only mode only runs on append-only plans, so nothing is retracted and
                // the bound stays NULL, as it always has.
                return new NullLiteral(valueType);
            }
            // A single retained value is trusted exactly: the bound is the value itself.
            return new If(new GreaterThan(count, new BigIntLiteral(0L)),
                    TypeCoercionUtils.castIfNotMatchType(visibleAlias.toSlot(), valueType),
                    new NullLiteral(valueType));
        }
        Slot values = stateAliases.get(topKStateKey).toSlot();
        DataType valueType = valueTypeOf((ArrayType) values.getDataType());
        return new CaseWhen(
                ImmutableList.of(new WhenClause(
                        new GreaterThanEqual(new Cardinality(values), new BigIntLiteral(minMaxTopK)),
                        elementAt(elementAt(values, minMaxTopK), "value"))),
                new NullLiteral(valueType));
    }

    /**
     * The pre-image array and bound live in the hidden state columns; the visible column is an
     * output apply computes, not a state carrier.
     */
    @Override
    boolean visibleColumnHoldsValueState(IvmAggTarget target) {
        return false;
    }

    @Override
    public void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot,
            List<NamedExpression> outputs, IvmAggExpressionBuilder ctx) {
        Expression arg = target.getExprArgs().get(0);
        long topK = target.getMinMaxTopK();
        if (topK <= 1) {
            // An aggregate output cannot hold a scalar function over two aggregates, so the sides
            // are emitted as plain MIN/MAX plus COUNT_AT_MIN/MAX and assembled by the delta top
            // project (see appendDeltaTopProjectOutputs).
            Expression inserted = ctx.insertOnlyValue(arg, dmlFactorSlot);
            Expression deleted = ctx.deleteOnlyValue(arg, dmlFactorSlot);
            outputs.add(new Alias(buildExtremeAggregate(inserted),
                    ctx.transientDeltaColumnName(target, INSERT_VALUE_SLOT)));
            outputs.add(new Alias(buildExtremeCountAggregate(inserted),
                    ctx.transientDeltaColumnName(target, INSERT_COUNT_SLOT)));
            outputs.add(new Alias(buildExtremeAggregate(deleted),
                    ctx.transientDeltaColumnName(target, DELETE_VALUE_SLOT)));
            outputs.add(new Alias(buildExtremeCountAggregate(deleted),
                    ctx.transientDeltaColumnName(target, DELETE_COUNT_SLOT)));
        } else {
            outputs.add(new Alias(buildTopKState(ctx.insertOnlyValue(arg, dmlFactorSlot), topK),
                    ctx.deltaColumnName(target, functionKind)));
            outputs.add(new Alias(buildTopKState(ctx.deleteOnlyValue(arg, dmlFactorSlot), topK),
                    deleteDeltaColumnName(target, ctx)));
        }
        outputs.add(new Alias(new Sum(ctx.nonNullDeltaCountValue(arg, dmlFactorSlot)),
                ctx.deltaColumnName(target, IvmAggFunctionKind.COUNT)));
    }

    /**
     * Assembles the two K &lt;= 1 delta arrays above the delta aggregate, where a scalar function over
     * two aggregate outputs is allowed. The names match the K &gt;= 2 aggregate outputs, so the
     * apply stage resolves them the same way.
     */
    @Override
    void appendDeltaTopProjectOutputs(IvmAggTarget target, Map<String, Slot> deltaAggOutputByName,
            List<NamedExpression> topOutputs, IvmAggExpressionBuilder ctx) {
        if (target.getMinMaxTopK() > 1) {
            return;
        }
        topOutputs.add(new Alias(
                singlePairArray(requireDeltaOutput(target, INSERT_VALUE_SLOT, deltaAggOutputByName, ctx),
                        requireDeltaOutput(target, INSERT_COUNT_SLOT, deltaAggOutputByName, ctx)),
                ctx.deltaColumnName(target, functionKind)));
        topOutputs.add(new Alias(
                singlePairArray(requireDeltaOutput(target, DELETE_VALUE_SLOT, deltaAggOutputByName, ctx),
                        requireDeltaOutput(target, DELETE_COUNT_SLOT, deltaAggOutputByName, ctx)),
                deleteDeltaColumnName(target, ctx)));
    }

    private Slot requireDeltaOutput(IvmAggTarget target, String slotName,
            Map<String, Slot> deltaAggOutputByName, IvmAggExpressionBuilder ctx) {
        String columnName = ctx.transientDeltaColumnName(target, slotName);
        Slot slot = deltaAggOutputByName.get(columnName);
        if (slot == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM agg delta rewrite failed to resolve delta output slot: "
                            + columnName + " for target " + target);
        }
        return slot;
    }

    @Override
    protected List<IvmAggFunctionKind> applyDeltaSlotKinds(IvmAggTarget target) {
        return ImmutableList.of(functionKind, IvmAggFunctionKind.COUNT);
    }

    @Override
    void mapApplyDeltaSlots(IvmAggTarget target, Map<String, Slot> outputByName,
            Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, Slot deltaGroupCountSlot,
            IvmAggExpressionBuilder ctx) {
        super.mapApplyDeltaSlots(target, outputByName, applyDeltaSlots, deltaGroupCountSlot, ctx);
        resolveDeltaSlot(target, deleteSlotName, outputByName, applyDeltaSlots, ctx);
    }

    @Override
    protected List<IvmAggFunctionKind> zeroDefaultSlotKinds(IvmAggTarget target, boolean scalarAgg) {
        return scalarAgg ? ImmutableList.of(IvmAggFunctionKind.COUNT) : ImmutableList.of();
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        Slot stateSlot = Objects.requireNonNull(target.getHiddenStateSlot(topKStateKey),
                "missing " + topKStateKey + " state for " + target);
        Slot boundSlot = Objects.requireNonNull(target.getHiddenStateSlot(boundStateKey),
                "missing " + boundStateKey + " state for " + target);
        Slot countSlot = Objects.requireNonNull(target.getHiddenStateSlot(IvmAggStateKey.COUNT),
                "missing count state for " + target);
        long topK = target.getMinMaxTopK();

        ArrayType valuesType = (ArrayType) stateSlot.getDataType();
        DataType valueType = valueTypeOf(valuesType);
        Expression oldValues = nonNullArray(applyContext.rawMvSlot(stateSlot.getName()), valuesType);
        Expression oldBound = applyContext.rawMvSlot(boundSlot.getName());
        Expression oldCount = ctx.zeroIfNull(applyContext.rawMvSlot(countSlot.getName()));
        Expression deltaCount = applyContext.deltaStateValue(target, IvmAggStateKey.COUNT);
        Expression newCount = applyContext.buildNewHiddenCount(target);
        Expression inserted = nonNullArray(applyContext.deltaSlotValue(target, functionKind),
                valuesType);
        Expression deleted = nonNullArray(applyContext.deltaSlotValue(target, deleteSlotRef(target)),
                valuesType);

        Expression stateExpr;
        Expression boundExpr;
        Expression extreme;
        if (topK == 0) {
            // A non-empty delete side means a non-NULL value was retracted: the append-only
            // premise the extreme-only mode relies on is broken, so the merge is not trustworthy.
            Expression guard = new AssertTrue(
                    new EqualTo(new Cardinality(deleted), new BigIntLiteral(0L)),
                    new StringLiteral(fallbackMessage()));
            stateExpr = guarded(guard, mergeExtreme(oldValues, inserted, newCount, valuesType),
                    valuesType);
            boundExpr = new NullLiteral(valueType);
            extreme = elementAt(elementAt(stateExpr, 1L), "value");
        } else {
            // A side holding exactly K entries may have dropped values, so its last entry bounds
            // the values every side is known to cover.
            Expression insertBound = deltaBound(inserted, topK, valueType);
            Expression deleteBound = deltaBound(deleted, topK, valueType);
            Expression effectiveBound = nonNullExtremum(oldBound, insertBound, deleteBound);

            Expression candidates = mergeValues(oldValues, inserted, deleted, effectiveBound,
                    valuesType);
            // No non-NULL value is left: the state is complete again, so nothing is carried over
            // even if an untrusted deletion outside the bound survived the merge.
            Expression values = new ArraySlice(candidates, new BigIntLiteral(1L),
                    new BigIntLiteral(topK));
            Expression stateValues = new If(new EqualTo(newCount, new BigIntLiteral(0L)),
                    new ArrayLiteral(ImmutableList.of(), valuesType), values);
            // The trusted range is used up when nothing survived while the group still has rows.
            Expression exhausted = new And(
                    new EqualTo(new Cardinality(candidates), new BigIntLiteral(0L)),
                    ctx.isPositive(newCount));
            Expression guard = new AssertTrue(new Not(exhausted),
                    new StringLiteral(fallbackMessage()));
            stateExpr = guarded(guard, stateValues, valuesType);
            boundExpr = buildBound(oldBound, insertBound, deleteBound, effectiveBound, candidates,
                    oldCount, deltaCount, topK, valueType);
            extreme = elementAt(elementAt(stateExpr, 1L), "value");
        }

        applyContext.putFinalExpression(target, stateSlot.getName(), stateExpr);
        applyContext.putFinalExpression(target, boundSlot.getName(), boundExpr);
        applyContext.putFinalExpression(target, countSlot.getName(), newCount);
        applyContext.putFinalExpression(target, target.getVisibleSlot().getName(),
                new If(ctx.isPositive(newCount),
                        TypeCoercionUtils.castIfNotMatchType(extreme,
                                target.getVisibleSlot().getDataType()),
                        new NullLiteral(target.getVisibleSlot().getDataType())));
    }

    private static Expression guarded(Expression guard, Expression value, DataType type) {
        return new If(guard, value, new NullLiteral(type));
    }

    private static Expression elementAt(Expression struct, String field) {
        return new ElementAt(struct, new StringLiteral(field));
    }

    private static Expression elementAt(Expression array, long index) {
        return new ElementAt(array, new BigIntLiteral(index));
    }

    /** The value type stored in the top-K pairs. */
    private static DataType valueTypeOf(ArrayType valuesType) {
        return ((StructType) valuesType.getItemType()).getField("value").getDataType();
    }

    /**
     * The delta's truncation point inferred from its length: {@code b = cardinality(array) == K ?
     * element_at(array, K).value : NULL}, where NULL means "not truncated". A delta holding exactly
     * K distinct values is conservatively treated as truncated.
     */
    private static Expression deltaBound(Expression array, long topK, DataType valueType) {
        return new CaseWhen(
                ImmutableList.of(new WhenClause(
                        new EqualTo(new Cardinality(array), new BigIntLiteral(topK)),
                        elementAt(elementAt(array, topK), "value"))),
                new NullLiteral(valueType));
    }

    private void resolveDeltaSlot(IvmAggTarget target, String slotName, Map<String, Slot> outputByName,
            Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, IvmAggExpressionBuilder ctx) {
        String columnName = ctx.transientDeltaColumnName(target, slotName);
        Slot slot = outputByName.get(columnName);
        if (slot == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM agg delta rewrite failed to resolve delta output slot: "
                            + columnName + " for target " + target);
        }
        applyDeltaSlots.put(new IvmAggDeltaSlotRef(target.getOrdinal(), slotName), slot);
    }

    /** The side's top-K aggregate (MIN_TOPK/MAX_TOPK) with the MV's capacity. */
    private Expression buildTopKState(Expression input, long topK) {
        return isTopKAscending() ? new MinTopK(input, new BigIntLiteral(topK))
                : new MaxTopK(input, new BigIntLiteral(topK));
    }

    /** The side's MIN or MAX aggregate; with K &lt;= 1 it provides the retained value. */
    protected abstract AggregateFunction buildExtremeAggregate(Expression input);

    /** The side's COUNT_AT_MIN or COUNT_AT_MAX aggregate; with K &lt;= 1 it provides the row count. */
    protected abstract AggregateFunction buildExtremeCountAggregate(Expression input);

    /** Whether this side's top-K state list is sorted ascending (MIN) or descending (MAX). */
    protected abstract boolean isTopKAscending();

    /** Returns the minimum (MIN) or maximum (MAX) of the given values. */
    protected abstract Expression leastOrGreatest(List<Expression> values);

    /**
     * Builds the merged list out of generic array functions: negate the delete side's counts so all
     * three lists are added, merge them with array_sorted_merge (two chained calls, because the
     * function merges two sorted arrays at a time) and keep the positive counts. The result is the
     * pre-truncation candidate list; the caller slices it to K and uses its length as the bound
     * position.
     *
     * <p>The merge lambda combines two elements with the same value by adding their counts and
     * returns NULL when the values differ, which keeps both entries. Each list holds distinct
     * values, so a key appears at most twice across two lists and one fold is enough.
     */
    private Expression mergeValues(Expression oldValues, Expression inserted, Expression deleted,
            Expression effectiveBound, ArrayType valuesType) {
        DataType pairType = valuesType.getItemType();
        // Only the trusted range (v <= bE for MIN, v >= bE for MAX) is known exactly by every
        // side; outside it a truncated side may be missing occurrences, so the counts would be
        // lower bounds. Drop those entries before adding the three lists.
        Expression oldTrusted = withinBound(oldValues, effectiveBound);
        Expression insertTrusted = withinBound(inserted, effectiveBound);
        Expression deleteTrusted = withinBound(deleted, effectiveBound);
        // S + I + (-D): make the delete side additive so both merge steps add counts.
        Expression negatedDeletes = new ArrayMap(negatedCountsLambda(deleteTrusted, pairType));
        Expression merged = new ArraySortedMerge(
                mergeLambda(oldTrusted, insertTrusted, pairType), BooleanLiteral.of(isTopKAscending()));
        merged = new ArraySortedMerge(
                mergeLambda(merged, negatedDeletes, pairType), BooleanLiteral.of(isTopKAscending()));
        return new ArrayFilter(positiveCountLambda(merged));
    }

    /**
     * {@code K = 0} merge: the state holds at most one {@code (extreme, count)} pair, so the new
     * pair is the better of the pre-image and inserted extremes. The counts are summed only when
     * the extremes agree; when one side is better, no row of that value exists on the other side
     * (its own extreme is a different value), so that side's count is already complete.
     */
    private Expression mergeExtreme(Expression oldValues, Expression inserted, Expression newCount,
            ArrayType valuesType) {
        Expression oldPair = elementAt(oldValues, 1L);
        Expression insertedPair = elementAt(inserted, 1L);
        Expression oldPairNull = new IsNull(oldPair);
        Expression insertedPairNull = new IsNull(insertedPair);
        Expression oldValue = nonNullField(elementAt(oldPair, "value"));
        Expression insertedValue = nonNullField(elementAt(insertedPair, "value"));
        Expression oldCount = nonNullField(elementAt(oldPair, "count"));
        Expression insertedCount = nonNullField(elementAt(insertedPair, "count"));
        Expression sameValue = new EqualTo(oldValue, insertedValue);

        Expression value = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(oldPairNull, insertedValue),
                        new WhenClause(insertedPairNull, oldValue),
                        new WhenClause(sameValue, oldValue)),
                leastOrGreatest(ImmutableList.of(oldValue, insertedValue)));
        // The count follows the better extreme: equal extremes add the two counts; otherwise the
        // better side's count is already complete, because no row of that value exists on the
        // other side (its own extreme differs).
        Expression count = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(oldPairNull, insertedCount),
                        new WhenClause(insertedPairNull, oldCount),
                        new WhenClause(sameValue, new Add(oldCount, insertedCount)),
                        new WhenClause(new EqualTo(value, oldValue), oldCount)),
                insertedCount);
        // The state's pair fields are nullable in the type (a lambda reading them through
        // element_at yields nullable fields), while the coalesced values above are not.
        Expression pair = TypeCoercionUtils.castIfNotMatchType(
                new CreateNamedStruct(
                        new StringLiteral("value"), nonNullField(value),
                        new StringLiteral("count"), nonNullField(count)),
                valuesType.getItemType());
        return new If(new GreaterThan(newCount, new BigIntLiteral(0L)), new Array(pair),
                new ArrayLiteral(ImmutableList.of(), valuesType));
    }

    /**
     * Keeps the trusted part of one sorted state list: values at or below the merged bound for
     * MIN, at or above it for MAX. A NULL merged bound means all three sides are complete, so
     * every entry is trusted.
     */
    private Expression withinBound(Expression array, Expression bound) {
        ArrayItemReference ref = new ArrayItemReference("entry", array);
        Slot entry = ref.toSlot();
        Expression inRange = isTopKAscending()
                ? new LessThanEqual(elementAt(entry, "value"), bound)
                : new GreaterThanEqual(elementAt(entry, "value"), bound);
        Lambda filter = new Lambda(ImmutableList.of(ref.getName()),
                new Or(new IsNull(bound), inRange), ImmutableList.of(ref));
        return new ArrayFilter(filter);
    }

    /**
     * Implements the bound rule on the pre-truncation candidates {@code positive}:
     * <ul>
     *   <li>{@code n' == 0}: the group disappeared, so the state is complete and the bound is
     *       NULL;</li>
     *   <li>all three sides complete and at most k candidates: still complete, bound NULL;</li>
     *   <li>more than k candidates: the k-th candidate is the new truncation point;</li>
     *   <li>otherwise the merged trusted bound {@code bE} carries over.</li>
     * </ul>
     */
    private Expression buildBound(Expression oldBound, Expression insertBound, Expression deleteBound,
            Expression effectiveBound, Expression positive, Expression oldCount, Expression deltaCount,
            long topK, DataType valueType) {
        Expression allComplete = new And(new IsNull(oldBound),
                new And(new IsNull(insertBound), new IsNull(deleteBound)));
        Expression newCount = new Add(oldCount, deltaCount);
        BigIntLiteral k = new BigIntLiteral(topK);
        Expression position = new Cardinality(positive);
        BigIntLiteral zero = new BigIntLiteral(0L);
        return new CaseWhen(
                ImmutableList.of(
                        new WhenClause(new EqualTo(newCount, zero),
                                new NullLiteral(valueType)),
                        new WhenClause(new And(allComplete, new LessThanEqual(position, k)),
                                new NullLiteral(valueType)),
                        new WhenClause(new GreaterThan(position, k),
                                elementAt(elementAt(positive, topK), "value"))),
                TypeCoercionUtils.castIfNotMatchType(effectiveBound, valueType));
    }

    /** The merged trusted bound: the best non-NULL side bound, or NULL when all sides are complete. */
    private Expression nonNullExtremum(Expression first, Expression second, Expression third) {
        return new If(new IsNull(first),
                new If(new IsNull(second), third,
                        new If(new IsNull(third), second,
                                leastOrGreatest(ImmutableList.of(second, third)))),
                new If(new IsNull(second),
                        new If(new IsNull(third), first,
                                leastOrGreatest(ImmutableList.of(first, third))),
                        new If(new IsNull(third),
                                leastOrGreatest(ImmutableList.of(first, second)),
                                leastOrGreatest(ImmutableList.of(first, second, third)))));
    }

    /**
     * {@code (a, b) -> IF(a.value = b.value, {value, a.count + b.count}, NULL)}: fold two
     * entries with the same value; NULL means the values differ and both must be kept.
     */
    private static Lambda mergeLambda(Expression leftArray, Expression rightArray, DataType pairType) {
        ArrayItemReference leftRef = new ArrayItemReference("merge_left", leftArray);
        ArrayItemReference rightRef = new ArrayItemReference("merge_right", rightArray);
        Slot left = leftRef.toSlot();
        Slot right = rightRef.toSlot();
        Expression sameValue = new EqualTo(elementAt(left, "value"), elementAt(right, "value"));
        // The state's pair fields are nullable in the type (a lambda reading them through
        // element_at yields nullable fields), while the coalesced values above are not.
        Expression combined = TypeCoercionUtils.castIfNotMatchType(
                new CreateNamedStruct(
                        new StringLiteral("value"), nonNullField(elementAt(left, "value")),
                        new StringLiteral("count"),
                        new Add(nonNullField(elementAt(left, "count")),
                                nonNullField(elementAt(right, "count")))),
                pairType);
        Expression different = TypeCoercionUtils.castIfNotMatchType(
                new NullLiteral(pairType), pairType);
        return new Lambda(ImmutableList.of(leftRef.getName(), rightRef.getName()),
                new If(sameValue, combined, different),
                ImmutableList.of(leftRef, rightRef));
    }

    /** {@code x -> {value, -count}}: turns the delete side into an additive list. */
    private static Lambda negatedCountsLambda(Expression array, DataType pairType) {
        ArrayItemReference ref = new ArrayItemReference("delete_entry", array);
        Slot entry = ref.toSlot();
        Expression negated = new CreateNamedStruct(
                new StringLiteral("value"), nonNullField(elementAt(entry, "value")),
                new StringLiteral("count"),
                new Multiply(nonNullField(elementAt(entry, "count")), new BigIntLiteral(-1L)));
        return new Lambda(ImmutableList.of(ref.getName()),
                TypeCoercionUtils.castIfNotMatchType(negated, pairType),
                ImmutableList.of(ref));
    }

    /**
     * Makes a struct field non-nullable. The state's value/count fields are required, while
     * {@code element_at} is always nullable in the type system; the coalesced default is never
     * used by a valid state, whose fields cannot be NULL.
     */
    private static Expression nonNullField(Expression field) {
        Expression zero = field.getDataType().zeroValue().orElseThrow(() ->
                new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "no zero value for top-K state field type " + field.getDataType().toSql()));
        return new Coalesce(field, zero);
    }

    /** {@code x -> x.count > 0}: drops entries whose cancellation removed every occurrence. */
    private static Lambda positiveCountLambda(Expression array) {
        ArrayItemReference ref = new ArrayItemReference("candidate", array);
        Slot entry = ref.toSlot();
        return new Lambda(ImmutableList.of(ref.getName()),
                new GreaterThan(elementAt(entry, "count"), new BigIntLiteral(0L)),
                ImmutableList.of(ref));
    }

    /** Assertion message used when the delta is not sufficient and complete refresh is required. */
    protected abstract String fallbackMessage();

    private IvmAggDeltaSlotRef deleteSlotRef(IvmAggTarget target) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), deleteSlotName);
    }

    private String deleteDeltaColumnName(IvmAggTarget target, IvmAggExpressionBuilder ctx) {
        return ctx.transientDeltaColumnName(target, deleteSlotName);
    }

    /**
     * Wraps NULL array inputs (a delta side with no rows for this group, or a new group's missing
     * pre-image) into a typed empty array, so the merge never propagates NULL.
     */
    private static Expression nonNullArray(Expression array, DataType arrayType) {
        return new Coalesce(array, new ArrayLiteral(ImmutableList.of(), arrayType));
    }
}
