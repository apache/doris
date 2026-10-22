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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
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
 * <p>The MV keeps a top-K state per group in {@code MIN_TOPK}/{@code MAX_TOPK}:
 * {@code STRUCT<values ARRAY<STRUCT<value, count>>, bound>}. The delta produces the insert-side and
 * delete-side top-K of the batch, and apply merges them with the pre-image state through
 * {@code topk_merge_min/max}; the merge result is the new state and is written straight back to the
 * hidden column. Only values within {@code min(pre-image bound, both delta bounds)} are trusted;
 * when that range is used up the assert triggers the existing complete-refresh fallback
 * ({@code MIN_MAX_BOUNDARY_HIT}).
 *
 * <p>{@code K = 0} is the extreme-only mode used for append-only plans: the delta carries the side's
 * extreme in {@code bound} and no list. A non-NULL delete-side bound means a non-NULL value was
 * retracted, which breaks the premise the mode relies on, so apply falls back; otherwise the new
 * extreme is {@code min(old, inserted)} (max for MAX).
 */
abstract class IvmAggExtremalProcessor extends IvmAggFunctionProcessor {
    private final IvmAggFunctionKind functionKind;
    private final IvmAggStateKey topKStateKey;
    private final String deleteSlotName;

    IvmAggExtremalProcessor(IvmAggFunctionKind functionKind, IvmAggStateKey topKStateKey,
            String deleteSlotName) {
        this.functionKind = functionKind;
        this.topKStateKey = topKStateKey;
        this.deleteSlotName = deleteSlotName;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return functionKind;
    }

    @Override
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function) {
        return ImmutableList.of(IvmAggStateKey.COUNT, topKStateKey);
    }

    /**
     * The old extreme and the old top-K list both live in the hidden state column; the visible
     * column is an output apply computes, not a state carrier.
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
        outputs.add(new Alias(buildTopKAggregate(ctx.insertOnlyValue(arg, dmlFactorSlot), topK),
                ctx.deltaColumnName(target, functionKind)));
        outputs.add(new Alias(buildTopKAggregate(ctx.deleteOnlyValue(arg, dmlFactorSlot), topK),
                deleteDeltaColumnName(target, ctx)));
        outputs.add(new Alias(new Sum(ctx.nonNullDeltaCountValue(arg, dmlFactorSlot)),
                ctx.deltaColumnName(target, IvmAggFunctionKind.COUNT)));
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
        Slot countSlot = Objects.requireNonNull(target.getHiddenStateSlot(IvmAggStateKey.COUNT),
                "missing count state for " + target);
        long topK = target.getMinMaxTopK();

        Expression oldState = applyContext.rawMvSlot(stateSlot.getName());
        Expression oldCount = ctx.zeroIfNull(applyContext.rawMvSlot(countSlot.getName()));
        Expression deltaCount = applyContext.deltaStateValue(target, IvmAggStateKey.COUNT);
        Expression newCount = applyContext.buildNewHiddenCount(target);
        Expression inserted = applyContext.deltaSlotValue(target, functionKind);
        Expression deleted = applyContext.deltaSlotValue(target, deleteSlotRef(target));

        Expression stateExpr;
        Expression extreme;
        if (topK == 0) {
            Expression oldBound = elementAt(oldState, "bound");
            Expression insertBound = elementAt(inserted, "bound");
            Expression deleteBound = elementAt(deleted, "bound");
            // A non-NULL delete-side bound means a non-NULL value was retracted: the append-only
            // premise the extreme-only mode relies on is broken, so the merge is not trustworthy.
            Expression guard = new AssertTrue(new IsNull(deleteBound),
                    new StringLiteral(fallbackMessage()));
            Expression merged = new CaseWhen(
                    ImmutableList.of(
                            new WhenClause(new IsNull(oldBound), insertBound),
                            new WhenClause(new IsNull(insertBound), oldBound)),
                    betterExtreme(oldBound, insertBound));
            extreme = guarded(guard, merged, insertBound.getDataType());
            // The hidden column stays a typed struct in this mode too: an empty list plus the
            // current extreme in bound. It is not a completeness marker here (see 75 section 4.7).
            StructType stateType = (StructType) stateSlot.getDataType();
            stateExpr = new CreateNamedStruct(
                    new StringLiteral("values"),
                    ctx.emptyArrayLiteral(stateType.getField("values").getDataType()),
                    new StringLiteral("bound"), extreme);
        } else {
            Expression merge = buildMerge(
                    elementAt(oldState, "values"), elementAt(oldState, "bound"),
                    elementAt(inserted, "values"), elementAt(inserted, "bound"),
                    elementAt(deleted, "values"), elementAt(deleted, "bound"),
                    oldCount, deltaCount, topK);
            StructType stateType = (StructType) stateSlot.getDataType();
            Expression values = elementAt(merge, "values");
            // The trusted range is used up when nothing survived while the group still has rows.
            Expression exhausted = new And(
                    new EqualTo(new Cardinality(values), new BigIntLiteral(0L)),
                    ctx.isPositive(newCount));
            Expression guard = new AssertTrue(new Not(exhausted),
                    new StringLiteral(fallbackMessage()));
            stateExpr = guarded(guard, merge, stateType);
            extreme = elementAt(elementAt(stateExpr, "values"), 1L);
            extreme = elementAt(extreme, "value");
        }

        applyContext.putFinalExpression(target, stateSlot.getName(), stateExpr);
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

    /** Builds the side's top-K aggregate: TOPK_MIN_COUNT or TOPK_MAX_COUNT with the MV's capacity. */
    protected abstract AggregateFunction buildTopKAggregate(Expression input, long topK);

    /** Builds TOPK_MERGE_MIN or TOPK_MERGE_MAX over the state and the two delta sides. */
    protected abstract Expression buildMerge(Expression oldValues, Expression oldBound,
            Expression insertValues, Expression insertBound, Expression deleteValues, Expression deleteBound,
            Expression oldCount, Expression deltaCount, long topK);

    /** Returns the better of two non-NULL extremes (LEAST for MIN, GREATEST for MAX). */
    protected abstract Expression betterExtreme(Expression oldExtreme, Expression insertedExtreme);

    /** Assertion message used when the delta is not sufficient and complete refresh is required. */
    protected abstract String fallbackMessage();

    private IvmAggDeltaSlotRef deleteSlotRef(IvmAggTarget target) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), deleteSlotName);
    }

    private String deleteDeltaColumnName(IvmAggTarget target, IvmAggExpressionBuilder ctx) {
        return ctx.transientDeltaColumnName(target, deleteSlotName);
    }
}
