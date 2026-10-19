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
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Shared delta/apply logic for MIN/MAX.
 *
 * <p>Besides the current extreme value, the MV stores the number of rows equal to that
 * extreme ({@code MIN_COUNT}/{@code MAX_COUNT}). The delta aggregate produces the extreme
 * on the insert and delete sides together with the count of rows equal to each side's
 * extreme. Apply uses those counts to decide whether the old extreme survived:
 *
 * <ul>
 *   <li>An inserted value better than the old extreme becomes the new extreme.</li>
 *   <li>Otherwise the old extreme survives while
 *       {@code oldCount + equal inserts - equal deletes > 0}.</li>
 *   <li>If the old extreme is exhausted and no inserted value is better, the new extreme
 *       cannot be derived from the current state and delta; the assert guard keeps the
 *       existing fallback to a complete refresh.</li>
 * </ul>
 *
 * <p>The count state is what makes deleting a duplicate extreme incremental: deleting one
 * of several rows equal to the minimum no longer trips the fallback.
 */
abstract class IvmAggExtremalProcessor extends IvmAggFunctionProcessor {
    private static final String INSERT_COUNT_SLOT = "EXTREME_INS_COUNT";
    private static final String DELETE_COUNT_SLOT = "EXTREME_DEL_COUNT";

    private final IvmAggFunctionKind functionKind;
    private final IvmAggStateKey countStateKey;
    private final String deleteSlotName;

    IvmAggExtremalProcessor(IvmAggFunctionKind functionKind, IvmAggStateKey countStateKey,
            String deleteSlotName) {
        this.functionKind = functionKind;
        this.countStateKey = countStateKey;
        this.deleteSlotName = deleteSlotName;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return functionKind;
    }

    @Override
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function) {
        return ImmutableList.of(IvmAggStateKey.COUNT, countStateKey);
    }

    @Override
    public void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot,
            List<NamedExpression> outputs, IvmAggExpressionBuilder ctx) {
        Expression arg = target.getExprArgs().get(0);
        outputs.add(new Alias(buildExtremeAggregate(ctx.insertOnlyValue(arg, dmlFactorSlot)),
                ctx.deltaColumnName(target, functionKind)));
        outputs.add(new Alias(buildExtremeAggregate(ctx.deleteOnlyValue(arg, dmlFactorSlot)),
                deleteDeltaColumnName(target, ctx)));
        outputs.add(new Alias(new Sum(ctx.nonNullDeltaCountValue(arg, dmlFactorSlot)),
                ctx.deltaColumnName(target, IvmAggFunctionKind.COUNT)));
        outputs.add(new Alias(buildExtremeCountAggregate(ctx.insertOnlyValue(arg, dmlFactorSlot)),
                ctx.transientDeltaColumnName(target, INSERT_COUNT_SLOT)));
        outputs.add(new Alias(buildExtremeCountAggregate(ctx.deleteOnlyValue(arg, dmlFactorSlot)),
                ctx.transientDeltaColumnName(target, DELETE_COUNT_SLOT)));
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
        resolveDeltaSlot(target, INSERT_COUNT_SLOT, outputByName, applyDeltaSlots, ctx);
        resolveDeltaSlot(target, DELETE_COUNT_SLOT, outputByName, applyDeltaSlots, ctx);
    }

    @Override
    protected List<IvmAggFunctionKind> zeroDefaultSlotKinds(IvmAggTarget target, boolean scalarAgg) {
        return scalarAgg ? ImmutableList.of(IvmAggFunctionKind.COUNT) : ImmutableList.of();
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        Slot oldExtreme = applyContext.rawMvSlot(target.getValueStateColumnName());
        Expression deltaInsert = applyContext.deltaSlotValue(target, functionKind);
        Expression deltaDelete = applyContext.deltaSlotValue(target, deleteSlotRef(target));
        Expression deltaInsertCount = applyContext.deltaSlotValue(target, insertCountSlotRef(target));
        Expression deltaDeleteCount = applyContext.deltaSlotValue(target, deleteCountSlotRef(target));

        Slot oldCountSlot = Objects.requireNonNull(target.getHiddenStateSlot(countStateKey),
                "missing extreme count state for " + target);
        Expression oldCount = ctx.zeroIfNull(applyContext.rawMvSlot(oldCountSlot.getName()));
        Expression newTotalCount = applyContext.buildNewHiddenCount(target);
        Expression zero = new BigIntLiteral(0L);

        // Keep every predicate non-NULL: an empty insert/delete side must read as false, not
        // as unknown, so the guard below is a plain boolean.
        Expression insertedPresent = new Not(new IsNull(deltaInsert));
        Expression deletedPresent = new Not(new IsNull(deltaDelete));
        Expression insertedBetter = new And(insertedPresent, insertedIsBetter(deltaInsert, oldExtreme));
        Expression insertedEqual = new And(insertedPresent, new EqualTo(deltaInsert, oldExtreme));
        Expression deletedEqual = new And(deletedPresent, new EqualTo(deltaDelete, oldExtreme));
        Expression survivingOldCount = new Subtract(
                new Add(oldCount, new If(insertedEqual, deltaInsertCount, zero)),
                new If(deletedEqual, deltaDeleteCount, zero));

        // The old extreme survives when no delete is below it and its remaining count is
        // positive. An inserted value that beats the old extreme also makes the merge
        // derivable without a complete refresh.
        Expression guardCond = new Or(ImmutableList.of(
                new EqualTo(newTotalCount, zero),
                new IsNull(oldExtreme),
                new IsNull(deltaDelete),
                new And(
                        new And(deletedPresent,
                                deletedExtremeIsAtLeastOld(deltaDelete, oldExtreme)),
                        new Or(ImmutableList.of(
                                insertedBetter,
                                new GreaterThan(survivingOldCount, zero))))));
        Expression guard = new AssertTrue(guardCond, new StringLiteral(fallbackMessage()));

        Expression newExtremeRaw = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(new EqualTo(newTotalCount, zero),
                                new NullLiteral(oldExtreme.getDataType())),
                        new WhenClause(new IsNull(oldExtreme), deltaInsert),
                        new WhenClause(insertedBetter, deltaInsert)),
                oldExtreme);
        Expression newExtremeGuarded = new If(guard, newExtremeRaw,
                new NullLiteral(newExtremeRaw.getDataType()));

        Expression newCountRaw = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(new EqualTo(newTotalCount, zero), zero),
                        new WhenClause(new IsNull(oldExtreme), deltaInsertCount),
                        new WhenClause(insertedBetter, deltaInsertCount)),
                survivingOldCount);
        Expression newCountGuarded = new If(guard, newCountRaw,
                new NullLiteral(BigIntType.INSTANCE));

        applyContext.putFinalExpression(target, oldCountSlot.getName(), newCountGuarded);
        applyContext.putFinalExpression(target, target.getHiddenStateSlot(IvmAggStateKey.COUNT).getName(),
                newTotalCount);
        applyContext.putFinalExpression(target, target.getVisibleSlot().getName(),
                new If(ctx.isPositive(newTotalCount),
                        TypeCoercionUtils.castIfNotMatchType(newExtremeGuarded,
                                target.getVisibleSlot().getDataType()),
                        new NullLiteral(target.getVisibleSlot().getDataType())));
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

    /** Builds MIN or MAX over a filtered insert/delete expression. */
    protected abstract Expression buildExtremeAggregate(Expression input);

    /** Builds COUNT_AT_MIN or COUNT_AT_MAX over a filtered insert/delete expression. */
    protected abstract Expression buildExtremeCountAggregate(Expression input);

    /** True when an inserted value becomes the new extreme. */
    protected abstract Expression insertedIsBetter(Expression deltaInsert, Slot oldExtreme);

    /** True when the deleted extreme does not fall below/above the old extreme. */
    protected abstract Expression deletedExtremeIsAtLeastOld(Expression deltaDelete, Slot oldExtreme);

    /** Assertion message used when the delta is not sufficient and complete refresh is required. */
    protected abstract String fallbackMessage();

    private IvmAggDeltaSlotRef deleteSlotRef(IvmAggTarget target) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), deleteSlotName);
    }

    private IvmAggDeltaSlotRef insertCountSlotRef(IvmAggTarget target) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), INSERT_COUNT_SLOT);
    }

    private IvmAggDeltaSlotRef deleteCountSlotRef(IvmAggTarget target) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), DELETE_COUNT_SLOT);
    }

    private String deleteDeltaColumnName(IvmAggTarget target, IvmAggExpressionBuilder ctx) {
        return ctx.transientDeltaColumnName(target, deleteSlotName);
    }
}
