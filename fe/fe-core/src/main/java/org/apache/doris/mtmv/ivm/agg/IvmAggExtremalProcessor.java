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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Shared logic for MIN/MAX.
 *
 * <p>The delta aggregate produces an insert-only extreme, a delete-only extreme, and a non-NULL count delta. Apply can
 * merge inserts directly, but if a deleted row may have been the old visible extreme, the assert guard forces fallback
 * to complete refresh because the new extreme cannot be derived from current MV state plus delta alone.
 */
abstract class IvmAggExtremalProcessor extends IvmAggFunctionProcessor {
    private final IvmAggFunctionKind functionKind;
    private final String deleteSlotName;

    IvmAggExtremalProcessor(IvmAggFunctionKind functionKind, String deleteSlotName) {
        this.functionKind = functionKind;
        this.deleteSlotName = deleteSlotName;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return functionKind;
    }

    @Override
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function) {
        return ImmutableList.of(IvmAggStateKey.COUNT);
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
    }

    @Override
    protected List<IvmAggFunctionKind> applyDeltaSlotKinds(IvmAggTarget target) {
        return ImmutableList.of(functionKind, IvmAggFunctionKind.COUNT);
    }

    @Override
    void mapApplyDeltaSlots(IvmAggTarget target, Map<String, Slot> outputByName,
            Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, Slot deltaGroupCountSlot, IvmAggExpressionBuilder ctx) {
        super.mapApplyDeltaSlots(target, outputByName, applyDeltaSlots, deltaGroupCountSlot, ctx);
        String columnName = deleteDeltaColumnName(target, ctx);
        Slot slot = outputByName.get(columnName);
        if (slot == null) {
            throw new AnalysisException("IVM agg delta rewrite failed to resolve delta output slot: "
                    + columnName + " for target " + target);
        }
        applyDeltaSlots.put(deleteSlotRef(target), slot);
    }

    @Override
    protected List<IvmAggFunctionKind> zeroDefaultSlotKinds(IvmAggTarget target, boolean scalarAgg) {
        return scalarAgg ? ImmutableList.of(IvmAggFunctionKind.COUNT) : ImmutableList.of();
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        Slot oldExtreme = applyContext.rawMvSlot(target.getVisibleSlot().getName());
        Expression deltaInsert = applyContext.deltaSlotValue(target, functionKind);
        Expression deltaDel = applyContext.deltaSlotValue(target, deleteSlotRef(target));
        Expression newCount = applyContext.buildNewHiddenCount(target);

        Expression guardCond = new Or(ImmutableList.of(
                new EqualTo(newCount, new BigIntLiteral(0L)),
                new IsNull(deltaDel),
                new IsNull(oldExtreme),
                deletedExtremeKeepsOldValueValid(deltaDel, oldExtreme, deltaInsert)
        ));
        Expression guard = new AssertTrue(guardCond, new StringLiteral(fallbackMessage()));
        Expression newExtremeRaw = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(new EqualTo(newCount, new BigIntLiteral(0L)),
                                new NullLiteral(oldExtreme.getDataType())),
                        new WhenClause(new IsNull(oldExtreme), deltaInsert),
                        new WhenClause(new IsNull(deltaInsert), oldExtreme)
                ),
                mergeOldAndInsertedExtreme(oldExtreme, deltaInsert)
        );
        Expression newExtremeGuarded = new If(guard, newExtremeRaw,
                new NullLiteral(newExtremeRaw.getDataType()));

        applyContext.putFinalExpression(target.getHiddenStateSlot(IvmAggStateKey.COUNT).getName(), newCount);
        applyContext.putFinalExpression(target.getVisibleSlot().getName(),
                new If(ctx.isPositive(newCount),
                        TypeCoercionUtils.castIfNotMatchType(newExtremeGuarded, target.getVisibleSlot().getDataType()),
                        new NullLiteral(target.getVisibleSlot().getDataType())));
    }

    /** Builds MIN or MAX over a filtered insert/delete expression. */
    protected abstract Expression buildExtremeAggregate(Expression input);

    /**
     * Returns true when the old visible extreme is still deterministically valid after deletes,
     * either because no delete hit the boundary point, or because the insert side provides a
     * value that equals or improves the old extreme position.
     */
    protected abstract Expression deletedExtremeKeepsOldValueValid(Expression deltaDeletedExtreme,
            Slot oldExtreme, Expression deltaInsertExtreme);

    /** Merges the old visible extreme with the insert-only delta extreme. */
    protected abstract Expression mergeOldAndInsertedExtreme(Slot oldExtreme, Expression deltaInsertExtreme);

    /** Assertion message used when the delta is not sufficient and complete refresh is required. */
    protected abstract String fallbackMessage();

    private IvmAggDeltaSlotRef deleteSlotRef(IvmAggTarget target) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), deleteSlotName);
    }

    private String deleteDeltaColumnName(IvmAggTarget target, IvmAggExpressionBuilder ctx) {
        return ctx.transientDeltaColumnName(target, deleteSlotName);
    }
}
