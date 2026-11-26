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
import org.apache.doris.mtmv.ivm.IvmFailureClassifier;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Shared insert-only bitmap state merge and delete fallback logic.
 *
 * <p>Bitmap aggregate state is monotonic under insert: the new state is old bitmap OR insert-delta bitmap. Doris does
 * not maintain per-element reference counts in MV state, so deleting a non-empty bitmap cannot be derived from the old
 * union bitmap. In that case the generated {@link AssertTrue} fails and the refresh task requires a later COMPLETE
 * refresh. Deletes whose aggregate input is NULL or an empty bitmap are harmless and stay incremental. If the target
 * group becomes empty, the MV row can be deleted or reset with an empty bitmap state.
 */
abstract class IvmAggBitmapProcessor extends IvmAggFunctionProcessor {
    private static final String DELETE_SLOT_NAME = "BITMAP_DELETE_UNION";
    private static final String FALLBACK_MESSAGE = IvmFailureClassifier.BITMAP_AGG_DELETE_MSG_PREFIX;

    @Override
    public void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot,
            List<NamedExpression> outputs, IvmAggExpressionBuilder ctx) {
        Expression arg = target.getExprArgs().get(0);
        outputs.add(new Alias(new BitmapUnion(ctx.insertOnlyValue(arg, dmlFactorSlot)),
                ctx.deltaColumnName(target, IvmAggFunctionKind.BITMAP_UNION)));
        outputs.add(new Alias(new BitmapUnion(ctx.deleteOnlyValue(arg, dmlFactorSlot)),
                deleteDeltaColumnName(target, ctx)));
    }

    @Override
    protected List<IvmAggFunctionKind> applyDeltaSlotKinds(IvmAggTarget target) {
        return ImmutableList.of(IvmAggFunctionKind.BITMAP_UNION);
    }

    @Override
    void mapApplyDeltaSlots(IvmAggTarget target, Map<String, Slot> outputByName,
            Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, Slot deltaGroupCountSlot, IvmAggExpressionBuilder ctx) {
        super.mapApplyDeltaSlots(target, outputByName, applyDeltaSlots, deltaGroupCountSlot, ctx);
        String columnName = deleteDeltaColumnName(target, ctx);
        Slot slot = outputByName.get(columnName);
        if (slot == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM agg delta rewrite failed to resolve delta output slot: "
                    + columnName + " for target " + target);
        }
        applyDeltaSlots.put(deleteSlotRef(target), slot);
    }

    Expression buildGuardedNewBitmap(IvmAggTarget target, IvmAggApplyContext applyContext, Slot oldBitmap) {
        Expression deltaBitmap = applyContext.deltaSlotValue(target, IvmAggFunctionKind.BITMAP_UNION);
        Expression deleteBitmap = applyContext.deltaSlotValue(target, deleteSlotRef(target));
        // bitmap_union ignores NULL input. A group deleted to zero rows uses an empty bitmap state.
        Expression guard = new AssertTrue(new Or(ImmutableList.of(
                new EqualTo(applyContext.newGroupCount(), new BigIntLiteral(0L)),
                new IsNull(deleteBitmap),
                new EqualTo(new BitmapCount(deleteBitmap), new BigIntLiteral(0L)))),
                new StringLiteral(FALLBACK_MESSAGE));
        Expression newBitmap = new CaseWhen(
                ImmutableList.of(
                        new WhenClause(new EqualTo(applyContext.newGroupCount(), new BigIntLiteral(0L)),
                                new BitmapEmpty()),
                        new WhenClause(new IsNull(oldBitmap), deltaBitmap),
                        new WhenClause(new IsNull(deltaBitmap), oldBitmap)
                ),
                new BitmapOr(oldBitmap, deltaBitmap));
        return new If(guard, newBitmap, new NullLiteral(newBitmap.getDataType()));
    }

    private IvmAggDeltaSlotRef deleteSlotRef(IvmAggTarget target) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), DELETE_SLOT_NAME);
    }

    private String deleteDeltaColumnName(IvmAggTarget target, IvmAggExpressionBuilder ctx) {
        return ctx.transientDeltaColumnName(target, DELETE_SLOT_NAME);
    }
}
