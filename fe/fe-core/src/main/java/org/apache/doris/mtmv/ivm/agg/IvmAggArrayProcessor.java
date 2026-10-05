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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayExceptAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;

import java.util.Map;

/**
 * Shared delta/apply logic for array-valued aggregate functions (ARRAY_AGG / COLLECT_LIST).
 *
 * <p>The stored visible array is the full aggregate state, so no hidden state column is needed: empty
 * groups and fully-deleted groups are handled by the central group-count machinery. Each subclass
 * decides how the delta aggregate emits its polarity columns, one over insert rows and one over delete
 * rows: COLLECT_LIST emits two conditional aggregates (NULL-filtering is safe because it skips NULL
 * rows), while ARRAY_AGG keeps NULL elements and therefore packs every change row into a single
 * {@code struct(dml_factor, elem)} array aggregate, deriving the two polarity columns above the delta
 * aggregate with scalar array functions. Apply merges them into the new visible array as a multiset:
 *
 * <pre>new = except_all(concat(coalesce(old, []), coalesce(ins, [])), coalesce(del, []))</pre>
 *
 * <p>The merge is a multiset identity: element order is irrelevant and duplicate elements cancel by
 * occurrence, which keeps updates and repeated values correct. NULL handling is delegated to each
 * underlying aggregate (ARRAY_AGG keeps NULL elements; COLLECT_LIST skips NULL rows).
 */
abstract class IvmAggArrayProcessor extends IvmAggFunctionProcessor {
    private static final String INS_SLOT = "ARRAY_INS";
    private static final String DEL_SLOT = "ARRAY_DEL";

    /** Transient output-name suffix of the insert/delete polarity column for one side. */
    String polaritySlotName(boolean insertSide) {
        return insertSide ? INS_SLOT : DEL_SLOT;
    }

    @Override
    void mapApplyDeltaSlots(IvmAggTarget target, Map<String, Slot> outputByName,
            Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, Slot deltaGroupCountSlot,
            IvmAggExpressionBuilder ctx) {
        super.mapApplyDeltaSlots(target, outputByName, applyDeltaSlots, deltaGroupCountSlot, ctx);
        resolveDeltaSlot(target, INS_SLOT, outputByName, applyDeltaSlots, ctx);
        resolveDeltaSlot(target, DEL_SLOT, outputByName, applyDeltaSlots, ctx);
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        Slot oldArray = applyContext.rawMvSlot(target.getVisibleSlot().getName());
        Expression empty = ctx.emptyArrayLiteral(target.getVisibleSlot().getDataType());
        // The old MV side is genuinely NULL for groups that appear only in the delta (new groups).
        // The ins/del transient columns can never be NULL -- every delta group holds at least one
        // change row, and array_agg/collect_list yield an empty array (not NULL) for zero kept
        // elements -- so their COALESCE is defensive only.
        Expression insArray = applyContext.deltaSlotValue(target, deltaSlotRef(target, INS_SLOT));
        Expression delArray = applyContext.deltaSlotValue(target, deltaSlotRef(target, DEL_SLOT));
        applyContext.putFinalExpression(target, target.getVisibleSlot().getName(),
                new ArrayExceptAll(
                        new ArrayConcat(new Coalesce(oldArray, empty), new Coalesce(insArray, empty)),
                        new Coalesce(delArray, empty)));
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
        applyDeltaSlots.put(deltaSlotRef(target, slotName), slot);
    }

    private IvmAggDeltaSlotRef deltaSlotRef(IvmAggTarget target, String slotName) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), slotName);
    }
}
