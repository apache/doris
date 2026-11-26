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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import java.util.Map;
import java.util.Set;

/**
 * Mutable apply-stage context passed to one aggregate processor at a time.
 *
 * <p>It carries the final projection map built by {@code IvmAggDeltaHandler#buildApplyPlan}, the raw MV scan for old
 * state lookup, the resolved delta slots from {@link IvmAggDeltaSlotRef}, and common expression helpers.
 */
public class IvmAggApplyContext {
    private final Map<String, Expression> finalByColumnName;
    private final LogicalOlapScan rawMvScan;
    private final Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots;
    private final Expression newGroupCount;
    private final IvmAggExpressionBuilder expressionBuilder;
    private final Set<String> visibleColumnNames;

    public IvmAggApplyContext(Map<String, Expression> finalByColumnName,
            LogicalOlapScan rawMvScan, Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots,
            Expression newGroupCount, IvmAggExpressionBuilder expressionBuilder,
            Set<String> visibleColumnNames) {
        this.finalByColumnName = finalByColumnName;
        this.rawMvScan = rawMvScan;
        this.applyDeltaSlots = applyDeltaSlots;
        this.newGroupCount = newGroupCount;
        this.expressionBuilder = expressionBuilder;
        this.visibleColumnNames = visibleColumnNames;
    }

    /** New total group count after applying this refresh delta. */
    Expression newGroupCount() {
        return newGroupCount;
    }

    /** Shared expression builder for processor-specific apply expressions. */
    IvmAggExpressionBuilder expressions() {
        return expressionBuilder;
    }

    /**
     * Adds or replaces one final output expression by normalized MV column name.
     *
     * <p>Shared hidden state columns may alias a visible column (for example AVG(x) reuses the visible
     * SUM(x) column as its hidden SUM state). Only the visible column owner may write such a column;
     * other targets skip writing it because the owner produces the final value. Columns without a
     * visible owner (pure hidden state) accept the last writer.
     */
    void putFinalExpression(IvmAggTarget target, String columnName, Expression expression) {
        boolean isVisibleOwner = columnName.equals(target.getVisibleSlot().getName());
        boolean columnHasVisibleOwner = visibleColumnNames.contains(columnName);
        if (columnHasVisibleOwner && !isVisibleOwner) {
            return;
        }
        finalByColumnName.put(columnName, expression);
    }

    /** Returns the resolved delta slot value for one aggregate target and logical delta slot. */
    Expression deltaSlotValue(IvmAggTarget target, IvmAggFunctionKind slotKind) {
        return deltaSlotValue(target, expressionBuilder.deltaSlotRef(target, slotKind));
    }

    /** Returns the resolved delta slot value for one aggregate target and processor-private delta slot. */
    Expression deltaSlotValue(IvmAggTarget target, IvmAggDeltaSlotRef slotRef) {
        Slot slot = applyDeltaSlots.get(slotRef);
        if (slot == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM agg delta rewrite failed to resolve delta slot: " + slotRef);
        }
        return slot;
    }

    /** Returns the delta slot that updates one persistent hidden state key. */
    Expression deltaStateValue(IvmAggTarget target, IvmAggStateKey stateKey) {
        return deltaSlotValue(target, expressionBuilder.stateDeltaSlotKind(stateKey));
    }

    /** Returns an old MV state/value slot converted to zero for arithmetic state merge. */
    Expression oldMvSlotZeroIfNull(String slotName) {
        return expressionBuilder.zeroIfNullMvSlot(rawMvScan, slotName);
    }

    /** Returns an old MV slot without NULL normalization, used when NULL is meaningful. */
    Slot rawMvSlot(String slotName) {
        return expressionBuilder.findSlotByName(rawMvScan, slotName);
    }

    /** Builds the new hidden non-NULL row count for SUM/AVG/MIN/MAX targets. */
    Expression buildNewHiddenCount(IvmAggTarget target) {
        return expressionBuilder.assertNonNegative(new Add(
                oldMvSlotZeroIfNull(target.getHiddenStateSlot(IvmAggStateKey.COUNT).getName()),
                deltaStateValue(target, IvmAggStateKey.COUNT)),
                "negative hidden count for " + target.getVisibleSlot().getName());
    }
}
