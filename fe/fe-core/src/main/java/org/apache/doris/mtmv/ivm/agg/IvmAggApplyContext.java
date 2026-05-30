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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import java.util.Map;

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

    public IvmAggApplyContext(Map<String, Expression> finalByColumnName,
            LogicalOlapScan rawMvScan, Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots,
            Expression newGroupCount, IvmAggExpressionBuilder expressionBuilder) {
        this.finalByColumnName = finalByColumnName;
        this.rawMvScan = rawMvScan;
        this.applyDeltaSlots = applyDeltaSlots;
        this.newGroupCount = newGroupCount;
        this.expressionBuilder = expressionBuilder;
    }

    /** New total group count after applying this refresh delta. */
    Expression newGroupCount() {
        return newGroupCount;
    }

    /** Shared expression builder for processor-specific apply expressions. */
    IvmAggExpressionBuilder expressions() {
        return expressionBuilder;
    }

    /** Adds or replaces one final output expression by normalized MV column name. */
    void putFinalExpression(String columnName, Expression expression) {
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
            throw new AnalysisException("IVM agg delta rewrite failed to resolve delta slot: " + slotRef);
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
