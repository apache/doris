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

import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
     * aliases.
     */
    IvmAggTargetSpec buildTargetSpec(int ordinal, AggregateFunction function, Alias visibleAlias) {
        ImmutableMap.Builder<IvmAggStateKey, Alias> hiddenAliases = ImmutableMap.builder();
        for (IvmAggStateKey stateKey : hiddenStateKeys(function)) {
            hiddenAliases.put(stateKey, new Alias(
                    hiddenStateAggregate(stateKey, function), IvmUtil.ivmAggHiddenColumnName(ordinal,
                            stateKey.name())));
        }
        return new IvmAggTargetSpec(ordinal, handledFunctionKind(), visibleAlias,
                hiddenAliases.build(), targetArguments(function));
    }

    /** Appends the processor-owned delta aggregate outputs for this target. */
    abstract void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot, List<NamedExpression> outputs,
            IvmAggExpressionBuilder ctx);

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
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function) {
        return ImmutableList.of();
    }

    /** Original aggregate arguments stored in {@link IvmAggTarget}; COUNT(*) stores an empty list. */
    protected List<Expression> targetArguments(AggregateFunction function) {
        return ImmutableList.of(function.child(0));
    }

    /** Aggregate function used to materialize one hidden state during normalize. */
    protected AggregateFunction hiddenStateAggregate(IvmAggStateKey stateKey, AggregateFunction function) {
        switch (stateKey) {
            case COUNT:
                return new Count(function.child(0));
            case SUM:
                return new Sum(function.child(0));
            case BITMAP_UNION:
                return new BitmapUnion(function.child(0));
            default:
                throw new IllegalArgumentException("Unsupported IVM aggregate state: " + stateKey);
        }
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
            throw new AnalysisException("IVM agg delta rewrite failed to resolve delta output slot: "
                    + columnName + " for target " + target);
        }
        return slot;
    }

    /** Delta slots where NULL has the same merge meaning as numeric zero. */
    protected List<IvmAggFunctionKind> zeroDefaultSlotKinds(IvmAggTarget target, boolean scalarAgg) {
        return ImmutableList.of();
    }
}
