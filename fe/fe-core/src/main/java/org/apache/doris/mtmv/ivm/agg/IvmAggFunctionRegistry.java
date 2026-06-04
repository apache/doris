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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;

import com.google.common.collect.ImmutableList;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Explicit registry for aggregate functions supported by IVM.
 *
 * <p>Each processor owns the function-specific work across the IVM aggregate lifecycle:
 * normalize hidden state, build delta aggregate outputs, map delta outputs for apply, and build apply expressions.
 *
 * <p>Data flow:
 * <ol>
 *   <li>{@code IvmNormalizeMtmv}: {@link #buildTargetSpec} records visible output, hidden state outputs, and
 *       original aggregate arguments into {@link IvmAggTargetSpec}. After normalize resolves final aggregate output
 *       slots, the spec becomes the stable {@link IvmAggTarget} stored in {@link IvmAggMeta}.</li>
 *   <li>{@code IvmAggDeltaHandler#buildDeltaSubPlan}: {@link #appendDeltaAggregateOutputs} creates the signed delta
 *       aggregate columns, and {@link #collectZeroDefaultDeltaOutputNames} tells the top delta project which
 *       arithmetic operands need {@code COALESCE(x, 0)}.</li>
 *   <li>{@code IvmAggDeltaHandler#buildApplyPlan}: {@link #mapApplyDeltaSlots} converts delta output slots into
 *       {@link IvmAggDeltaSlotRef} keys, then {@link #appendApplyExpressions} derives the final MV columns.</li>
 * </ol>
 */
public class IvmAggFunctionRegistry {
    public static final IvmAggFunctionRegistry INSTANCE = new IvmAggFunctionRegistry();

    private final List<IvmAggFunctionProcessor> processors;
    private final Map<IvmAggFunctionKind, IvmAggFunctionProcessor> processorByKind;

    private IvmAggFunctionRegistry() {
        processors = ImmutableList.of(
                new IvmAggCountProcessor(),
                new IvmAggSumProcessor(),
                new IvmAggAvgProcessor(),
                new IvmAggMinProcessor(),
                new IvmAggMaxProcessor(),
                new IvmAggBitmapUnionProcessor(),
                new IvmAggBitmapUnionCountProcessor());
        processorByKind = new EnumMap<>(IvmAggFunctionKind.class);
        for (IvmAggFunctionProcessor processor : processors) {
            processorByKind.put(processor.handledFunctionKind(), processor);
        }
    }

    /**
     * Builds target metadata for one original aggregate function and its optional hidden state columns.
     */
    public IvmAggTargetSpec buildTargetSpec(int ordinal, AggregateFunction function, Alias visibleAlias) {
        return findProcessor(function).buildTargetSpec(ordinal, function, visibleAlias);
    }

    /**
     * Appends the aggregate expressions produced by the delta aggregate for one visible MV aggregate column.
     */
    public void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot, List<NamedExpression> outputs,
            IvmAggExpressionBuilder ctx) {
        processorFor(target).appendDeltaAggregateOutputs(target, dmlFactorSlot, outputs, ctx);
    }

    /**
     * Maps the delta plan output slots to stable logical keys consumed by the apply expressions.
     */
    public void mapApplyDeltaSlots(IvmAggTarget target, Map<String, Slot> outputByName,
            Map<IvmAggDeltaSlotRef, Slot> applyDeltaSlots, Slot deltaGroupCountSlot, IvmAggExpressionBuilder ctx) {
        processorFor(target).mapApplyDeltaSlots(target, outputByName, applyDeltaSlots, deltaGroupCountSlot, ctx);
    }

    /**
     * Adds delta output names where NULL should be normalized to zero before apply consumes them.
     */
    public void collectZeroDefaultDeltaOutputNames(IvmAggTarget target, boolean scalarAgg,
            Set<String> outputNames, IvmAggExpressionBuilder ctx) {
        processorFor(target).collectZeroDefaultDeltaOutputNames(target, scalarAgg, outputNames, ctx);
    }

    /**
     * Appends this aggregate target's final visible and hidden-state expressions to the apply projection.
     */
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext ctx) {
        processorFor(target).appendApplyExpressions(target, ctx);
    }

    private IvmAggFunctionProcessor processorFor(IvmAggTarget target) {
        IvmAggFunctionProcessor processor = processorByKind.get(target.getFunctionKind());
        if (processor == null) {
            throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                    "Unsupported aggregate function for IVM: " + target.getFunctionKind());
        }
        return processor;
    }

    private IvmAggFunctionProcessor findProcessor(AggregateFunction function) {
        if (function.isDistinct()) {
            throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                    "Aggregate DISTINCT is not supported for IVM: " + function.toSql());
        }
        for (IvmAggFunctionProcessor processor : processors) {
            if (processor.supportsOriginalFunction(function)) {
                return processor;
            }
        }
        throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                "Unsupported aggregate function for IVM: " + function.getName());
    }
}
