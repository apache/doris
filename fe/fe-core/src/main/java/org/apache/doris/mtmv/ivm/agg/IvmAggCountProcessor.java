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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/** Processor for COUNT(*) and COUNT(expr). */
class IvmAggCountProcessor extends IvmAggFunctionProcessor {
    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof Count;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return IvmAggFunctionKind.COUNT;
    }

    @Override
    protected List<Expression> targetArguments(AggregateFunction function) {
        return ((Count) function).isCountStar()
                ? ImmutableList.of()
                : ImmutableList.of(function.child(0));
    }

    @Override
    public void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot,
            List<NamedExpression> outputs, IvmAggExpressionBuilder ctx) {
        if (!target.isCountStar()) {
            Expression arg = target.getExprArgs().get(0);
            outputs.add(new Alias(new Sum(ctx.nonNullDeltaCountValue(arg, dmlFactorSlot)),
                    ctx.deltaColumnName(target, IvmAggFunctionKind.COUNT)));
        }
    }

    @Override
    protected List<IvmAggFunctionKind> applyDeltaSlotKinds(IvmAggTarget target) {
        return ImmutableList.of(IvmAggFunctionKind.COUNT);
    }

    @Override
    protected Slot resolveDeltaOutputSlot(IvmAggTarget target, IvmAggFunctionKind slotKind,
            Map<String, Slot> outputByName, Slot deltaGroupCountSlot, IvmAggExpressionBuilder ctx) {
        return target.isCountStar() ? deltaGroupCountSlot : super.resolveDeltaOutputSlot(
                target, slotKind, outputByName, deltaGroupCountSlot, ctx);
    }

    @Override
    protected List<IvmAggFunctionKind> zeroDefaultSlotKinds(IvmAggTarget target, boolean scalarAgg) {
        return scalarAgg && !target.isCountStar()
                ? ImmutableList.of(IvmAggFunctionKind.COUNT)
                : ImmutableList.of();
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        if (target.isCountStar()) {
            applyContext.putFinalExpression(target.getVisibleSlot().getName(),
                    TypeCoercionUtils.castIfNotMatchType(
                            applyContext.newGroupCount(), target.getVisibleSlot().getDataType()));
            return;
        }

        Expression newCount = ctx.assertNonNegative(new Add(
                applyContext.oldMvSlotZeroIfNull(target.getVisibleSlot().getName()),
                applyContext.deltaSlotValue(target, IvmAggFunctionKind.COUNT)),
                "negative count for " + target.getVisibleSlot().getName());
        applyContext.putFinalExpression(target.getVisibleSlot().getName(),
                new If(ctx.isPositive(newCount),
                        TypeCoercionUtils.castIfNotMatchType(newCount, target.getVisibleSlot().getDataType()),
                        ctx.zeroOf(target.getVisibleSlot().getDataType())));
    }
}
