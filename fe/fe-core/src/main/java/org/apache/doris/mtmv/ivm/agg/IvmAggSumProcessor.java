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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Processor for SUM(expr), using visible SUM plus hidden non-NULL count as persistent state. */
class IvmAggSumProcessor extends IvmAggSumLikeProcessor {
    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof Sum;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return IvmAggFunctionKind.SUM;
    }

    @Override
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function) {
        return ImmutableList.of(IvmAggStateKey.COUNT);
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        Expression newSum = new Add(
                applyContext.oldMvSlotZeroIfNull(target.getVisibleSlot().getName()),
                applyContext.deltaSlotValue(target, IvmAggFunctionKind.SUM));
        Expression newCount = applyContext.buildNewHiddenCount(target);
        applyContext.putFinalExpression(target.getHiddenStateSlot(IvmAggStateKey.COUNT).getName(), newCount);
        Expression visibleValue = TypeCoercionUtils.castIfNotMatchType(newSum, target.getVisibleSlot().getDataType());
        Expression emptyValue = target.getVisibleSlot().nullable()
                ? new NullLiteral(target.getVisibleSlot().getDataType())
                : ctx.zeroOf(target.getVisibleSlot().getDataType());
        applyContext.putFinalExpression(target.getVisibleSlot().getName(),
                new If(ctx.isPositive(newCount), visibleValue, emptyValue));
    }
}
