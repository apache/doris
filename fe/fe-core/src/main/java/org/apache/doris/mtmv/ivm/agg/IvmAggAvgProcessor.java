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
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Truncate;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Processor for AVG(expr), using hidden SUM and hidden COUNT as persistent state. */
class IvmAggAvgProcessor extends IvmAggSumLikeProcessor {
    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof Avg;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return IvmAggFunctionKind.AVG;
    }

    @Override
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function) {
        return ImmutableList.of(IvmAggStateKey.SUM, IvmAggStateKey.COUNT);
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        Expression newSum = new Add(
                applyContext.oldMvSlotZeroIfNull(target.getHiddenStateSlot(IvmAggStateKey.SUM).getName()),
                applyContext.deltaSlotValue(target, IvmAggFunctionKind.SUM));
        Expression newCount = applyContext.buildNewHiddenCount(target);
        applyContext.putFinalExpression(target.getHiddenStateSlot(IvmAggStateKey.SUM).getName(), newSum);
        applyContext.putFinalExpression(target.getHiddenStateSlot(IvmAggStateKey.COUNT).getName(), newCount);
        Divide coercedDivide = (Divide) TypeCoercionUtils.processDivide(new Divide(newSum, newCount));
        Expression guardedCount = new If(ctx.isPositive(newCount), coercedDivide.right(),
                new NullLiteral(coercedDivide.right().getDataType()));
        Expression average = coercedDivide.withChildren(coercedDivide.left(), guardedCount);
        if (target.getVisibleSlot().getDataType() instanceof DecimalV3Type) {
            DecimalV3Type visibleType = (DecimalV3Type) target.getVisibleSlot().getDataType();
            average = TypeCoercionUtils.processBoundFunction(
                    new Truncate(average, new IntegerLiteral(visibleType.getScale())));
        }
        Expression visibleValue = TypeCoercionUtils.castIfNotMatchType(average,
                target.getVisibleSlot().getDataType());
        applyContext.putFinalExpression(target.getVisibleSlot().getName(),
                visibleValue);
    }
}
