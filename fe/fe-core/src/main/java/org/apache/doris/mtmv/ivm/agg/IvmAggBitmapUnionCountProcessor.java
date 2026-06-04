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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapCount;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Processor for BITMAP_UNION_COUNT(expr). */
class IvmAggBitmapUnionCountProcessor extends IvmAggBitmapProcessor {
    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof BitmapUnionCount;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return IvmAggFunctionKind.BITMAP_UNION_COUNT;
    }

    @Override
    protected List<IvmAggStateKey> hiddenStateKeys(AggregateFunction function) {
        return ImmutableList.of(IvmAggStateKey.BITMAP_UNION);
    }

    @Override
    public void appendApplyExpressions(IvmAggTarget target, IvmAggApplyContext applyContext) {
        IvmAggExpressionBuilder ctx = applyContext.expressions();
        // Keep the bitmap union as hidden MV state; the visible BITMAP_UNION_COUNT value is bitmap_count(state).
        Slot oldBitmap = applyContext.rawMvSlot(target.getHiddenStateSlot(IvmAggStateKey.BITMAP_UNION).getName());
        Expression newBitmap = buildGuardedNewBitmap(target, applyContext, oldBitmap);
        applyContext.putFinalExpression(target.getHiddenStateSlot(IvmAggStateKey.BITMAP_UNION).getName(), newBitmap);
        applyContext.putFinalExpression(target.getVisibleSlot().getName(),
                ctx.castIfNeeded(new BitmapCount(newBitmap), target.getVisibleSlot().getDataType()));
    }
}
