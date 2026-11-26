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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Shared delta logic for SUM and AVG.
 *
 * <p>Both functions need a signed sum delta and a non-NULL count delta. SUM stores the count as hidden state so it can
 * return NULL for empty groups; AVG stores both hidden sum and hidden count.
 */
abstract class IvmAggSumLikeProcessor extends IvmAggFunctionProcessor {
    @Override
    public void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot,
            List<NamedExpression> outputs, IvmAggExpressionBuilder ctx) {
        Expression arg = target.getExprArgs().get(0);
        outputs.add(new Alias(new Sum(ctx.signedDeltaValue(arg, dmlFactorSlot)),
                ctx.deltaColumnName(target, IvmAggFunctionKind.SUM)));
        outputs.add(new Alias(new Sum(ctx.nonNullDeltaCountValue(arg, dmlFactorSlot)),
                ctx.deltaColumnName(target, IvmAggFunctionKind.COUNT)));
    }

    @Override
    protected List<IvmAggFunctionKind> applyDeltaSlotKinds(IvmAggTarget target) {
        return ImmutableList.of(IvmAggFunctionKind.SUM, IvmAggFunctionKind.COUNT);
    }

    @Override
    protected List<IvmAggFunctionKind> zeroDefaultSlotKinds(IvmAggTarget target, boolean scalarAgg) {
        return scalarAgg
                ? ImmutableList.of(IvmAggFunctionKind.SUM, IvmAggFunctionKind.COUNT)
                : ImmutableList.of(IvmAggFunctionKind.SUM);
    }
}
