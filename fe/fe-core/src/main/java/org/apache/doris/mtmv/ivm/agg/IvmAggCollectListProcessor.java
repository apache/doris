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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;

import java.util.List;

/**
 * Processor for COLLECT_LIST(expr) with exactly one argument.
 *
 * <p>COLLECT_LIST skips NULL rows, so the polarity columns use the plain conditional-argument idiom:
 * rows of the other polarity become NULL and are skipped by the aggregate itself.
 *
 * <p>The two-argument LIMIT variant is not incrementally maintainable and is intentionally not
 * supported (falls back to complete refresh).
 */
class IvmAggCollectListProcessor extends IvmAggArrayProcessor {
    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof CollectList && function.children().size() == 1;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return IvmAggFunctionKind.COLLECT_LIST;
    }

    @Override
    void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot,
            List<NamedExpression> outputs, IvmAggExpressionBuilder ctx) {
        Expression arg = target.getExprArgs().get(0);
        outputs.add(new Alias(buildPolarityAggregate(true, arg, dmlFactorSlot, ctx),
                ctx.transientDeltaColumnName(target, polaritySlotName(true))));
        outputs.add(new Alias(buildPolarityAggregate(false, arg, dmlFactorSlot, ctx),
                ctx.transientDeltaColumnName(target, polaritySlotName(false))));
    }

    private AggregateFunction buildPolarityAggregate(boolean insertSide, Expression elem, Slot dmlFactorSlot,
            IvmAggExpressionBuilder ctx) {
        Expression filtered = insertSide
                ? ctx.insertOnlyValue(elem, dmlFactorSlot)
                : ctx.deleteOnlyValue(elem, dmlFactorSlot);
        return new CollectList(filtered);
    }
}
