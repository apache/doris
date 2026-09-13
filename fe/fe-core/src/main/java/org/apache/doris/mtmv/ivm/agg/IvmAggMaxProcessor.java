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

import org.apache.doris.mtmv.ivm.IvmFailureClassifier;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Greatest;

/** MAX-specific extreme comparisons and merge expression. */
class IvmAggMaxProcessor extends IvmAggExtremalProcessor {
    IvmAggMaxProcessor() {
        super(IvmAggFunctionKind.MAX, "DELMAX");
    }

    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof Max;
    }

    @Override
    protected Expression buildExtremeAggregate(Expression input) {
        return new Max(input);
    }

    @Override
    protected Expression deletedExtremeKeepsOldValueValid(Expression deltaDeletedExtreme,
            Slot oldExtreme, Expression deltaInsertExtreme) {
        return new Or(
                new LessThan(deltaDeletedExtreme, oldExtreme),
                new GreaterThanEqual(deltaInsertExtreme, oldExtreme));
    }

    @Override
    protected Expression mergeOldAndInsertedExtreme(Slot oldExtreme, Expression deltaInsertExtreme) {
        return new Greatest(oldExtreme, deltaInsertExtreme);
    }

    @Override
    protected String fallbackMessage() {
        return IvmFailureClassifier.MIN_MAX_BOUNDARY_MSG_PREFIX
                + ": deleted row may be current MAX value";
    }
}
