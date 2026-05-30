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
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Least;

/** MIN-specific extreme comparisons and merge expression. */
class IvmAggMinProcessor extends IvmAggExtremalProcessor {
    IvmAggMinProcessor() {
        super(IvmAggFunctionKind.MIN, "DELMIN");
    }

    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof Min;
    }

    @Override
    protected Expression buildExtremeAggregate(Expression input) {
        return new Min(input);
    }

    @Override
    protected Expression deletedExtremeKeepsOldValueValid(Expression deltaDeletedExtreme, Slot oldExtreme) {
        return new GreaterThan(deltaDeletedExtreme, oldExtreme);
    }

    @Override
    protected Expression mergeOldAndInsertedExtreme(Slot oldExtreme, Expression deltaInsertExtreme) {
        return new Least(oldExtreme, deltaInsertExtreme);
    }

    @Override
    protected String fallbackMessage() {
        return "IVM: deleted row may be current MIN value, fallback to COMPLETE";
    }
}
