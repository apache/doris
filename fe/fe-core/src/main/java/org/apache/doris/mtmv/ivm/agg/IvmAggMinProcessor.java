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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopKMinCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Least;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TopKMergeMin;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;

/** MIN-specific top-K state, delta and merge. */
class IvmAggMinProcessor extends IvmAggExtremalProcessor {
    IvmAggMinProcessor() {
        super(IvmAggFunctionKind.MIN, IvmAggStateKey.MIN_TOPK, "DELMIN");
    }

    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        return function instanceof Min;
    }

    @Override
    protected AggregateFunction buildTopKAggregate(Expression input, long topK) {
        return new TopKMinCount(input, new BigIntLiteral(topK));
    }

    @Override
    protected Expression buildMerge(Expression oldValues, Expression oldBound,
            Expression insertValues, Expression insertBound, Expression deleteValues, Expression deleteBound,
            Expression oldCount, Expression deltaCount, long topK) {
        return new TopKMergeMin(oldValues, oldBound, insertValues, insertBound,
                deleteValues, deleteBound, oldCount, deltaCount, new BigIntLiteral(topK));
    }

    @Override
    protected Expression betterExtreme(Expression oldExtreme, Expression insertedExtreme) {
        return new Least(oldExtreme, insertedExtreme);
    }

    @Override
    protected String fallbackMessage() {
        return IvmFailureClassifier.MIN_MAX_BOUNDARY_MSG_PREFIX
                + ": deleted row may be current MIN value";
    }
}
