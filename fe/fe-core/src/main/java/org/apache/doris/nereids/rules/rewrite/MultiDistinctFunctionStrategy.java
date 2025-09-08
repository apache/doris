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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;

/**
 * Processes all distinct function use multi distinct function
 */
public class MultiDistinctFunctionStrategy {
    /** rewrite aggFunc(distinct) to multi_distinct_xxx */
    public static LogicalAggregate<? extends Plan> rewrite(LogicalAggregate<? extends Plan> aggregate) {
        List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        return AggregateUtils.tryConvertToMultiDistinct((AggregateFunction) outputChild);
                    }
                    return outputChild;
                });
        return aggregate.withAggOutput(newOutput);
    }
}
