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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;

/**
 * WrapAggregateExpressionForAggregateFunction.
 *
 * this rule exists the last of the rewrite stage, so aggregate rules in optimize stage can separate stage
 * for the aggregate
 */
public class WrapAggregateExpressionForAggregateFunction extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> !ExpressionUtils.containsType(agg.getOutputExpressions(), AggregateExpression.class))
                .then(agg -> {
                    List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                            agg.getOutputExpressions(),
                            outputChild -> {
                                if (outputChild instanceof AggregateFunction) {
                                    AggregateFunction aggregateFunction = (AggregateFunction) outputChild;
                                    return new AggregateExpression(aggregateFunction, AggregateParam.localGather());
                                }
                                return outputChild;
                            });
                    return agg.withAggOutput(newOutput);
                }).toRule(RuleType.WRAP_AGGREGATE_EXPRESSION_FOR_AGGREGATE_FUNCTION);
    }
}
