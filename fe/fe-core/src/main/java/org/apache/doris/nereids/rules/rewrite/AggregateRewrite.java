// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.FunctionCall;
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Used to generate the merge agg node for execution.
 */
public class AggregateRewrite extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalAggregation().thenApply(ctx -> {
            Plan plan = ctx.root;
            Operator operator = plan.getOperator();
            LogicalAggregation agg = (LogicalAggregation) operator;
            for (Expression expression : agg.getoutputExpressions()) {
                if (expression instanceof FunctionCall) {
                    FunctionCall functionCall = (FunctionCall) expression;
                    String functionName = functionCall.getFnName().toString();
                    FunctionSet.
                }
            }
            LogicalAggregation mergeAgg = new LogicalAggregation(
                    agg.getGroupByExpressions(),
                    agg.getoutputExpressions(),
                    true
            );
            return plan(mergeAgg, plan);
        }).toRule(RuleType.REWRITE_AGG);
    }
}
