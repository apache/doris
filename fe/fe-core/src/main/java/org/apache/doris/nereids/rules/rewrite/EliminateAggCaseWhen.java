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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Change argument 'case when' or 'if' inside aggregate function , to aggregate function(filter)
 * example:
 * select sum(case when t1.c1 = 101 then 1 END) from t1;
 * ==>
 * select sum(1) from t1 where t1.c1 = 101;
 * note :
 * only If expression is needed to process cause CaseWhenToIf have already changed case when to if
 * but in sql we can still see case when so case when is reserved to explain this rule
 * we can only have one output aggregate function cause of filter would influence other projection
 * we can only have one aggregate function cause of filter would influence other aggregate function
 * we can only have case when/if function without else cause of then can only have one branch of choice
 * we can only have one case in case when cause of then can only have one branch of choice
 */
public final class EliminateAggCaseWhen extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> {
            Set<AggregateFunction> aggFunctions = agg.getAggregateFunctions();
            // check whether we only have one aggregate function, and only one projection of aggregate function
            if (aggFunctions.size() != 1 || agg.getOutputExpressions().size() != 1
                        || !agg.getGroupByExpressions().isEmpty()) {
                return null;
            }
            for (AggregateFunction aggFun : aggFunctions) {
                // check whether we only have on case when/if in aggregate function
                if (aggFun.getArguments().size() != 1) {
                    return null;
                }
                // only If expression is needed to process cause CaseWhenToIf have already changed case when to if
                if (aggFun.getArgument(0) instanceof If) {
                    If anIf = (If) aggFun.getArgument(0);
                    if (!(anIf.getArgument(2) instanceof NullLiteral)) {
                        return null;
                    }
                    Expression operand = anIf.getArgument(0);
                    Filter filter = new LogicalFilter<>(ExpressionUtils.extractConjunctionToSet(operand), agg.child());
                    Expression result = anIf.getArgument(1);
                    Map<Expression, Expression> constantExprsReplaceMap = new HashMap<>(aggFunctions.size());
                    constantExprsReplaceMap.put(aggFun, ((AggregateFunction) aggFun).withChildren(result));
                    return agg.withChildAndOutput((Plan) filter,
                            ExpressionUtils.replaceNamedExpressions(
                                    agg.getOutputExpressions(), constantExprsReplaceMap));
                }
            }
            return null;
        }).toRule(RuleType.ELIMINATE_AGG_CASE_WHEN);
    }
}
