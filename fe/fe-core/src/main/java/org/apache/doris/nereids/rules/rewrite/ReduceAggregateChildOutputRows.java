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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.Set;

/** ReduceAggregateChildOutputRows
 * with group by:
 * select max(1) from t1 group by c1; -> select 1 from (select c1 from t1 group by c1);
 * without group by:
 * select max(1) from t1; -> select max(1) from (select 1 from t1 limit 1) tmp;
 * */
public class ReduceAggregateChildOutputRows extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> {
            Set<AggregateFunction> aggFunctions = agg.getAggregateFunctions();
            // check whether we have aggregate(constant) in all aggregateFunctions
            if (!(agg.child() instanceof LogicalLimit && ((LogicalLimit) agg.child()).getLimit() == 1)
                    || aggFunctions.isEmpty() || !aggFunctions.stream().allMatch(
                        f -> (f instanceof Min || f instanceof Max)
                            && (f.arity() == 1 && f.child(0).isConstant()))) {
                return null;
            }

            ImmutableList.Builder<NamedExpression> newOutput = ImmutableList.builder();
            for (int i = 0; i < agg.getOutputExpressions().size(); i++) {
                NamedExpression expr = agg.getOutputExpressions().get(i);
                if (expr instanceof Alias && expr.child(0) instanceof AggregateFunction) {
                    AggregateFunction f = (AggregateFunction) expr.child(0);
                    if (f instanceof Min || f instanceof Max) {
                        newOutput.add(new Alias(expr.getExprId(), f.child(0), expr.getName()));
                    } else {
                        throw new AnalysisException("Unexpected aggregate function: " + f);
                    }
                } else {
                    newOutput.add(expr);
                }
            }

            if (agg.getGroupByExpressions().isEmpty()) {
                LogicalAggregate newAgg = new LogicalAggregate<>(agg.getGroupByExpressions(),
                        agg.getOutputExpressions(), new LogicalLimit(1, 0, LimitPhase.ORIGIN,
                                new LogicalProject<>(newOutput.build(), agg.child())));
                return newAgg;
            } else {
                ImmutableList.Builder<NamedExpression> childOutput =
                        ImmutableList.builderWithExpectedSize(agg.getGroupByExpressions().size());
                for (Expression expr : agg.getGroupByExpressions()) {
                    childOutput.add((NamedExpression) expr);
                }
                return new LogicalProject<>(newOutput.build(),
                                new LogicalAggregate<>(agg.getGroupByExpressions(), childOutput.build(), agg.child()));
            }
        }).toRule(RuleType.REDUCE_AGGREGATE_CHILD_OUTPUT_ROWS);
    }

}
