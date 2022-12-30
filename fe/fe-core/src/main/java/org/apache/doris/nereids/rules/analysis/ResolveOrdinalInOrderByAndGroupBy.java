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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rewrite.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * SELECT col1, col2 FROM t1 ORDER BY 1 -> SELECT col1, col2 FROM t1 ORDER BY col1
 * SELECT col1, SUM(col2) FROM t1 GROUP BY 1 -> SELECT col1, SUM(col2) FROM t1 GROUP BY col1
 */
public class ResolveOrdinalInOrderByAndGroupBy implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.<Rule>builder()
                .add(RuleType.RESOLVE_ORDINAL_IN_ORDER_BY.build(
                        logicalSort().then(sort -> {
                            List<Slot> childOutput = sort.child().getOutput();
                            List<OrderKey> orderKeys = sort.getOrderKeys();
                            List<OrderKey> orderKeysWithoutOrd = new ArrayList<>();
                            for (OrderKey k : orderKeys) {
                                Expression expression = k.getExpr();
                                expression = FoldConstantRule.INSTANCE.rewrite(expression);
                                if (expression instanceof IntegerLikeLiteral) {
                                    IntegerLikeLiteral i = (IntegerLikeLiteral) expression;
                                    int ord = i.getIntValue();
                                    checkOrd(ord, childOutput.size());
                                    orderKeysWithoutOrd
                                            .add(new OrderKey(childOutput.get(ord - 1), k.isAsc(), k.isNullFirst()));
                                } else {
                                    orderKeysWithoutOrd.add(k);
                                }
                            }
                            return sort.withOrderByKey(orderKeysWithoutOrd);
                        })
                ))
                .add(RuleType.RESOLVE_ORDINAL_IN_GROUP_BY.build(
                        logicalAggregate().then(agg -> {
                            List<NamedExpression> aggOutput = agg.getOutputExpressions();
                            List<Expression> groupByWithoutOrd = new ArrayList<>();
                            boolean ordExists = false;
                            for (Expression groupByExpr : agg.getGroupByExpressions()) {
                                groupByExpr = FoldConstantRule.INSTANCE.rewrite(groupByExpr);
                                if (groupByExpr instanceof IntegerLikeLiteral) {
                                    IntegerLikeLiteral i = (IntegerLikeLiteral) groupByExpr;
                                    int ord = i.getIntValue();
                                    checkOrd(ord, aggOutput.size());
                                    Expression aggExpr = aggOutput.get(ord - 1);
                                    groupByWithoutOrd.add(aggExpr);
                                    ordExists = true;
                                } else {
                                    groupByWithoutOrd.add(groupByExpr);
                                }
                            }
                            if (ordExists) {
                                return new LogicalAggregate(groupByWithoutOrd, agg.getOutputExpressions(), agg.child());
                            } else {
                                return agg;
                            }

                        }))).build();
    }

    private void checkOrd(int ord, int childOutputSize) {
        if (ord < 1 || ord > childOutputSize) {
            throw new IllegalStateException(String.format("ordinal exceeds number of items in select list: %s", ord));
        }
    }
}
