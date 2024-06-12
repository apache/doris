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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

import java.util.List;

/**
 * Eliminate GroupBy.
 */
public class EliminateGroupBy extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> ExpressionUtils.allMatch(agg.getGroupByExpressions(), Slot.class::isInstance))
                .then(agg -> {
                    List<Expression> groupByExpressions = agg.getGroupByExpressions();
                    Builder<Slot> groupBySlots
                            = ImmutableSet.builderWithExpectedSize(groupByExpressions.size());
                    for (Expression groupByExpression : groupByExpressions) {
                        groupBySlots.add((Slot) groupByExpression);
                    }
                    Plan child = agg.child();
                    boolean unique = child.getLogicalProperties()
                            .getTrait()
                            .isUniqueAndNotNull(groupBySlots.build());
                    if (!unique) {
                        return null;
                    }
                    for (AggregateFunction f : agg.getAggregateFunctions()) {
                        if (!((f instanceof Sum || f instanceof Count || f instanceof Min || f instanceof Max)
                                && (f.arity() == 1 && f.child(0) instanceof Slot))) {
                            return null;
                        }
                    }
                    List<NamedExpression> outputExpressions = agg.getOutputExpressions();

                    ImmutableList.Builder<NamedExpression> newOutput
                            = ImmutableList.builderWithExpectedSize(outputExpressions.size());

                    for (NamedExpression ne : outputExpressions) {
                        if (ne instanceof Alias && ne.child(0) instanceof AggregateFunction) {
                            AggregateFunction f = (AggregateFunction) ne.child(0);
                            if (f instanceof Sum || f instanceof Min || f instanceof Max) {
                                newOutput.add(new Alias(ne.getExprId(), TypeCoercionUtils
                                        .castIfNotSameType(f.child(0), f.getDataType()), ne.getName()));
                            } else if (f instanceof Count) {
                                newOutput.add((NamedExpression) ne.withChildren(
                                        new If(
                                            new IsNull(f.child(0)),
                                            Literal.of(0),
                                            Literal.of(1)
                                        )
                                ));
                            } else {
                                throw new IllegalStateException("Unexpected aggregate function: " + f);
                            }
                        } else {
                            newOutput.add(ne);
                        }
                    }
                    return PlanUtils.projectOrSelf(newOutput.build(), child);
                }).toRule(RuleType.ELIMINATE_GROUP_BY);
    }
}
