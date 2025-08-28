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
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.AvgWeighted;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.MaxBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.Median;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MinBy;
import org.apache.doris.nereids.trees.expressions.functions.agg.Percentile;
import org.apache.doris.nereids.trees.expressions.functions.agg.Stddev;
import org.apache.doris.nereids.trees.expressions.functions.agg.StddevSamp;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.functions.agg.Variance;
import org.apache.doris.nereids.trees.expressions.functions.agg.VarianceSamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Eliminate GroupBy.
 */
public class EliminateGroupBy extends OneRewriteRuleFactory {
    private static final ImmutableSet<Class<? extends Expression>> supportedBasicFunctions
            = ImmutableSet.of(Sum.class, Avg.class, Min.class, Max.class, Median.class, AnyValue.class);
    private static final ImmutableSet<Class<? extends Expression>> supportedTwoArgsFunctions
            = ImmutableSet.of(MinBy.class, MaxBy.class, AvgWeighted.class, Percentile.class);
    private static final ImmutableSet<Class<? extends Expression>> supportedDevLikeFunctions
            = ImmutableSet.of(Stddev.class, StddevSamp.class, Variance.class, VarianceSamp.class);
    private static final ImmutableSet<Class<? extends Expression>> supportedFunctionSum0
            = ImmutableSet.of(Sum0.class);
    private static final ImmutableSet<Class<? extends Expression>> allFunctionsExceptCount
            = ImmutableSet.<Class<? extends Expression>>builder()
                    .addAll(supportedBasicFunctions)
                    .addAll(supportedTwoArgsFunctions)
                    .addAll(supportedDevLikeFunctions)
                    .addAll(supportedFunctionSum0)
                    .build();

    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> ExpressionUtils.allMatch(agg.getGroupByExpressions(), Slot.class::isInstance))
                .then(this::rewrite).toRule(RuleType.ELIMINATE_GROUP_BY);
    }

    private Plan rewrite(LogicalAggregate<Plan> agg) {
        Set<Slot> groupBySlots = (Set) ImmutableSet.copyOf(agg.getGroupByExpressions());
        Plan child = agg.child();
        boolean unique = child.getLogicalProperties()
                .getTrait()
                .isUniqueAndNotNull(groupBySlots);
        if (!unique) {
            return null;
        }
        for (AggregateFunction f : agg.getAggregateFunctions()) {
            if (!canRewrite(f)) {
                return null;
            }
        }
        List<NamedExpression> outputExpressions = agg.getOutputExpressions();

        ImmutableList.Builder<NamedExpression> newOutput
                = ImmutableList.builderWithExpectedSize(outputExpressions.size());

        for (NamedExpression ne : outputExpressions) {
            if (ne instanceof Alias && ne.child(0) instanceof AggregateFunction) {
                AggregateFunction f = (AggregateFunction) ne.child(0);
                if (supportedBasicFunctions.contains(f.getClass())) {
                    newOutput.add(new Alias(ne.getExprId(), TypeCoercionUtils
                            .castIfNotSameType(f.child(0), f.getDataType()), ne.getName()));
                } else if (f instanceof Count) {
                    if (((Count) f).isStar()) {
                        newOutput.add((NamedExpression) ne.withChildren(TypeCoercionUtils
                                .castIfNotSameType(new BigIntLiteral(1), f.getDataType())));
                    } else {
                        newOutput.add((NamedExpression) ne.withChildren(
                                ifNullElse(f.child(0), new BigIntLiteral(0), new BigIntLiteral(1))));
                    }
                } else if (f instanceof Sum0) {
                    Coalesce coalesce = new Coalesce(f.child(0),
                            Literal.convertToTypedLiteral(0, f.child(0).getDataType()));
                    newOutput.add((NamedExpression) ne.withChildren(
                            TypeCoercionUtils.castIfNotSameType(coalesce, f.getDataType())));
                } else if (supportedTwoArgsFunctions.contains(f.getClass())) {
                    Expression expr = ifNullElse(f.child(1), new NullLiteral(f.child(0).getDataType()),
                            f.child(0));
                    newOutput.add((NamedExpression) ne.withChildren(
                            TypeCoercionUtils.castIfNotSameType(expr, f.getDataType())));
                } else if (supportedDevLikeFunctions.contains(f.getClass())) {
                    Expression expr = ifNullElse(f.child(0), new NullLiteral(DoubleType.INSTANCE),
                            new DoubleLiteral(0));
                    newOutput.add((NamedExpression) ne.withChildren(expr));
                } else {
                    return null;
                }
            } else {
                newOutput.add(ne);
            }
        }
        return PlanUtils.projectOrSelf(newOutput.build(), child);
    }

    private boolean canRewrite(AggregateFunction f) {
        if (allFunctionsExceptCount.contains(f.getClass())) {
            return true;
        }
        if (f instanceof Count) {
            return ((Count) f).isStar() || 1 == f.arity();
        }
        return false;
    }

    private Expression ifNullElse(Expression conditionExpr, Expression ifExpr, Expression elseExpr) {
        return conditionExpr.nullable() ? new If(new IsNull(conditionExpr), ifExpr, elseExpr) : elseExpr;
    }
}
