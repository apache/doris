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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.SupportMultiDistinct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Utils for aggregate
 */
public class AggregateUtils {
    public static final double LOW_CARDINALITY_THRESHOLD = 0.001;
    public static final double MID_CARDINALITY_THRESHOLD = 0.01;
    public static final double HIGH_CARDINALITY_THRESHOLD = 0.1;
    public static final int LOW_NDV_THRESHOLD = 1024;

    public static AggregateFunction tryConvertToMultiDistinct(AggregateFunction function) {
        if (function instanceof SupportMultiDistinct && function.isDistinct()) {
            return ((SupportMultiDistinct) function).convertToMultiDistinct();
        }
        return function;
    }

    /**countDistinctMultiExprToCountIf*/
    public static Expression countDistinctMultiExprToCountIf(Count count) {
        Set<Expression> arguments = ImmutableSet.copyOf(count.getArguments());
        Expression countExpr = count.getArgument(arguments.size() - 1);
        for (int i = arguments.size() - 2; i >= 0; --i) {
            Expression argument = count.getArgument(i);
            If ifNull = new If(new IsNull(argument), NullLiteral.INSTANCE, countExpr);
            countExpr = assignNullType(ifNull);
        }
        return new Count(countExpr);
    }

    private static If assignNullType(If ifExpr) {
        If ifWithCoercion = (If) TypeCoercionUtils.processBoundFunction(ifExpr);
        Expression trueValue = ifWithCoercion.getArgument(1);
        if (trueValue instanceof Cast && trueValue.child(0) instanceof NullLiteral) {
            List<Expression> newArgs = Lists.newArrayList(ifWithCoercion.getArguments());
            // backend don't support null type, so we should set the type
            newArgs.set(1, new NullLiteral(((Cast) trueValue).getDataType()));
            return ifWithCoercion.withChildren(newArgs);
        }
        return ifWithCoercion;
    }

    public static boolean maybeUsingStreamAgg(List<Expression> groupExpressions, AggregateParam param) {
        ConnectContext ctx = ConnectContext.get();
        return ctx != null && !ctx.getSessionVariable().disableStreamPreaggregations
                && !groupExpressions.isEmpty()
                && param.aggPhase.isLocal();
    }

    /**hasUnknownStatistics*/
    public static boolean hasUnknownStatistics(Collection<Expression> expressions, Statistics inputStatistics) {
        for (Expression gbyExpr : expressions) {
            ColumnStatistic colStats = inputStatistics.findColumnStatistics(gbyExpr);
            if (colStats == null) {
                colStats = ExpressionEstimation.estimate(gbyExpr, inputStatistics);
            }
            if (colStats == null || colStats.isUnKnown()) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsCountDistinctMultiExpr(LogicalAggregate<? extends Plan> aggregate) {
        return ExpressionUtils.deapAnyMatch(aggregate.getOutputExpressions(), expr ->
                expr instanceof Count && ((Count) expr).isDistinct() && expr.arity() > 1);
    }

    /**getAllKeySet*/
    public static Set<NamedExpression> getAllKeySet(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> distinctArguments = getDistinctNamedExpr(aggregate);
        Set<NamedExpression> groupBySet = getGroupBySetNamedExpr(aggregate);
        return ImmutableSet.<NamedExpression>builder()
                .addAll(groupBySet)
                .addAll(distinctArguments)
                .build();
    }

    /**getGroupBySetNamedExpr*/
    public static Set<NamedExpression> getGroupBySetNamedExpr(LogicalAggregate<? extends Plan> aggregate) {
        return aggregate.getGroupByExpressions().stream()
                .filter(NamedExpression.class::isInstance)
                .map(NamedExpression.class::cast)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**getDistinctNamedExpr*/
    public static Set<NamedExpression> getDistinctNamedExpr(LogicalAggregate<? extends Plan> aggregate) {
        return aggregate.getAggregateFunctions().stream()
                .filter(AggregateFunction::isDistinct)
                .flatMap(aggFunc -> aggFunc.getArguments().stream())
                .filter(NamedExpression.class::isInstance)
                .map(NamedExpression.class::cast)
                .collect(ImmutableSet.toImmutableSet());
    }
}
