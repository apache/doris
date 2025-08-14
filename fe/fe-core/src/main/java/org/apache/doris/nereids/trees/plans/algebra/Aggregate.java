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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.SupportMultiDistinct;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Common interface for logical/physical Aggregate.
 */
public interface Aggregate<CHILD_TYPE extends Plan> extends UnaryPlan<CHILD_TYPE>, OutputPrunable {

    List<Expression> getGroupByExpressions();

    List<NamedExpression> getOutputExpressions();

    Aggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput);

    @Override
    Aggregate<Plan> withChildren(List<Plan> children);

    @Override
    default Aggregate<CHILD_TYPE> pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withAggOutput(prunedOutputs);
    }

    default Set<AggregateFunction> getAggregateFunctions() {
        return ExpressionUtils.collect(getOutputExpressions(), AggregateFunction.class::isInstance);
    }

    /** getDistinctArguments */
    default Set<Expression> getDistinctArguments() {
        ImmutableSet.Builder<Expression> distinctArguments = ImmutableSet.builder();
        for (NamedExpression outputExpression : getOutputExpressions()) {
            outputExpression.foreach(expr -> {
                if (expr instanceof AggregateFunction) {
                    AggregateFunction aggFun = (AggregateFunction) expr;
                    if (aggFun.isDistinct()) {
                        distinctArguments.addAll(aggFun.getDistinctArguments());
                    }
                }
            });
        }
        return distinctArguments.build();
    }

    /** everyDistinctArgumentNumIsOne */
    default boolean everyDistinctArgumentNumIsOne() {
        AtomicBoolean hasDistinctArguments = new AtomicBoolean(false);
        for (NamedExpression outputExpression : getOutputExpressions()) {
            boolean distinctArgumentSizeNotOne = outputExpression.anyMatch(expr -> {
                if (expr instanceof AggregateFunction) {
                    AggregateFunction aggFun = (AggregateFunction) expr;
                    if (aggFun.isDistinct()) {
                        hasDistinctArguments.set(true);
                        return aggFun.getDistinctArguments().size() != 1;
                    }
                }
                return false;
            });
            if (distinctArgumentSizeNotOne) {
                return false;
            }
        }
        return hasDistinctArguments.get();
    }

    /** mustUseMultiDistinctAgg */
    default boolean mustUseMultiDistinctAgg() {
        for (AggregateFunction aggregateFunction : getAggregateFunctions()) {
            if (aggregateFunction.mustUseMultiDistinctAgg()) {
                return true;
            }
        }
        return false;
    }

    default boolean isDistinct() {
        return getOutputExpressions().stream().allMatch(e -> e instanceof Slot)
                && getGroupByExpressions().stream().allMatch(e -> e instanceof Slot);
    }

    /**
     * Skew rewrite is applicable only when all the following conditions are met:
     * 1. The rule is not disabled in the current session (checked via `disableRules`).
     * 2. There is exactly one distinct argument (e.g., `COUNT(DISTINCT x,y)` cannot be optimized).
     * 3. There is exactly one aggregate function (e.g., not mixed `COUNT` and `SUM`).
     * 4. The aggregate function supports multi-distinct (e.g., `COUNT`, `SUM`, `GROUP_CONCAT`).
     * 6. The aggregate function is marked as skewed
     * 7. Skew rewrite requires group by key.
     * 8. The distinct argument is not part of the GROUP BY.
     */
    default boolean canSkewRewrite() {
        ConnectContext connectContext = ConnectContext.get();
        BitSet disableRules = connectContext == null ? new BitSet() : connectContext.getStatementContext()
                .getOrCacheDisableRules(connectContext.getSessionVariable());
        if (disableRules.get(RuleType.AGG_SKEW_REWRITE.type())) {
            return false;
        }
        Set<Expression> distinctArguments = getDistinctArguments();
        Set<AggregateFunction> aggregateFunctions = getAggregateFunctions();
        return distinctArguments.size() == 1
                && aggregateFunctions.size() == 1
                && aggregateFunctions.iterator().next() instanceof SupportMultiDistinct
                && aggregateFunctions.iterator().next().isSkew()
                && aggregateFunctions.iterator().next().arity() == 1
                && aggregateFunctions.iterator().next().child(0) instanceof Slot
                && !getGroupByExpressions().isEmpty()
                && !(new HashSet<>(getGroupByExpressions()).containsAll(distinctArguments));
    }
}
