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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * select id, max(a) from t group by id having max(a)>10;
 * ->
 * select id, max(a) from t where a>10 group by id;
 * select id, min(a) from t group by id having min(a)<10;
 * ->
 * select id, min(a) from t where a<10 group by id;
 */
@DependsRules({
        ExpressionRewrite.class
})
public class MaxMinFilterPushDown extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalAggregate().whenNot(agg -> agg.getGroupByExpressions().isEmpty()))
                .then(this::pushDownMaxMinFilter)
                .toRule(RuleType.MAX_MIN_FILTER_PUSH_DOWN);
    }

    private Plan pushDownMaxMinFilter(LogicalFilter<LogicalAggregate<Plan>> filter) {
        Set<Expression> conjuncts = filter.getConjuncts();
        LogicalAggregate<Plan> agg = filter.child();
        Plan aggChild = agg.child();
        List<NamedExpression> aggOutputExpressions = agg.getOutputExpressions();
        Set<Expression> aggFuncs = ExpressionUtils.collect(aggOutputExpressions,
                expr -> expr instanceof AggregateFunction);
        Set<Expression> maxMinFunc = ExpressionUtils.collect(aggFuncs,
                expr -> expr instanceof Max || expr instanceof Min);
        // LogicalAggregate only outputs one aggregate function, which is max or min
        if (aggFuncs.size() != 1 || maxMinFunc.size() != 1) {
            return null;
        }
        ExprId exprId = null;
        Expression func = maxMinFunc.iterator().next();
        for (NamedExpression expr : aggOutputExpressions) {
            if (expr instanceof Alias && ((Alias) expr).child().equals(func)) {
                Alias alias = (Alias) expr;
                exprId = alias.getExprId();
            }
        }
        // try to find min(a)<10 or max(a)>10
        Expression originConjunct = findMatchingConjunct(conjuncts, func instanceof Max, exprId).orElse(null);
        if (null == originConjunct) {
            return null;
        }
        Set<Expression> newUpperConjuncts = new HashSet<>(conjuncts);
        newUpperConjuncts.remove(originConjunct);
        Expression newPredicate = null;
        if (func instanceof Max) {
            if (originConjunct instanceof GreaterThan) {
                newPredicate = new GreaterThan(func.child(0), originConjunct.child(1));
            } else if (originConjunct instanceof GreaterThanEqual) {
                newPredicate = new GreaterThanEqual(func.child(0), originConjunct.child(1));
            }
        } else {
            if (originConjunct instanceof LessThan) {
                newPredicate = new LessThan(func.child(0), originConjunct.child(1));
            } else if (originConjunct instanceof LessThanEqual) {
                newPredicate = new LessThanEqual(func.child(0), originConjunct.child(1));
            }
        }
        Preconditions.checkState(newPredicate != null, "newPredicate is null");
        LogicalFilter<Plan> newPushDownFilter = new LogicalFilter<>(ImmutableSet.of(newPredicate), aggChild);
        LogicalAggregate<Plan> newAgg = agg.withChildren(ImmutableList.of(newPushDownFilter));
        return PlanUtils.filterOrSelf(newUpperConjuncts, newAgg);
    }

    private Optional<Expression> findMatchingConjunct(Set<Expression> conjuncts, boolean isMax, ExprId exprId) {
        for (Expression conjunct : conjuncts) {
            if ((isMax && (conjunct instanceof GreaterThan || conjunct instanceof GreaterThanEqual))
                    || (!isMax && (conjunct instanceof LessThan || conjunct instanceof LessThanEqual))) {
                if (conjunct.child(0) instanceof SlotReference && conjunct.child(1) instanceof Literal) {
                    SlotReference slot = (SlotReference) conjunct.child(0);
                    if (slot.getExprId().equals(exprId)) {
                        return Optional.of(conjunct);
                    }
                }
            }
        }
        return Optional.empty();
    }
}
