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
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;

/**
 * change scalar sub query containing agg to window function. such as:
 * SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
 *  FROM lineitem, part
 *  WHERE p_partkey = l_partkey AND
 *  p_brand = 'Brand#23' AND
 *  p_container = 'MED BOX' AND
 *  l_quantity<(SELECT 0.2*avg(l_quantity)
 *  FROM lineitem
 *  WHERE l_partkey = p_partkey);
 * to:
 * SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
 *  FROM (SELECT l_extendedprice, l_quantity,
 *    avg(l_quantity)over(partition by p_partkey)
 *    AS avg_l_quantity
 *    FROM lineitem, part
 *    WHERE p_partkey = l_partkey and
 *    p_brand = 'Brand#23' and
 *    p_container = 'MED BOX') t
 * WHERE l_quantity < 0.2 * avg_l_quantity;
 */

public class AggScalarSubQueryToWindowFunction implements RewriteRuleFactory {
    private static final ImmutableSet<Class<? extends AggregateFunction>> supportedFunction = ImmutableSet.of(
            Min.class, Max.class, Count.class, Sum.class, Avg.class
    );

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION.build(
                        // logical apply to ensure there's scalar sub-query in outer scope and the sub-query contains a
                        // aggregation.
                        logicalApply(any(), logicalAggregate())
                                .when(apply -> apply.getSubqueryExpr() instanceof ScalarSubquery)
                                .when(apply -> checkCorrelatedSlot(apply.getCorrelationSlot()))
                                .when(apply -> checkAggFunction(apply.getSubqueryExpr().getQueryPlan()))
                                .then(apply -> {
                                    return null;
                                })
                )
        );
    }

    private boolean checkAggFunction(LogicalPlan subQuery) {
        Preconditions.checkArgument(subQuery instanceof LogicalAggregate);
        LogicalAggregate<Plan> aggregate = ((LogicalAggregate<Plan>) subQuery);
        List<AggregateFunction> functionSet = ExpressionUtils.collectAll(aggregate.getExpressions(),
                AggregateFunction.class::isInstance);
        return supportedFunction.containsAll(functionSet);
    }

    private boolean checkCorrelatedSlot(List<Expression> correlatedSlots) {
        return false;
    }
}

/*
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
    FROM lineitem, part
    WHERE p_partkey = l_partkey AND
    p_brand = 'Brand#23' AND
    p_container = 'MED BOX' AND
    l_quantity<(SELECT 0.2*avg(l_quantity)
    FROM lineitem
    WHERE l_partkey = p_partkey);
*/
