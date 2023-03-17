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

import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    private static final ImmutableSet<Class<? extends AggregateFunction>> SUPPORTED_FUNCTION = ImmutableSet.of(
            Min.class, Max.class, Count.class, Sum.class, Avg.class
    );
    private static final ImmutableSet<Class<? extends LogicalPlan>> LEFT_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalOlapScan.class, LogicalLimit.class, LogicalJoin.class, LogicalProject.class
    );
    private static final ImmutableSet<Class<? extends LogicalPlan>> RIGHT_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalOlapScan.class, LogicalJoin.class, LogicalProject.class, LogicalAggregate.class, LogicalFilter.class
    );

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION.build(
                        logicalFilter(logicalProject(logicalApply(any(), logicalAggregate())))
                                .when(node -> {
                                    LogicalApply apply = node.child().child();
                                    return apply.isScalar() && apply.isCorrelated();
                                })
                                .then(node -> {
                                    return null;
                                })
                )
        );
    }

    // check children's nodes because query process will be changed
    private boolean checkPlanType(LogicalPlan outerPlan, LogicalPlan innerPlan) {
        return outerPlan.anyMatch(p -> LEFT_SUPPORTED_PLAN.contains(p.getClass()))
                && innerPlan.anyMatch(p -> RIGHT_SUPPORTED_PLAN.contains(p.getClass()));
    }

    // check aggregation of inner scope
    private boolean checkAggType(LogicalPlan innerPlan) {
        List<LogicalAggregate> aggSet = innerPlan.collectToList(LogicalAggregate.class::isInstance);
        if (aggSet.size() > 1) {
            // window functions don't support nesting.
            return false;
        }
        return ((List<AggregateFunction>) ExpressionUtils.collectAll(
                aggSet.get(0).getOutputExpressions(), AggregateFunction.class::isInstance))
                .stream().allMatch(f -> SUPPORTED_FUNCTION.contains(f.getClass()) && !f.isDistinct());
    }

    // check if the relations of the outer's includes the inner's
    private boolean checkRelation(LogicalPlan outerPlan, LogicalPlan innerPlan) {
        List<TableIf> outerTables = outerPlan.collectToList(LogicalRelation.class::isInstance)
                .stream().map(r -> ((LogicalRelation) r).getTable()).collect(Collectors.toList());
        List<TableIf> innerTables = innerPlan.collectToList(LogicalRelation.class::isInstance)
                .stream().map(r -> ((LogicalRelation) r).getTable()).collect(Collectors.toList());

        // to check all of the relation is different.
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
