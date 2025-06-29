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
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.SupportMultiDistinct;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

/**
 * If there are multiple distinct aggregate functions that cannot
 * be transformed into multi_distinct, an error is reported.
 * The following functions can be transformed into multi_distinct:
 * - count -> MULTI_DISTINCT_COUNT
 * - sum -> MULTI_DISTINCT_SUM
 * - avg -> MULTI_DISTINCT_AVG
 * - group_concat -> MULTI_DISTINCT_GROUP_CONCAT
 * - collect_list -> MULTI_DISTINCT_COLLECT_LIST
 * - array_agg -> MULTI_DISTINCT_ARRAY_AGG
 */
public class CheckMultiDistinct extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> checkDistinct(agg)).toRule(RuleType.CHECK_ANALYSIS);
    }

    private LogicalAggregate checkDistinct(LogicalAggregate<? extends Plan> aggregate) {
        if (aggregate.getDistinctArguments().size() > 1) {

            for (AggregateFunction func : aggregate.getAggregateFunctions()) {
                if (func.isDistinct() && !(func instanceof SupportMultiDistinct)) {
                    throw new AnalysisException(func.toString() + " can't support multi distinct.");
                }
            }
        }

        boolean distinctMultiColumns = false;
        for (AggregateFunction func : aggregate.getAggregateFunctions()) {
            if (!func.isDistinct()) {
                continue;
            }
            if (func.arity() <= 1) {
                continue;
            }
            for (int i = 1; i < func.arity(); i++) {
                if (!func.child(i).getInputSlots().isEmpty() && !(func.child(i) instanceof OrderExpression)) {
                    // think about group_concat(distinct col_1, ',')
                    distinctMultiColumns = true;
                    break;
                }
            }
            if (distinctMultiColumns) {
                break;
            }
        }

        long distinctFunctionNum = 0;
        for (AggregateFunction aggregateFunction : aggregate.getAggregateFunctions()) {
            distinctFunctionNum += aggregateFunction.isDistinct() ? 1 : 0;
        }

        if (distinctMultiColumns && distinctFunctionNum > 1) {
            throw new AnalysisException(
                    "The query contains multi count distinct or sum distinct, each can't have multi columns");
        }
        return aggregate;
    }
}
