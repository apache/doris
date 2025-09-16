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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

/**
 * Push down the 'partitionTopN' into the 'window'.
 * It will convert the filter condition to the 'limit value' and push down below the 'window'.
 * But there are some restrictions, the details are explained below.
 * For example:
 * 'SELECT * FROM (
 *     SELECT *, ROW_NUMBER() OVER (ORDER BY b) AS row_number
 *     FROM t
 * ) AS tt WHERE row_number <= 100;'
 * The filter 'row_number <= 100' can be pushed down into the window operator.
 * The following will demonstrate how the plan changes:
 * Logical plan tree:
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                 any_node
 * transformed to:
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                partition_topn(PARTITION BY: a, ORDER BY b, Partition Limit: 100)
 *                   |
 *                 any_node
 */

public class CreatePartitionTopNFromWindow extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalWindow()).thenApply(ctx -> {
            LogicalFilter<LogicalWindow<Plan>> filter = ctx.root;
            LogicalWindow<Plan> window = filter.child();

            // We have already done such optimization rule, so just ignore it.
            if (window.child(0) instanceof LogicalPartitionTopN
                    || (window.child(0) instanceof LogicalFilter
                    && window.child(0).child(0) != null
                    && window.child(0).child(0) instanceof LogicalPartitionTopN)) {
                return filter;
            }

            Pair<WindowExpression, Long> windowFuncPair = window.getPushDownWindowFuncAndLimit(filter, Long.MAX_VALUE);
            if (windowFuncPair == null) {
                return filter;
            } else if (windowFuncPair.second == -1) {
                // limit -1 indicating a empty relation case
                return new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(), filter.getOutput());
            } else {
                Plan newWindow = window.pushPartitionLimitThroughWindow(windowFuncPair.first,
                        windowFuncPair.second, false);
                return filter.withChildren(newWindow);
            }
        }).toRule(RuleType.CREATE_PARTITION_TOPN_FOR_WINDOW);
    }
}
