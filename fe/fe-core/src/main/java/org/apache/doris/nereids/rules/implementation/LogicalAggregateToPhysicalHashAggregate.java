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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;
import java.util.Optional;

/**
 * Implementation rule that convert logical aggregate to physical assertNumRows.
 */
public class LogicalAggregateToPhysicalHashAggregate extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(this::implement)
                .toRule(RuleType.LOGICAL_AGGREGATE_TO_PHYSICAL_HASH_AGGREGATE);
    }

    private Plan implement(LogicalAggregate<GroupPlan> logicalAgg) {
        // 这个地方可以控制是否要有一阶段的AGG
        if (!logicalAgg.getAggregateParam().isSplit) {
            if (logicalAgg.isAggregateDistinct()) {
                return null;
            }
            // 如果是没有经过splitagg的原始agg,那么需要将输出中的aggfunc加上AggregateExpression
            List<NamedExpression> aggOutput = ExpressionUtils.rewriteDownShortCircuit(
                    logicalAgg.getOutputExpressions(), expr -> {
                        if (expr instanceof AggregateFunction) {
                            return new AggregateExpression((AggregateFunction) expr, logicalAgg.getAggregateParam());
                        }
                        return expr;
                    });
            return new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), aggOutput,
                    logicalAgg.getPartitionExpressions(), logicalAgg.getAggregateParam(), false,
                     logicalAgg.getLogicalProperties(), null,
                    logicalAgg.child());
        }
        // 这个maybeUsingStream看下从logical agg中删除掉,然后在这里直接设置到physical agg上
        return new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), logicalAgg.getOutputExpressions(),
                logicalAgg.getPartitionExpressions(), logicalAgg.getAggregateParam(), logicalAgg.maybeUsingStream(),
                Optional.empty(), logicalAgg.getLogicalProperties(), null,
                logicalAgg.child());
    }
}
