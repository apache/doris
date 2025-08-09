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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Implementation rule that convert logical aggregate to physical assertNumRows.
 */
public class LogicalAggregateToPhysicalHashAggregate extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .thenApply(ctx -> implement(ctx.root, ctx.connectContext))
                .toRule(RuleType.LOGICAL_AGGREGATE_TO_PHYSICAL_HASH_AGGREGATE);
    }

    private Plan implement(LogicalAggregate<GroupPlan> logicalAgg, ConnectContext connectContext) {
        if (logicalAgg.hasDistinctFunc()) {
            return null;
        }
        ImmutableList.Builder<NamedExpression> builder = ImmutableList.builder();
        boolean changed = false;
        for (NamedExpression expr : logicalAgg.getOutputExpressions()) {
            if (expr instanceof Alias && expr.child(0) instanceof AggregateFunction) {
                Alias alias = (Alias) expr;
                AggregateExpression aggExpr = new AggregateExpression((AggregateFunction) expr.child(0),
                        logicalAgg.getAggregateParam());
                builder.add(alias.withChildren(ImmutableList.of(aggExpr)));
                changed = true;
            } else {
                builder.add(expr);
            }
        }
        List<NamedExpression> aggOutput = changed ? builder.build() : logicalAgg.getOutputExpressions();
        return new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), aggOutput,
                logicalAgg.getPartitionExpressions(), logicalAgg.getAggregateParam(),
                AggregateUtils.maybeUsingStreamAgg(connectContext, logicalAgg),
                Optional.empty(), logicalAgg.getLogicalProperties(), null,
                logicalAgg.child());
    }
}
