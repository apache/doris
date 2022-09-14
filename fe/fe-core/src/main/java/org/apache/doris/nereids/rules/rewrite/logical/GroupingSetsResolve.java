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
import org.apache.doris.nereids.rules.analysis.OneAnalysisRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.grouping.GroupingSetsFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalGroupBy;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.planner.PlannerContext;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parse the grouping sets operator,
 * for the groupingFunc function, create a virtualSlotRefrance for a period of time.
 *
 * At the same time, synchronize the groupBy in groupingSets to aggregate.
 */
public class GroupingSetsResolve extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalGroupBy().whenNot(LogicalGroupBy::isResolved)).thenApply(ctx -> {
            LogicalAggregate<LogicalGroupBy<GroupPlan>> aggregate = ctx.root;
            LogicalGroupBy<GroupPlan> groupBy = aggregate.child();

            List<Expression> originGroupingExprs = groupBy.getOriginalGroupByExpressions();
            List<List<Expression>> groupingSets = groupBy.getGroupingSets();
            Set<VirtualSlotReference> newVirtualSlotRefs = new LinkedHashSet<>();
            newVirtualSlotRefs.addAll(groupBy.getVirtualSlotRefs());

            // resolve grouping func in groupBy
            List<NamedExpression> groupByNewOutput =
                    groupBy.getOutputExpressions().stream()
                            .map(e -> resolve(e, newVirtualSlotRefs))
                            .map(NamedExpression.class::cast)
                            .collect(Collectors.toList());

            List<BitSet> groupingIdList = groupBy.genGroupingIdList(originGroupingExprs, groupingSets);
            List<Expression> virtualGroupingExprs = Lists.newArrayList();
            virtualGroupingExprs.addAll(newVirtualSlotRefs);
            List<List<Long>> groupingList = groupBy.genGroupingList(originGroupingExprs,
                    virtualGroupingExprs, groupingIdList, newVirtualSlotRefs);
            List<Expression> aggGroupByExpressions = new ArrayList<>(originGroupingExprs);
            aggGroupByExpressions.addAll(virtualGroupingExprs);

            return new LogicalAggregate<>(aggGroupByExpressions, groupByNewOutput,
                    groupBy.replace(groupingSets, originGroupingExprs,
                    groupByNewOutput,
                    groupingIdList, newVirtualSlotRefs, virtualGroupingExprs,
                    groupingList, true, groupBy.hasChangedOutput(), groupBy.isNormalized()));
        }).toRule(RuleType.GROUP_BY_RESOLVE);
    }

    private Expression resolve(Expression expr, Set<VirtualSlotReference> virtualSlotRefs) {
        return new ResolveGroupingFun(virtualSlotRefs).resolve(expr);
    }

    private class ResolveGroupingFun extends DefaultExpressionRewriter<PlannerContext> {
        private final Set<VirtualSlotReference> virtualSlotRefs;

        public ResolveGroupingFun(Set<VirtualSlotReference> virtualSlotRefs) {
            this.virtualSlotRefs = Objects.requireNonNull(virtualSlotRefs, "virtualSlotRefs can not null");
        }

        public Expression resolve(Expression expr) {
            return expr.accept(this, null);
        }

        @Override
        public Expression visitGroupingSetsFunction(
                GroupingSetsFunction groupingSetsFunction, PlannerContext ctx) {
            if (groupingSetsFunction.child(0) instanceof VirtualSlotReference) {
                return groupingSetsFunction;
            }
            return genVirtualSlotReference(groupingSetsFunction);
        }

        private Expression genVirtualSlotReference(GroupingSetsFunction groupingSetsFunction) {
            String colName = groupingSetsFunction.children().stream()
                    .map(Expression::toSql).collect(Collectors.joining("_"));
            colName = LogicalGroupBy.GROUPING_PREFIX + colName;
            VirtualSlotReference virtualSlotReference = new VirtualSlotReference(
                    colName, BigIntType.INSTANCE, groupingSetsFunction.children(), false);
            virtualSlotRefs.add(virtualSlotReference);
            return groupingSetsFunction.repeatChildrenWithVirtualRef(virtualSlotReference,
                    Optional.of(groupingSetsFunction.children()));
        }
    }
}
