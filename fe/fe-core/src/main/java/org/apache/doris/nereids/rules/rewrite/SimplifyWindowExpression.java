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
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * rewrite func(para) over (partition by unique_keys)
 * 1. func() is count(non-null) or rank/dense_rank/row_number -> 1
 * 2. func(para) is min/max/sum/avg/first_value/last_value -> para
 * e.g
 * select max(c1) over(partition by pk) from t1;
 * -> select c1 from t1;
 * */
@DependsRules({
        ExtractAndNormalizeWindowExpression.class
})
public class SimplifyWindowExpression extends OneRewriteRuleFactory {
    private static final String COUNT = "count";
    private static final ImmutableSet<String> REWRRITE_TO_CONST_WINDOW_FUNCTIONS =
            ImmutableSet.of("rank", "dense_rank", "row_number");
    private static final ImmutableSet<String> REWRRITE_TO_SLOT_WINDOW_FUNCTIONS =
            ImmutableSet.of("min", "max", "sum", "avg", "first_value", "last_value");

    @Override
    public Rule build() {
        return logicalWindow(any()).thenApply(this::simplify)
                .toRule(RuleType.SIMPLIFY_WINDOW_EXPRESSION);
    }

    private Plan simplify(MatchingContext<LogicalWindow<Plan>> ctx) {
        LogicalWindow<Plan> window = ctx.root;
        ImmutableList.Builder<NamedExpression> projectionsBuilder = ImmutableList.builder();
        ImmutableList.Builder<NamedExpression> remainWindowExpression = ImmutableList.builder();
        List<NamedExpression> windowExpressions = window.getWindowExpressions();
        for (NamedExpression expr : windowExpressions) {
            Alias alias = (Alias) expr;
            WindowExpression windowExpression = (WindowExpression) alias.child();
            if (windowExpression.getPartitionKeys().stream().anyMatch((
                    partitionKey -> partitionKey.getDataType().isOnlyMetricType()))) {
                continue;
            }
            // after normalize window, partition key must be slot
            List<Slot> partitionSlots = (List<Slot>) (List) windowExpression.getPartitionKeys();
            Set<Slot> partitionSlotSet = new HashSet<>(partitionSlots);
            if (!window.getLogicalProperties().getTrait().isUnique(partitionSlotSet)) {
                remainWindowExpression.add(expr);
                continue;
            }
            Expression function = windowExpression.getFunction();
            if (function instanceof BoundFunction) {
                BoundFunction boundFunction = (BoundFunction) function;
                String name = ((BoundFunction) function).getName();
                if ((name.equals(COUNT) && checkCount((Count) boundFunction))
                        || REWRRITE_TO_CONST_WINDOW_FUNCTIONS.contains(name)) {
                    projectionsBuilder.add(new Alias(alias.getExprId(),
                            new Cast(new TinyIntLiteral((byte) 1), function.getDataType()), alias.getName()));
                } else if (REWRRITE_TO_SLOT_WINDOW_FUNCTIONS.contains(name)) {
                    projectionsBuilder.add(new Alias(alias.getExprId(),
                            TypeCoercionUtils.castIfNotSameType(boundFunction.child(0), boundFunction.getDataType()),
                            alias.getName()));
                } else {
                    remainWindowExpression.add(expr);
                }
            } else {
                remainWindowExpression.add(expr);
            }
        }
        List<NamedExpression> projections = projectionsBuilder.build();
        List<NamedExpression> remainWindows = remainWindowExpression.build();
        if (projections.isEmpty()) {
            return window;
        } else if (remainWindows.isEmpty()) {
            Plan windowChild = window.child(0);
            List<Slot> slots = windowChild.getOutput();
            List<NamedExpression> finalProjections = Lists.newArrayList(projections);
            finalProjections.addAll(slots);
            return new LogicalProject(finalProjections, windowChild);
        } else {
            List<Slot> windowOutputs = Lists.newArrayList();
            for (NamedExpression remainWindow : remainWindows) {
                windowOutputs.add(remainWindow.toSlot());
            }
            List<NamedExpression> finalProjections = Lists.newArrayList(projections);
            finalProjections.addAll(windowOutputs);
            return new LogicalProject(finalProjections, window.withExpressionsAndChild(remainWindows,
                    window.child(0)));
        }
    }

    private boolean checkCount(Count count) {
        return count.isCountStar() || count.child(0).notNullable();
    }
}
