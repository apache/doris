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

import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**SimplifyWindowExpression*/
public class SimplifyWindowExpression extends OneRewriteRuleFactory {
    private static final ImmutableSet<String> REWRRITE_TO_CONST_WINDOW_FUNCTIONS =
            ImmutableSet.of("count", "rank", "dense_rank", "row_number", "first_value", "last_value");
    private static final ImmutableSet<String> REWRRITE_TO_SLOT_WINDOW_FUNCTIONS =
            ImmutableSet.of("min", "max", "sum", "avg");

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
            if (!windowExpression.getOrderKeys().isEmpty()) {
                remainWindowExpression.add(expr);
                continue;
            }
            // after normalize window, partition key must be slot
            List<Slot> partitionSlots = (List<Slot>) (List) windowExpression.getPartitionKeys();
            Set<Slot> partitionSlotSet = new HashSet<>(partitionSlots);
            if (!window.getLogicalProperties().getFunctionalDependencies().isUnique(partitionSlotSet)) {
                remainWindowExpression.add(expr);
                continue;
            }
            Expression function = windowExpression.getFunction();
            if (function instanceof BoundFunction) {
                BoundFunction boundFunction = (BoundFunction) function;
                String name = ((BoundFunction) function).getName();
                if (REWRRITE_TO_CONST_WINDOW_FUNCTIONS.contains(name)) {
                    projectionsBuilder.add(new Alias(alias.getExprId(), new TinyIntLiteral((byte)1), alias.getName()));
                } else if (REWRRITE_TO_SLOT_WINDOW_FUNCTIONS.contains(name)) {
                    projectionsBuilder.add(new Alias(alias.getExprId(), boundFunction.child(0), alias.getName()));
                } else {
                    remainWindowExpression.add(expr);
                }
            } else {
                remainWindowExpression.add(expr);
            }
        }
        List<NamedExpression> projections = projectionsBuilder.build();
        if (projections.isEmpty()) {
            return window;
        } else {
            List<NamedExpression> remainWindow = remainWindowExpression.build();
            if (remainWindow.isEmpty()) {
                return new LogicalProject(projections, window.child(0));
            }
            return new LogicalProject(projections, window.withExpression(remainWindow,
                    window.child(0)));
        }
    }

}















