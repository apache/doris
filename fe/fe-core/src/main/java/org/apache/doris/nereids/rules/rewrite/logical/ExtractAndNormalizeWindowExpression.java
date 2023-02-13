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
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * extract window expressions from LogicalProject.projects and Normalize LogicalWindow
 */
public class ExtractAndNormalizeWindowExpression extends OneRewriteRuleFactory implements NormalizeToSlot {

    @Override
    public Rule build() {
        return logicalProject().when(project -> containsWindowExpression(project.getProjects())).then(project -> {
            List<NamedExpression> outputs = project.getProjects();

            // 1. handle bottom projects
            Set<Alias> existedAlias = ExpressionUtils.collect(outputs, Alias.class::isInstance);
            Set<Expression> toBePushedDown = collectExpressionsToBePushedDown(outputs);
            NormalizeToSlotContext context = NormalizeToSlotContext.buildContext(existedAlias, toBePushedDown);
            // set toBePushedDown exprs as NamedExpression, e.g. (a+1) -> Alias(a+1)
            Set<NamedExpression> bottomProjects = context.pushDownToNamedExpression(toBePushedDown);
            Plan normalizedChild;
            if (bottomProjects.isEmpty()) {
                normalizedChild = project.child();
            } else {
                boolean needAggregate = bottomProjects.stream().anyMatch(expr ->
                        expr.anyMatch(AggregateFunction.class::isInstance));
                if (needAggregate) {
                    normalizedChild = new LogicalAggregate<>(
                            ImmutableList.of(), ImmutableList.copyOf(bottomProjects), project.child());
                } else {
                    normalizedChild = new LogicalProject<>(ImmutableList.copyOf(bottomProjects), project.child());
                }
            }

            // 2. handle window's outputs and windowExprs
            // need to replace exprs with SlotReference in WindowSpec, due to LogicalWindow.getExpressions()
            List<NamedExpression> normalizedOutputs1 = context.normalizeToUseSlotRef(outputs);
            Set<WindowExpression> normalizedWindows =
                    ExpressionUtils.collect(normalizedOutputs1, WindowExpression.class::isInstance);

            existedAlias = ExpressionUtils.collect(normalizedOutputs1, Alias.class::isInstance);
            NormalizeToSlotContext ctxForWindows = NormalizeToSlotContext.buildContext(
                    existedAlias, Sets.newHashSet(normalizedWindows));

            Set<NamedExpression> normalizedWindowWithAlias = ctxForWindows.pushDownToNamedExpression(normalizedWindows);
            // only need normalized windowExpressions
            LogicalWindow normalizedLogicalWindow =
                    new LogicalWindow(Lists.newArrayList(normalizedWindowWithAlias), normalizedChild);

            // 3. handle top projects
            List<NamedExpression> topProjects = ctxForWindows.normalizeToUseSlotRef(normalizedOutputs1);
            return new LogicalProject<>(topProjects, normalizedLogicalWindow);
        }).toRule(RuleType.EXTRACT_AND_NORMALIZE_WINDOW_EXPRESSIONS);
    }

    private Set<Expression> collectExpressionsToBePushedDown(List<NamedExpression> expressions) {
        // bottomProjects includes:
        // 1. expressions from function and WindowSpec's partitionKeys and orderKeys
        // 2. other slots of outputExpressions
        return expressions.stream()
            .flatMap(expression -> {
                if (expression.anyMatch(WindowExpression.class::isInstance)) {
                    Set<WindowExpression> collects = expression.collect(WindowExpression.class::isInstance);
                    return collects.stream().flatMap(windowExpression ->
                        windowExpression.getExpressionsInWindowSpec().stream()
                            // constant arguments may in WindowFunctions(e.g. Lead, Lag), which shouldn't be pushed down
                            .filter(expr -> !expr.isConstant())
                    );
                }
                return ImmutableList.of(expression).stream();
            })
            .collect(ImmutableSet.toImmutableSet());
    }

    private boolean containsWindowExpression(List<NamedExpression> expressions) {
        // WindowExpression in top LogicalProject will be normalized as Alias(SlotReference) after this rule,
        // so it will not be normalized infinitely
        return expressions.stream().anyMatch(expr -> expr.anyMatch(WindowExpression.class::isInstance));
    }
}
