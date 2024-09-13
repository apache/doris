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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * extract window expressions from LogicalProject#projects and Normalize LogicalWindow
 */
public class ExtractAndNormalizeWindowExpression extends OneRewriteRuleFactory implements NormalizeToSlot {

    @Override
    public Rule build() {
        return logicalProject()
                .when(project -> ExpressionUtils.containsWindowExpression(project.getProjects()))
                .then(this::normalize)
                .toRule(RuleType.EXTRACT_AND_NORMALIZE_WINDOW_EXPRESSIONS);
    }

    private Plan normalize(LogicalProject<Plan> project) {
        List<NamedExpression> outputs =
                ExpressionUtils.rewriteDownShortCircuit(project.getProjects(), output -> {
                    if (output instanceof WindowExpression) {
                        WindowExpression windowExpression = (WindowExpression) output;
                        Expression expression = ((WindowExpression) output).getFunction();
                        if (expression instanceof NullableAggregateFunction) {
                            // NullableAggregateFunction in window function should be always nullable
                            // Because there may be no data in the window frame, null values will be generated.
                            windowExpression = ((WindowExpression) output).withFunction(
                                    ((NullableAggregateFunction) expression).withAlwaysNullable(true));
                        }

                        ImmutableList.Builder<Expression> nonLiteralPartitionKeys =
                                ImmutableList.builderWithExpectedSize(windowExpression.getPartitionKeys().size());
                        for (Expression partitionKey : windowExpression.getPartitionKeys()) {
                            if (!partitionKey.isConstant()) {
                                nonLiteralPartitionKeys.add(partitionKey);
                            }
                        }

                        ImmutableList.Builder<OrderExpression> nonLiteralOrderExpressions =
                                ImmutableList.builderWithExpectedSize(windowExpression.getOrderKeys().size());
                        for (OrderExpression orderExpr : windowExpression.getOrderKeys()) {
                            if (!orderExpr.getOrderKey().getExpr().isConstant()) {
                                nonLiteralOrderExpressions.add(orderExpr);
                            }
                        }

                        // remove literal partition by and order by keys
                        return windowExpression.withPartitionKeysOrderKeys(
                                nonLiteralPartitionKeys.build(),
                                nonLiteralOrderExpressions.build()
                        );
                    }
                    return output;
                });

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
            normalizedChild = project.withProjectsAndChild(
                    ImmutableList.copyOf(bottomProjects), project.child());
        }

        // 2. handle window's outputs and windowExprs
        // need to replace exprs with SlotReference in WindowSpec, due to LogicalWindow.getExpressions()

        // because alias is pushed down to bottom project
        // we need replace alias's child expr with corresponding alias's slot in output
        // so create a customNormalizeMap alias's child -> alias.toSlot to do it
        Map<Expression, Slot> customNormalizeMap = toBePushedDown.stream()
                .filter(expr -> expr instanceof Alias)
                .collect(Collectors.toMap(expr -> ((Alias) expr).child(), expr -> ((Alias) expr).toSlot(),
                        (oldExpr, newExpr) -> oldExpr));

        // customNormalizeMap is only for alias, so we just normalize alias in outputs too
        List<NamedExpression> normalizedOutputs = context.normalizeToUseSlotRef(outputs,
                (ctx, expr) -> expr instanceof Alias ? customNormalizeMap.getOrDefault(expr, null) : null);
        // replace child exprs in normalizedOutputs by customNormalizeMap
        normalizedOutputs = ExpressionUtils.replaceNamedExpressions(normalizedOutputs, customNormalizeMap);
        Set<WindowExpression> normalizedWindows =
                ExpressionUtils.collect(normalizedOutputs, WindowExpression.class::isInstance);

        existedAlias = ExpressionUtils.collect(normalizedOutputs, Alias.class::isInstance);
        NormalizeToSlotContext ctxForWindows = NormalizeToSlotContext.buildContext(
                existedAlias, Sets.newHashSet(normalizedWindows));

        Set<NamedExpression> normalizedWindowWithAlias = ctxForWindows.pushDownToNamedExpression(normalizedWindows);
        // only need normalized windowExpressions
        LogicalWindow normalizedLogicalWindow =
                new LogicalWindow<>(ImmutableList.copyOf(normalizedWindowWithAlias), normalizedChild);

        // 3. handle top projects
        List<NamedExpression> topProjects = ctxForWindows.normalizeToUseSlotRef(normalizedOutputs);
        return project.withProjectsAndChild(topProjects, normalizedLogicalWindow);
    }

    private Set<Expression> collectExpressionsToBePushedDown(List<NamedExpression> expressions) {
        // bottomProjects includes:
        // 1. expressions from function and WindowSpec's partitionKeys and orderKeys
        // 2. other slots of outputExpressions
        //
        // avg(c) / sum(a+1) over (order by avg(b))  group by a
        // win(x/sum(z) over y)
        //     prj(x, y, a+1 as z)
        //         agg(avg(c) x, avg(b) y, a)
        //             proj(a b c)
        // toBePushDown = {avg(c), a+1, avg(b)}
        return expressions.stream()
            .flatMap(expression -> {
                if (expression.anyMatch(WindowExpression.class::isInstance)) {
                    Set<Slot> inputSlots = Sets.newHashSet(expression.getInputSlots());
                    Set<WindowExpression> collects = expression.collect(WindowExpression.class::isInstance);
                    // substr(
                    //   ref_1.cp_type,
                    //   sum(CASE WHEN ref_1.cp_type  = 0 THEN 3 ELSE 2 END) OVER (),
                    //   1),
                    //
                    //  in above case,
                    //  ref_1.cp_type and CASE WHEN ref_1.cp_type = 0 THEN 3 ELSE 2 END
                    //  should be pushed down.
                    //
                    //  inputSlots= {ref_1.cp_type}
                    return Stream.concat(
                            collects.stream().flatMap(windowExpression ->
                                    windowExpression.getExpressionsInWindowSpec().stream()
                                    // constant arguments may in WindowFunctions(e.g. Lead, Lag)
                                    // which shouldn't be pushed down
                                    .filter(expr -> !expr.isConstant())
                            ),
                            inputSlots.stream()
                    ).distinct();
                }
                return ImmutableList.of(expression).stream();
            })
            .collect(ImmutableSet.toImmutableSet());
    }
}
