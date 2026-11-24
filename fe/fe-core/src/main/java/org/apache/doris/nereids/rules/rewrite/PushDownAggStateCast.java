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
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.AggStateType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Move codec-only state casts to the COMBINE producer before its bytes are written. */
public class PushDownAggStateCast extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(subTree(LogicalAggregate.class, LogicalFilter.class,
                LogicalSort.class, LogicalLimit.class, LogicalProject.class))
                .when(project -> project.getProjects().stream().anyMatch(
                        expr -> expr.anyMatch(e -> e instanceof Cast
                                && ((Cast) e).getDataType() instanceof AggStateType)))
                .then(PushDownAggStateCast::rewrite)
                .toRule(RuleType.PUSH_DOWN_AGG_STATE_CAST);
    }

    private static Plan rewrite(LogicalProject<Plan> project) {
        Plan child = project.child();
        List<Plan> passthrough = new ArrayList<>();
        // Keep predicates, order keys and limits unchanged. A projection can expose
        // a separate target-codec value while it keeps its original outputs.
        while (child instanceof LogicalFilter || child instanceof LogicalSort || child instanceof LogicalLimit
                || (child instanceof LogicalProject && !((LogicalProject<?>) child).isDistinct())) {
            passthrough.add(child);
            child = child.child(0);
        }
        if (!(child instanceof LogicalAggregate)) {
            return null;
        }
        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) child;
        StateCastRewriter rewriter = new StateCastRewriter(passthrough, aggregate);
        List<NamedExpression> projects = project.getProjects().stream()
                .map(expr -> (NamedExpression) rewriter.rewriteExpression(expr, 0))
                .collect(ImmutableList.toImmutableList());
        if (rewriter.addedOutputs.get(passthrough.size()).isEmpty()) {
            return null;
        }
        List<NamedExpression> outputs = ImmutableList.<NamedExpression>builder()
                .addAll(aggregate.getOutputExpressions())
                .addAll(rewriter.addedOutputs.get(passthrough.size()).values()).build();
        Plan rewritten = aggregate.withAggOutput(outputs);
        for (int i = passthrough.size() - 1; i >= 0; i--) {
            Plan parent = passthrough.get(i);
            if (parent instanceof LogicalProject) {
                LogicalProject<?> projection = (LogicalProject<?>) parent;
                List<NamedExpression> forwarded = new ArrayList<>(projection.getProjects());
                forwarded.addAll(rewriter.addedOutputs.get(i).values());
                parent = projection.withProjects(forwarded);
            }
            rewritten = parent.withChildren(rewritten);
        }
        return project.withProjects(projects).withChildren(rewritten);
    }

    private static class StateCastRewriter {
        private final List<Plan> passthrough;
        private final Map<Slot, CombineCombinator> producers = new HashMap<>();
        private final List<Map<Slot, Expression>> projectInputs = new ArrayList<>();
        private final List<Map<Expression, Alias>> addedOutputs = new ArrayList<>();

        StateCastRewriter(List<Plan> passthrough, LogicalAggregate<?> aggregate) {
            this.passthrough = passthrough;
            for (NamedExpression output : aggregate.getOutputExpressions()) {
                if (output instanceof Alias && output.child(0) instanceof CombineCombinator) {
                    producers.put(output.toSlot(), (CombineCombinator) output.child(0));
                }
            }
            for (Plan plan : passthrough) {
                Map<Slot, Expression> inputs = new HashMap<>();
                if (plan instanceof LogicalProject) {
                    for (NamedExpression output : ((LogicalProject<?>) plan).getProjects()) {
                        inputs.put(output.toSlot(), output instanceof Alias ? output.child(0) : output);
                    }
                }
                projectInputs.add(inputs);
                addedOutputs.add(new LinkedHashMap<>());
            }
            addedOutputs.add(new LinkedHashMap<>()); // Aggregate outputs are at the last level.
        }

        Expression rewriteExpression(Expression expression, int level) {
            return expression.rewriteUp(e -> {
                if (!(e instanceof Cast) || !(e.getDataType() instanceof AggStateType)) {
                    return e;
                }
                // Conditional casts now contain casts of their value slots. Visit
                // those new casts too; the original rewriteUp has already passed them.
                return ConvertAggStateCast.convert((Cast) e).rewriteUp(branch -> retargetSlot(branch, level));
            });
        }

        private Expression retargetSlot(Expression expression, int level) {
            if (!(expression instanceof Cast) || !(expression.getDataType() instanceof AggStateType)
                    || !(expression.child(0) instanceof Slot)
                    || !(expression.child(0).getDataType() instanceof AggStateType)) {
                return expression;
            }
            AggStateType source = (AggStateType) expression.child(0).getDataType();
            AggStateType target = (AggStateType) expression.getDataType();
            // Only move a codec change. Leave other casts at their original location.
            if (source.equals(target) || !source.hasSameAggregateSignature(target)) {
                return expression;
            }
            if (level == passthrough.size()) {
                CombineCombinator producer = producers.get(expression.child(0));
                if (producer == null) {
                    return expression;
                }
                Cast converted = ConvertAggStateCast.convert(new Cast(producer, expression.getDataType()));
                if (converted.child() == producer) {
                    return expression;
                }
                // Keep the original output for other consumers. Equal target-codec
                // casts share a new producer, even when they occur in different branches.
                return addedOutputs.get(level).computeIfAbsent(converted.child(), Alias::new).toSlot();
            }
            if (!(passthrough.get(level) instanceof LogicalProject)) {
                return retargetSlot(expression, level + 1);
            }
            Expression input = projectInputs.get(level).get(expression.child(0));
            if (input == null || input.containsVolatileExpression()
                    || input.containsType(NoneMovableFunction.class)) {
                // Do not duplicate a volatile or non-movable expression for another
                // codec consumer. Leave its cast for the normal compatibility check.
                return expression;
            }
            // A projection below ORDER BY/LIMIT can contain IF rather than a bare slot.
            // Build its target-codec value at that same level. Keep the condition's
            // input slots in scope and preserve the original output for other consumers.
            Expression converted = rewriteExpression(new Cast(input, expression.getDataType()), level + 1);
            return addedOutputs.get(level).computeIfAbsent(converted, Alias::new).toSlot();
        }
    }
}
