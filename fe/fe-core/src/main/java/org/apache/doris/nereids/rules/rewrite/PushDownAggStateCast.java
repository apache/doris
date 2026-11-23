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
        // These operators pass through the aggregate's slots. Keep their predicates,
        // order keys and limits unchanged; only the new producer slot passes through.
        while (child instanceof LogicalFilter || child instanceof LogicalSort || child instanceof LogicalLimit
                || (child instanceof LogicalProject && !((LogicalProject<?>) child).isDistinct())) {
            passthrough.add(child);
            child = child.child(0);
        }
        if (!(child instanceof LogicalAggregate)) {
            return null;
        }
        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) child;
        Map<Slot, CombineCombinator> producers = new HashMap<>();
        for (NamedExpression output : aggregate.getOutputExpressions()) {
            if (output instanceof Alias && output.child(0) instanceof CombineCombinator) {
                producers.put(output.toSlot(), (CombineCombinator) output.child(0));
            }
        }
        // NormalizeAggregate can put a projection between HAVING/ORDER BY and the
        // aggregate. Follow only slots and their aliases, not expressions that change
        // the state value or its outer NULL. Do not add columns across DISTINCT.
        for (int i = passthrough.size() - 1; i >= 0; i--) {
            if (passthrough.get(i) instanceof LogicalProject) {
                Map<Slot, CombineCombinator> forwarded = new HashMap<>();
                for (NamedExpression output : ((LogicalProject<?>) passthrough.get(i)).getProjects()) {
                    Expression input = output instanceof Alias ? output.child(0) : output;
                    CombineCombinator producer = producers.get(input);
                    if (producer != null) {
                        forwarded.put(output.toSlot(), producer);
                    }
                }
                producers = forwarded;
            }
        }
        Map<Slot, CombineCombinator> visibleProducers = producers;
        Map<Expression, Alias> addedOutputs = new LinkedHashMap<>();
        List<NamedExpression> projects = project.getProjects().stream()
                .map(expr -> (NamedExpression) expr.rewriteUp(e -> {
                    if (!(e instanceof Cast) || !(e.getDataType() instanceof AggStateType)) {
                        return e;
                    }
                    CombineCombinator producer = visibleProducers.get(e.child(0));
                    if (producer == null) {
                        return e;
                    }
                    Cast converted = ConvertAggStateCast.convert(new Cast(producer, e.getDataType()));
                    if (converted.child() == producer) {
                        return e;
                    }
                    // Keep the original output: another consumer can still need version 20.
                    // A cast to version 19 gets a separate producer and ExprId. Equal casts
                    // share that new output, while different codecs remain distinct.
                    return addedOutputs.computeIfAbsent(converted.child(), Alias::new).toSlot();
                })).collect(ImmutableList.toImmutableList());
        if (addedOutputs.isEmpty()) {
            return null;
        }
        List<NamedExpression> outputs = ImmutableList.<NamedExpression>builder()
                .addAll(aggregate.getOutputExpressions()).addAll(addedOutputs.values()).build();
        Plan rewritten = aggregate.withAggOutput(outputs);
        for (int i = passthrough.size() - 1; i >= 0; i--) {
            Plan parent = passthrough.get(i);
            if (parent instanceof LogicalProject) {
                LogicalProject<?> projection = (LogicalProject<?>) parent;
                List<NamedExpression> forwarded = new ArrayList<>(projection.getProjects());
                addedOutputs.values().forEach(output -> forwarded.add(output.toSlot()));
                parent = projection.withProjects(forwarded);
            }
            rewritten = parent.withChildren(rewritten);
        }
        return project.withProjects(projects).withChildren(rewritten);
    }
}
