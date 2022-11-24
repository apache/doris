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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * normalize aggregate's group keys and AggregateFunction's child to SlotReference
 * and generate a LogicalProject top on LogicalAggregate to hold to order of aggregate output,
 * since aggregate output's order could change when we do translate.
 * <p>
 * Apply this rule could simplify the processing of enforce and translate.
 * <pre>
 * Original Plan:
 * Aggregate(
 *   keys:[k1#1, K2#2 + 1],
 *   outputs:[k1#1, Alias(K2# + 1)#4, Alias(k1#1 + 1)#5, Alias(SUM(v1#3))#6,
 *            Alias(SUM(v1#3 + 1))#7, Alias(SUM(v1#3) + 1)#8])
 * </pre>
 * After rule:
 * Project(k1#1, Alias(SR#9)#4, Alias(k1#1 + 1)#5, Alias(SR#10))#6, Alias(SR#11))#7, Alias(SR#10 + 1)#8)
 * +-- Aggregate(keys:[k1#1, SR#9], outputs:[k1#1, SR#9, Alias(SUM(v1#3))#10, Alias(SUM(v1#3 + 1))#11])
 * +-- Project(k1#1, Alias(K2#2 + 1)#9, v1#3)
 * <p>
 * More example could get from UT {NormalizeAggregateTest}
 */
public class NormalizeAggregate extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().whenNot(LogicalAggregate::isNormalized).then(aggregate -> {
            // substitution map used to substitute expression in aggregate's output to use it as top projections
            Map<Expression, Expression> substitutionMap = Maps.newHashMap();
            List<Expression> keys = aggregate.getGroupByExpressions();
            List<NamedExpression> newOutputs = Lists.newArrayList();

            // keys
            Map<Boolean, List<Expression>> partitionedKeys = keys.stream()
                    .collect(Collectors.groupingBy(SlotReference.class::isInstance));
            List<Expression> newKeys = Lists.newArrayList();
            List<NamedExpression> bottomProjections = Lists.newArrayList();
            if (partitionedKeys.containsKey(false)) {
                // process non-SlotReference keys
                newKeys.addAll(partitionedKeys.get(false).stream()
                        .map(e -> new Alias(e, e.toSql()))
                        .peek(a -> substitutionMap.put(a.child(), a.toSlot()))
                        .peek(bottomProjections::add)
                        .map(Alias::toSlot)
                        .collect(Collectors.toList()));
            }
            if (partitionedKeys.containsKey(true)) {
                // process SlotReference keys
                partitionedKeys.get(true).stream()
                        .map(SlotReference.class::cast)
                        .peek(s -> substitutionMap.put(s, s))
                        .peek(bottomProjections::add)
                        .forEach(newKeys::add);
            }
            // add all necessary key to output
            substitutionMap.entrySet().stream()
                    .filter(kv -> aggregate.getOutputExpressions().stream()
                            .anyMatch(e -> e.anyMatch(kv.getKey()::equals)))
                    .map(Entry::getValue)
                    .map(NamedExpression.class::cast)
                    .forEach(newOutputs::add);

            // if we generate bottom, we need to generate to project too.
            // output
            List<NamedExpression> outputs = aggregate.getOutputExpressions();
            Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                    .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)));

            boolean needBottomProjects = partitionedKeys.containsKey(false);
            if (partitionedOutputs.containsKey(true)) {
                // process expressions that contain aggregate function
                Set<AggregateFunction> aggregateFunctions = partitionedOutputs.get(true).stream()
                        .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                        .collect(Collectors.toSet());

                // replace all non-slot expression in aggregate functions children.
                for (AggregateFunction aggregateFunction : aggregateFunctions) {
                    List<Expression> newChildren = Lists.newArrayList();
                    for (Expression child : aggregateFunction.getArguments()) {
                        if (child instanceof SlotReference || child instanceof Literal) {
                            newChildren.add(child);
                            if (child instanceof SlotReference) {
                                bottomProjections.add((SlotReference) child);
                            }
                        } else {
                            needBottomProjects = true;
                            Alias alias = new Alias(child, child.toSql());
                            bottomProjections.add(alias);
                            newChildren.add(alias.toSlot());
                        }
                    }
                    AggregateFunction newFunction = (AggregateFunction) aggregateFunction.withChildren(newChildren);
                    Alias alias = new Alias(newFunction, newFunction.toSql());
                    newOutputs.add(alias);
                    substitutionMap.put(aggregateFunction, alias.toSlot());
                }
            }

            // assemble
            LogicalPlan root = aggregate.child();
            if (needBottomProjects) {
                root = new LogicalProject<>(bottomProjections, root);
            }
            root = new LogicalAggregate<>(newKeys, newOutputs, aggregate.isDisassembled(),
                    true, aggregate.isFinalPhase(), aggregate.getAggPhase(),
                    aggregate.getSourceRepeat(), root);
            List<NamedExpression> projections = outputs.stream()
                    .map(e -> ExpressionUtils.replace(e, substitutionMap))
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
            root = new LogicalProject<>(projections, root);

            return root;
        }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }
}
