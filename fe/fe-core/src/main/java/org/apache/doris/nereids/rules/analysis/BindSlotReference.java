// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * BindSlotReference.
 */
public class BindSlotReference implements AnalysisRuleFactory {
    @Override
    public List<Rule<Plan>> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_PROJECT_SLOT.build(
                logicalProject().then(project -> {
                    List<NamedExpression> boundSlots = bind(project.operator.getProjects(), project.children());
                    return plan(new LogicalProject(boundSlots), project.child());
                })
            ),
            RuleType.BINDING_FILTER_SLOT.build(
                logicalFilter().then(filter -> {
                    Expression boundPredicates = bind(filter.operator.getPredicates(), filter.children());
                    return plan(new LogicalFilter(boundPredicates), filter.child());
                })
            ),
            RuleType.BINDING_JOIN_SLOT.build(
                logicalJoin().then(join -> {
                    Optional<Expression> cond = join.operator.getCondition().map(expr -> bind(expr, join.children()));
                    LogicalJoin op = new LogicalJoin(join.operator.getJoinType(), cond);
                    return plan(op, join.left(), join.right());
                })
            )
        );
    }

    private <E extends Expression> List<E> bind(List<E> exprList, List<Plan> inputs) {
        return exprList.stream()
            .map(expr -> bind(expr, inputs))
            .collect(Collectors.toList());
    }

    private <E extends Expression> E bind(E expr, List<Plan> inputs) {
        List<Slot> boundedSlots = inputs.stream()
                .flatMap(input -> input.getOutput().stream())
                .collect(Collectors.toList());
        return (E) expr.accept(new SlotBinder(boundedSlots), null);
    }

    private class SlotBinder extends DefaultExpressionRewriter<Void> {
        private List<Slot> boundSlots;

        public SlotBinder(List<Slot> boundSlots) {
            this.boundSlots = boundSlots;
        }


        @Override
        public Expression visit(UnboundAlias unboundAlias, Void context) {
            Expression child = unboundAlias.child().accept(this, context);
            return new Alias(child, unboundAlias.getName());
        }

        @Override
        public Slot visit(UnboundSlot unboundSlot, Void context) {
            List<Slot> bounded = bindSlot(unboundSlot, boundSlots);
            switch (bounded.size()) {
                case 0:
                    throw new AnalysisException("Cannot resolve " + unboundSlot.toString());
                case 1:
                    return bounded.get(0);
                default:
                    throw new AnalysisException(unboundSlot + " is ambiguous： "
                        + bounded.stream()
                            .map(Slot::toString)
                            .collect(Collectors.joining(", ")));
            }
        }

        @Override
        public Expression visit(UnboundStar unboundStar, Void context) {
            List<String> qualifier = unboundStar.getQualifier();
            List<Slot> boundSlots = Lists.newArrayList();
            switch (qualifier.size()) {
                case 0: // select *
                    boundSlots.addAll(boundSlots);
                    break;
                case 1: // select table.*
                    analyzeBoundSlots(qualifier, context);
                    break;
                case 2: // select db.table.*
                    analyzeBoundSlots(qualifier, context);
                    break;
                default:
                    throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
            }
            return null;
        }

        private void analyzeBoundSlots(List<String> qualifier, Void context) {
            this.boundSlots.stream()
                    .forEach(slot ->
                        boundSlots.add(visit(new UnboundSlot(
                            ImmutableList.<String>builder()
                                .addAll(qualifier)
                                .add(slot.getName())
                                .build()
                        ), context))
                    );
        }

        private List<Slot> bindSlot(UnboundSlot unboundSlot, List<Slot> boundSlots) {
            return boundSlots.stream().filter(boundSlot -> {
                List<String> nameParts = unboundSlot.getNameParts();
                switch (nameParts.size()) {
                    case 1:
                        // Unbound slot name is `column`
                        return nameParts.get(0).equalsIgnoreCase(boundSlot.getName());
                    case 2:
                        // Unbound slot name is `table`.`column`
                        List<String> qualifier = boundSlot.getQualifier();
                        switch (qualifier.size()) {
                            case 2:
                                // qualifier is `db`.`table`
                                return nameParts.get(0).equalsIgnoreCase(qualifier.get(1))
                                    && nameParts.get(1).equalsIgnoreCase(boundSlot.getName());
                            case 1:
                                // qualifier is `table`
                                return nameParts.get(0).equalsIgnoreCase(qualifier.get(0))
                                    && nameParts.get(1).equalsIgnoreCase(boundSlot.getName());
                            default:
                                throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifier, "."));
                        }
                    default:
                        throw new AnalysisException("Not supported name: "
                            + StringUtils.join(nameParts, "."));
                }
            }).collect(Collectors.toList());
        }
    }
}
