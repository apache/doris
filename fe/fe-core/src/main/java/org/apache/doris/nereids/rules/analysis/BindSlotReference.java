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
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.operators.plans.logical.LogicalSort;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * BindSlotReference.
 */
public class BindSlotReference implements AnalysisRuleFactory {
    @Override
    public List<Rule<Plan>> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_PROJECT_SLOT.build(
                logicalProject().then(project -> {
                    List<NamedExpression> boundSlots =
                            bind(project.operator.getProjects(), project.children(), project);
                    return plan(new LogicalProject(flatBoundStar(boundSlots)), project.child());
                })
            ),
            RuleType.BINDING_FILTER_SLOT.build(
                logicalFilter().then(filter -> {
                    Expression boundPredicates = bind(
                            filter.operator.getPredicates(), filter.children(), filter);
                    return plan(new LogicalFilter(boundPredicates), filter.child());
                })
            ),
            RuleType.BINDING_JOIN_SLOT.build(
                logicalJoin().then(join -> {
                    Optional<Expression> cond = join.operator.getCondition()
                            .map(expr -> bind(expr, join.children(), join));
                    LogicalJoin op = new LogicalJoin(join.operator.getJoinType(), cond);
                    return plan(op, join.left(), join.right());
                })
            ),
            RuleType.BINDING_AGGREGATE_SLOT.build(
                logicalAggregate().then(agg -> {
                    List<Expression> groupBy = bind(
                            agg.operator.getGroupByExprList(), agg.children(), agg);
                    List<NamedExpression> output = bind(
                            agg.operator.getOutputExpressionList(), agg.children(), agg);
                    LogicalAggregate op = new LogicalAggregate(groupBy, output);
                    return plan(op, agg.child());
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort().then(sort -> {
                    List<OrderKey> sortItemList = sort.operator.getOrderKeys()
                            .stream()
                            .map(orderKey -> {
                                Expression item = bind(orderKey.getExpr(), sort.children(), sort);
                                return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                            }).collect(Collectors.toList());

                    LogicalSort op = new LogicalSort(sortItemList);
                    return plan(op, sort.child());
                })
            )
        );
    }

    private List<NamedExpression> flatBoundStar(List<NamedExpression> boundSlots) {
        return boundSlots
            .stream()
            .flatMap(slot -> {
                if (slot instanceof BoundStar) {
                    return ((BoundStar) slot).getSlots().stream();
                } else {
                    return Stream.of(slot);
                }
            }).collect(Collectors.toList());
    }

    private <E extends Expression> List<E> bind(List<E> exprList, List<Plan> inputs, Plan plan) {
        return exprList.stream()
            .map(expr -> bind(expr, inputs, plan))
            .collect(Collectors.toList());
    }

    private <E extends Expression> E bind(E expr, List<Plan> inputs, Plan plan) {
        List<Slot> boundedSlots = inputs.stream()
                .flatMap(input -> input.getOutput().stream())
                .collect(Collectors.toList());
        return (E) new SlotBinder(boundedSlots, plan).bind(expr);
    }

    private class SlotBinder extends DefaultExpressionRewriter<Void> {
        private List<Slot> boundSlots;
        private Plan plan;

        public SlotBinder(List<Slot> boundSlots, Plan plan) {
            this.boundSlots = boundSlots;
            this.plan = plan;
        }

        public Expression bind(Expression expression) {
            return expression.accept(this, null);
        }

        @Override
        public Expression visitUnboundAlias(UnboundAlias unboundAlias, Void context) {
            Expression child = unboundAlias.child().accept(this, context);
            if (child instanceof NamedExpression) {
                return new Alias(child, ((NamedExpression) child).getName());
            } else {
                // TODO: resolve aliases
                return new Alias(child, child.sql());
            }
        }

        @Override
        public Slot visitUnboundSlot(UnboundSlot unboundSlot, Void context) {
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
        public Expression visitUnboundStar(UnboundStar unboundStar, Void context) {
            if (!(plan.getOperator() instanceof LogicalProject)) {
                throw new AnalysisException("UnboundStar must exists in Projection");
            }
            List<String> qualifier = unboundStar.getQualifier();
            switch (qualifier.size()) {
                case 0: // select *
                    return new BoundStar(boundSlots);
                case 1: // select table.*
                case 2: // select db.table.*
                    return bindQualifiedStar(qualifier, context);
                default:
                    throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
            }
        }

        private BoundStar bindQualifiedStar(List<String> qualifierStar, Void context) {
            // FIXME: compatible with previous behavior:
            // https://github.com/apache/doris/pull/10415/files/3fe9cb0c3f805ab3a9678033b281b16ad93ec60a#r910239452
            List<Slot> slots = boundSlots.stream().filter(boundSlot -> {
                switch (qualifierStar.size()) {
                    // table.*
                    case 1:
                        List<String> boundSlotQualifier = boundSlot.getQualifier();
                        switch (boundSlotQualifier.size()) {
                            // bound slot is `column` and no qualified
                            case 0: return false;
                            case 1: // bound slot is `table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0));
                            case 2:// bound slot is `db`.`table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(1));
                            default:
                                throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, "."));
                        }
                    case 2: // db.table.*
                        boundSlotQualifier = boundSlot.getQualifier();
                        switch (boundSlotQualifier.size()) {
                            // bound slot is `column` and no qualified
                            case 0:
                            case 1: // bound slot is `table`.`column`
                                return false;
                            case 2:// bound slot is `db`.`table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0))
                                        && qualifierStar.get(1).equalsIgnoreCase(boundSlotQualifier.get(1));
                            default:
                                throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, ".") + ".*");
                        }
                    default:
                        throw new AnalysisException("Not supported name: "
                            + StringUtils.join(qualifierStar, ".") + ".*");
                }
            }).collect(Collectors.toList());

            return new BoundStar(slots);
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

    /** BoundStar is used to wrap list of slots for temporary. */
    private class BoundStar extends NamedExpression {
        public BoundStar(List<Slot> children) {
            super(NodeType.BOUND_STAR, children.toArray(new Slot[0]));
            Preconditions.checkArgument(children.stream().allMatch(slot -> !(slot instanceof UnboundSlot)),
                    "BoundStar can not wrap UnboundSlot"
            );
        }

        public List<Slot> getSlots() {
            return (List) children();
        }
    }
}
