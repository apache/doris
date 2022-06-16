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
import org.apache.doris.nereids.trees.expressions.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
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
        Rule<Plan> bindProjectSlot = logicalProject().then(project -> {
            List<Slot> output = project.child().getOutput();
            List<NamedExpression> bound = project.operator.getProjects().stream()
                    .map(expr -> (NamedExpression) bind(expr, output))
                    .collect(Collectors.toList());
            return new LogicalUnaryPlan(new LogicalProject(bound), project.child());
        }).toRule(RuleType.BINDING_PROJECT_SLOT);

        Rule<Plan> bindFilterSlot = logicalFilter().then(filter -> {
            List<Slot> output = filter.child().getOutput();
            Expression boundPredicates = bind(filter.operator.getPredicates(), output);
            return new LogicalUnaryPlan(new LogicalFilter(boundPredicates), filter.child());
        }).toRule(RuleType.BINDING_FILTER_SLOT);

        Rule<Plan> bindJoinSlot = logicalJoin().then(join -> {
            List<Slot> output = Lists.newArrayList();
            output.addAll(join.left().getOutput());
            output.addAll(join.right().getOutput());
            Optional<Expression> cond = join.operator.getCondition().map(expr -> bind(expr, output));
            LogicalJoin op = new LogicalJoin(join.operator.getJoinType(), cond);
            return new LogicalBinaryPlan(op, join.left(), join.right());
        }).toRule(RuleType.BINDING_JOIN_SLOT);

        return new ImmutableList.Builder<Rule<Plan>>().add(bindProjectSlot, bindFilterSlot, bindJoinSlot).build();
    }

    private Expression bind(Expression expr, List<Slot> boundSlots) {
        return new SlotBinder(boundSlots).visit(expr, null);
    }

    private class SlotBinder extends DefaultExpressionRewriter<Void> {
        private List<Slot> boundSlots;

        public SlotBinder(List<Slot> boundSlots) {
            this.boundSlots = boundSlots;
        }

        @Override
        public Expression visitUnboundAlias(UnboundAlias unboundAlias, Void context) {
            // todo: impl
            return super.visitUnboundAlias(unboundAlias, context);
        }

        @Override
        public Expression visitUnboundSlot(UnboundSlot unboundSlot, Void context) {
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
            // todo: impl
            return super.visitUnboundStar(unboundStar, context);
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
                                throw new AnalysisException(
                                        "Not supported qualifier: " + StringUtils.join(qualifier, "."));
                        }
                    default:
                        throw new AnalysisException("Not supported name: " + StringUtils.join(nameParts, "."));
                }
            }).collect(Collectors.toList());
        }
    }
}
