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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * We don't fill the missing slots in FillUpMissingSlots.
 * Because for distinct queries,
 * for example:
 * select distinct year,country from sales having year > 2000 qualify row_number() over (order by year + 1) > 1;
 * It would be converted into the form of agg.
 * before logical plan:
 * qualify
 *   |
 * project(distinct)
 *   |
 * scan
 * apply ProjectWithDistinctToAggregate rule
 * after logical plan:
 * qualify
 *   |
 *  agg
 *   |
 * scan
 * if fill the missing slots in FillUpMissingSlots(after ProjectWithDistinctToAggregate). qualify could hardly be
 * pushed under the agg of distinct.
 * But apply FillUpQualifyMissingSlot rule before ProjectWithDistinctToAggregate
 * logical plan:
 * project(distinct)
 *   |
 * qualify
 *   |
 * project
 *   |
 * scan
 * and then apply ProjectWithDistinctToAggregate rule
 * logical plan:
 * agg
 *   |
 * qualify
 *   |
 * project
 *   |
 * scan
 * So it is easy to handle.
 */
public class FillUpQualifyMissingSlot extends FillUpMissingSlots {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            /*
               qualify -> project
               qualify -> project(distinct)
               qualify -> project(distinct) -> agg
               qualify -> project(distinct) -> having -> agg
            */
            RuleType.FILL_UP_QUALIFY_PROJECT.build(
                logicalQualify(logicalProject())
                    .then(qualify -> {
                        checkWindow(qualify);
                        LogicalProject<Plan> project = qualify.child();
                        return createPlan(project, qualify.getConjuncts(), (newConjuncts, projects) -> {
                            LogicalProject<Plan> bottomProject = new LogicalProject<>(projects, project.child());
                            LogicalQualify<Plan> logicalQualify = new LogicalQualify<>(newConjuncts, bottomProject);
                            ImmutableList<NamedExpression> copyOutput = ImmutableList.copyOf(project.getOutput());
                            return new LogicalProject<>(copyOutput, project.isDistinct(), logicalQualify);
                        });
                    })
            ),
            /*
               qualify -> agg
             */
            RuleType.FILL_UP_QUALIFY_AGGREGATE.build(
                logicalQualify(aggregate()).then(qualify -> {
                    checkWindow(qualify);
                    Aggregate<Plan> agg = qualify.child();
                    Resolver resolver = new Resolver(agg);
                    qualify.getConjuncts().forEach(resolver::resolve);
                    return createPlan(resolver, agg, (r, a) -> {
                        Set<Expression> newConjuncts = ExpressionUtils.replace(
                                qualify.getConjuncts(), r.getSubstitution());
                        boolean notChanged = newConjuncts.equals(qualify.getConjuncts());
                        if (notChanged && a.equals(agg)) {
                            return null;
                        }
                        return notChanged ? qualify.withChildren(a) : new LogicalQualify<>(newConjuncts, a);
                    });
                })
            ),
            /*
               qualify -> having -> agg
             */
            RuleType.FILL_UP_QUALIFY_HAVING_AGGREGATE.build(
                logicalQualify(logicalHaving(aggregate())).then(qualify -> {
                    checkWindow(qualify);
                    LogicalHaving<Aggregate<Plan>> having = qualify.child();
                    Aggregate<Plan> agg = qualify.child().child();
                    Resolver resolver = new Resolver(agg);
                    qualify.getConjuncts().forEach(resolver::resolve);
                    return createPlan(resolver, agg, (r, a) -> {
                        Set<Expression> newConjuncts = ExpressionUtils.replace(
                                qualify.getConjuncts(), r.getSubstitution());
                        boolean notChanged = newConjuncts.equals(qualify.getConjuncts());
                        if (notChanged && a.equals(agg)) {
                            return null;
                        }
                        return notChanged ? qualify.withChildren(having.withChildren(a)) :
                            new LogicalQualify<>(newConjuncts, having.withChildren(a));
                    });
                })
            ),
            /*
               qualify -> having -> project
               qualify -> having -> project(distinct)
             */
            RuleType.FILL_UP_QUALIFY_HAVING_PROJECT.build(
                logicalQualify(logicalHaving(logicalProject())).then(qualify -> {
                    checkWindow(qualify);
                    LogicalHaving<LogicalProject<Plan>> having = qualify.child();
                    LogicalProject<Plan> project = qualify.child().child();
                    return createPlan(project, qualify.getConjuncts(), (newConjuncts, projects) -> {
                        ImmutableList<NamedExpression> copyOutput = ImmutableList.copyOf(project.getOutput());
                        if (project.isDistinct()) {
                            Set<Slot> missingSlots = having.getExpressions().stream()
                                    .map(Expression::getInputSlots)
                                    .flatMap(Set::stream)
                                    .filter(s -> !projects.contains(s))
                                    .collect(Collectors.toSet());
                            List<NamedExpression> output = ImmutableList.<NamedExpression>builder()
                                    .addAll(projects).addAll(missingSlots).build();
                            LogicalQualify<LogicalProject<Plan>> logicalQualify =
                                    new LogicalQualify<>(newConjuncts, new LogicalProject<>(output, project.child()));
                            return having.withChildren(project.withProjects(copyOutput).withChildren(logicalQualify));
                        } else {
                            return new LogicalProject<>(copyOutput, new LogicalQualify<>(newConjuncts,
                                    having.withChildren(project.withProjects(projects))));
                        }
                    });
                })
            )
        );
    }

    interface PlanGenerator {
        Plan apply(Set<Expression> newConjuncts, List<NamedExpression> projects);
    }

    private Plan createPlan(LogicalProject<Plan> project, Set<Expression> conjuncts, PlanGenerator planGenerator) {
        Set<Slot> projectOutputSet = project.getOutputSet();
        List<NamedExpression> newOutputSlots = Lists.newArrayList();
        Set<Expression> newConjuncts = new HashSet<>();
        for (Expression conjunct : conjuncts) {
            conjunct = conjunct.accept(new DefaultExpressionRewriter<List<NamedExpression>>() {
                @Override
                public Expression visitWindow(WindowExpression window, List<NamedExpression> context) {
                    Alias alias = new Alias(window);
                    context.add(alias);
                    return alias.toSlot();
                }
            }, newOutputSlots);
            newConjuncts.add(conjunct);
        }
        Set<Slot> notExistedInProject = conjuncts.stream()
                .map(Expression::getInputSlots)
                .flatMap(Set::stream)
                .filter(s -> !projectOutputSet.contains(s))
                .collect(Collectors.toSet());

        newOutputSlots.addAll(notExistedInProject);
        if (newOutputSlots.isEmpty()) {
            return null;
        }
        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                .addAll(project.getProjects())
                .addAll(newOutputSlots).build();

        return planGenerator.apply(newConjuncts, projects);
    }

    private void checkWindow(LogicalQualify<? extends Plan> qualify) throws AnalysisException {
        Set<SlotReference> inputSlots = new HashSet<>();
        AtomicBoolean hasWindow = new AtomicBoolean(false);
        for (Expression conjunct : qualify.getConjuncts()) {
            conjunct.accept(new DefaultExpressionVisitor<Void, Set<SlotReference>>() {
                @Override
                public Void visitWindow(WindowExpression windowExpression, Set<SlotReference> context) {
                    hasWindow.set(true);
                    return null;
                }

                @Override
                public Void visitSlotReference(SlotReference slotReference, Set<SlotReference> context) {
                    context.add(slotReference);
                    return null;
                }

            }, inputSlots);
        }
        if (hasWindow.get()) {
            return;
        }
        qualify.accept(new DefaultPlanVisitor<Void, Void>() {
            private void findWindow(List<NamedExpression> namedExpressions) {
                for (NamedExpression slot : namedExpressions) {
                    if (slot instanceof Alias && slot.child(0) instanceof WindowExpression) {
                        if (inputSlots.contains(slot.toSlot())) {
                            hasWindow.set(true);
                        }
                    }
                }
            }

            @Override
            public Void visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
                findWindow(project.getProjects());
                return visit(project, context);
            }

            @Override
            public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
                findWindow(aggregate.getOutputExpressions());
                return visit(aggregate, context);
            }
        }, null);
        if (!hasWindow.get()) {
            throw new AnalysisException("qualify only used for window expression");
        }
    }
}
