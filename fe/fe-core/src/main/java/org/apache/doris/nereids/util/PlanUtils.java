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

package org.apache.doris.nereids.util;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.CollectOneLevelRelation;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Util for plan
 */
public class PlanUtils {
    public static Optional<LogicalFilter<? extends Plan>> filter(Set<Expression> predicates, Plan plan) {
        if (predicates.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LogicalFilter<>(predicates, plan));
    }

    public static Plan filterOrSelf(Set<Expression> predicates, Plan plan) {
        return filter(predicates, plan).map(Plan.class::cast).orElse(plan);
    }

    /**
     * normalize comparison predicate on a binary plan to its two sides are corresponding to the child's output.
     */
    public static ComparisonPredicate maybeCommuteComparisonPredicate(ComparisonPredicate expression, Plan left) {
        Set<Slot> slots = expression.left().getInputSlots();
        Set<Slot> leftSlots = left.getOutputSet();
        Set<Slot> buffer = Sets.newHashSet(slots);
        buffer.removeAll(leftSlots);
        return buffer.isEmpty() ? expression : expression.commute();
    }

    public static Optional<LogicalProject<? extends Plan>> project(List<NamedExpression> projects, Plan plan) {
        if (projects.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LogicalProject<>(projects, plan));
    }

    public static Plan projectOrSelf(List<NamedExpression> projects, Plan plan) {
        return project(projects, plan).map(Plan.class::cast).orElse(plan);
    }

    public static LogicalAggregate<Plan> distinct(Plan plan) {
        if (plan instanceof LogicalAggregate && ((LogicalAggregate<?>) plan).isDistinct()) {
            return (LogicalAggregate<Plan>) plan;
        } else {
            return new LogicalAggregate<>(ImmutableList.copyOf(plan.getOutput()), false, plan);
        }
    }

    /**
     * For the columns whose output exists in grouping sets, they need to be assigned as nullable.
     */
    public static List<NamedExpression> adjustNullableForRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> outputs) {
        Set<Slot> groupingSetsUsedSlots = groupingSets.stream()
                .flatMap(Collection::stream)
                .map(Expression::getInputSlots)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        Builder<NamedExpression> nullableOutputs = ImmutableList.builderWithExpectedSize(outputs.size());
        for (NamedExpression output : outputs) {
            Expression nullableOutput = output.rewriteUp(expr -> {
                if (expr instanceof Slot && groupingSetsUsedSlots.contains(expr)) {
                    return ((Slot) expr).withNullable(true);
                }
                return expr;
            });
            nullableOutputs.add((NamedExpression) nullableOutput);
        }
        return nullableOutputs.build();
    }

    /**
     * merge childProjects with parentProjects
     */
    public static List<NamedExpression> mergeProjections(List<? extends NamedExpression> childProjects,
            List<? extends NamedExpression> parentProjects) {
        Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(childProjects);
        return ExpressionUtils.replaceNamedExpressions(parentProjects, replaceMap);
    }

    public static List<Expression> replaceExpressionByProjections(List<NamedExpression> childProjects,
            List<Expression> targetExpression) {
        Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(childProjects);
        return ExpressionUtils.replace(targetExpression, replaceMap);
    }

    /**
     * replace targetExpressions with project.
     * if the target expression contains a slot which is an alias and its origin expression contains
     * non-foldable expression and the slot exits multiple times, then can not replace.
     * for example, target expressions: [a, a + 10],  child project: [ t + random() as a ],
     * if replace with the projects, then result expressions: [ t + random(),  t + random() + 10 ],
     * it will calculate random two times, this is error.
     */
    public static boolean canMergeWithProjections(List<? extends NamedExpression> childProjects,
            List<? extends Expression> targetExpressions) {
        Set<Slot> uniqueFunctionSlots = Sets.newHashSet();
        for (Entry<Slot, Expression> kv : ExpressionUtils.generateReplaceMap(childProjects).entrySet()) {
            if (kv.getValue().containsUniqueFunction()) {
                uniqueFunctionSlots.add(kv.getKey());
            }
        }
        if (uniqueFunctionSlots.isEmpty()) {
            return true;
        }

        Set<Slot> counterSet = Sets.newHashSet();
        return targetExpressions.stream().noneMatch(target -> target.anyMatch(
                e -> (e instanceof Slot) && uniqueFunctionSlots.contains(e) && !counterSet.add((Slot) e)));
    }

    public static Plan skipProjectFilterLimit(Plan plan) {
        if (plan instanceof LogicalProject && ((LogicalProject<?>) plan).isAllSlots()
                || plan instanceof LogicalFilter || plan instanceof LogicalLimit) {
            return plan.child(0);
        }
        return plan;
    }

    public static Set<LogicalCatalogRelation> getLogicalScanFromRootPlan(LogicalPlan rootPlan) {
        return rootPlan.collect(LogicalCatalogRelation.class::isInstance);
    }

    /**
     * get table set from plan root.
     */
    public static ImmutableSet<TableIf> getTableSet(LogicalPlan plan) {
        Set<LogicalCatalogRelation> tableSet = plan.collect(LogicalCatalogRelation.class::isInstance);
        return tableSet.stream()
                .map(LogicalCatalogRelation::getTable)
                .collect(ImmutableSet.<TableIf>toImmutableSet());
    }

    /** fastGetChildrenOutput */
    public static List<Slot> fastGetChildrenOutputs(List<Plan> children) {
        switch (children.size()) {
            case 1: return children.get(0).getOutput();
            case 0: return ImmutableList.of();
            default: {
            }
        }

        int outputNum = 0;
        // child.output is cached by AbstractPlan.logicalProperties,
        // we can compute output num without the overhead of re-compute output
        for (Plan child : children) {
            List<Slot> output = child.getOutput();
            outputNum += output.size();
        }
        // generate output list only copy once and without resize the list
        Builder<Slot> output = ImmutableList.builderWithExpectedSize(outputNum);
        for (Plan child : children) {
            output.addAll(child.getOutput());
        }
        return output.build();
    }

    /** fastGetChildrenOutput */
    public static List<Slot> fastGetChildrenAsteriskOutputs(List<Plan> children) {
        switch (children.size()) {
            case 1: return children.get(0).getAsteriskOutput();
            case 0: return ImmutableList.of();
            default: {
            }
        }

        int outputNum = 0;
        // child.output is cached by AbstractPlan.logicalProperties,
        // we can compute output num without the overhead of re-compute output
        for (Plan child : children) {
            List<Slot> output = child.getAsteriskOutput();
            outputNum += output.size();
        }
        // generate output list only copy once and without resize the list
        Builder<Slot> output = ImmutableList.builderWithExpectedSize(outputNum);
        for (Plan child : children) {
            output.addAll(child.getAsteriskOutput());
        }
        return output.build();
    }

    /** fastGetInputSlots */
    public static Set<Slot> fastGetInputSlots(List<? extends Expression> expressions) {
        switch (expressions.size()) {
            case 1: return expressions.get(0).getInputSlots();
            case 0: return ImmutableSet.of();
            default: {
            }
        }

        int inputSlotsNum = 0;
        // child.inputSlots is cached by Expression.inputSlots,
        // we can compute output num without the overhead of re-compute output
        for (Expression expr : expressions) {
            Set<Slot> output = expr.getInputSlots();
            inputSlotsNum += output.size();
        }
        // generate output list only copy once and without resize the list
        ImmutableSet.Builder<Slot> inputSlots = ImmutableSet.builderWithExpectedSize(inputSlotsNum);
        for (Expression expr : expressions) {
            inputSlots.addAll(expr.getInputSlots());
        }
        return inputSlots.build();
    }

    /**
     * Check if slot is from the plan.
     */
    public static boolean checkSlotFrom(Plan plan, SlotReference slot) {
        Set<LogicalCatalogRelation> tableSets = PlanUtils.getLogicalScanFromRootPlan((LogicalPlan) plan);
        for (LogicalCatalogRelation table : tableSets) {
            if (table.getOutputExprIds().contains(slot.getExprId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the expression is a column reference.
     */
    public static boolean isColumnRef(Expression expr) {
        return expr instanceof SlotReference
                && ((SlotReference) expr).getOriginalColumn().isPresent()
                && ((SlotReference) expr).getOriginalTable().isPresent();
    }

    /**
     * collect non_window_agg_func
     */
    public static class CollectNonWindowedAggFuncs {
        public static List<AggregateFunction> collect(Collection<? extends Expression> expressions) {
            List<AggregateFunction> aggFunctions = Lists.newArrayList();
            for (Expression expression : expressions) {
                doCollect(expression, aggFunctions);
            }
            return aggFunctions;
        }

        public static List<AggregateFunction> collect(Expression expression) {
            List<AggregateFunction> aggFuns = Lists.newArrayList();
            doCollect(expression, aggFuns);
            return aggFuns;
        }

        private static void doCollect(Expression expression, List<AggregateFunction> aggFunctions) {
            expression.foreach(expr -> {
                if (expr instanceof AggregateFunction) {
                    aggFunctions.add((AggregateFunction) expr);
                    return true;
                } else if (expr instanceof WindowExpression) {
                    WindowExpression windowExpression = (WindowExpression) expr;
                    for (Expression exprInWindowsSpec : windowExpression.getExpressionsInWindowSpec()) {
                        doCollect(exprInWindowsSpec, aggFunctions);
                    }
                    return true;
                } else {
                    return false;
                }
            });
        }
    }

    /**OutermostPlanFinderContext*/
    public static class OutermostPlanFinderContext {
        public Plan outermostPlan = null;
        public boolean found = false;
    }

    /**OutermostPlanFinder*/
    public static class OutermostPlanFinder extends
            DefaultPlanVisitor<Void, OutermostPlanFinderContext> {
        public static final OutermostPlanFinder INSTANCE = new OutermostPlanFinder();

        @Override
        public Void visit(Plan plan, OutermostPlanFinderContext ctx) {
            if (ctx.found) {
                return null;
            }
            ctx.outermostPlan = plan;
            ctx.found = true;
            return null;
        }

        @Override
        public Void visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
                OutermostPlanFinderContext ctx) {
            if (ctx.found) {
                return null;
            }
            return super.visit(cteAnchor, ctx);
        }

        @Override
        public Void visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
                OutermostPlanFinderContext ctx) {
            return null;
        }
    }

    /**
     * translate to legacy expr, which do not need complex expression and table columns
     */
    public static Expr translateToLegacyExpr(Expression expression, TableIf table, ConnectContext ctx) {
        LogicalEmptyRelation plan = new LogicalEmptyRelation(
                ConnectContext.get().getStatementContext().getNextRelationId(), new ArrayList<>());
        CascadesContext cascadesContext = CascadesContext.initContext(ctx.getStatementContext(), plan,
                PhysicalProperties.ANY);
        ExpressionAnalyzer analyzer = new CustomExpressionAnalyzer(table, cascadesContext);
        expression = analyzer.analyze(expression);

        PlanTranslatorContext translatorContext = new PlanTranslatorContext(cascadesContext);
        ExpressionToExpr translator = new ExpressionToExpr();
        return expression.accept(translator, translatorContext);
    }

    private static class CustomExpressionAnalyzer extends ExpressionAnalyzer {
        private Map<String, DataType> columnTypes = new CaseInsensitiveMap();

        public CustomExpressionAnalyzer(TableIf table, CascadesContext cascadesContext) {
            super(null, new Scope(ImmutableList.of()), cascadesContext, false, false);
            if (table != null) {
                for (Column column : table.getFullSchema()) {
                    columnTypes.put(column.getName(), DataType.fromCatalogType(column.getType()));
                }
            }
        }

        @Override
        public Expression visitUnboundSlot(UnboundSlot unboundSlot, ExpressionRewriteContext context) {
            DataType dataType = columnTypes.getOrDefault(unboundSlot.getName(), VarcharType.MAX_VARCHAR_TYPE);
            return new SlotReference(unboundSlot.getName(), dataType);
        }
    }

    private static class ExpressionToExpr extends ExpressionTranslator {
        @Override
        public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
            SlotRef slotRef = new SlotRef(slotReference.getDataType().toCatalogDataType(), slotReference.nullable());
            slotRef.setLabel(slotReference.getName());
            slotRef.setCol(slotReference.getName());
            slotRef.disableTableName();
            return slotRef;
        }
    }

    /**
     * table collect
     */
    public static Map<List<String>, TableIf> tableCollect(String sql, ConnectContext connectContext) {
        ImmutableList<RewriteJob> rewriteJobs = ImmutableList.of(Rewriter.topDown(new CollectOneLevelRelation()));
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        CascadesContext cascadesContext = CascadesContext.initContext(
                statementContext, new NereidsParser().parseSingle(sql), PhysicalProperties.GATHER);
        Rewriter.getCteChildrenRewriter(cascadesContext, rewriteJobs).execute();
        return statementContext.getTables();
    }
}
