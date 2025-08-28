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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.jobs.executor.Analyzer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseErrorToNull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseErrorToValue;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLoadProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPreFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * NereidsLoadUtils
 */
public class NereidsLoadUtils {
    /**
     * parse a expression list as 'select expr1, expr2,... exprn' into a nereids Expression List
     */
    public static List<Expression> parseExpressionSeq(String expressionSeq) throws UserException {
        LogicalPlan parsedPlan = new NereidsParser().parseSingle("SELECT " + expressionSeq);
        List<Expression> expressions = new ArrayList<>();
        parsedPlan.accept(new DefaultPlanVisitor<Void, List<Expression>>() {
            @Override
            public Void visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, List<Expression> exprs) {
                processProject(oneRowRelation.getProjects(), exprs);
                return null;
            }

            @Override
            public Void visitUnboundOneRowRelation(UnboundOneRowRelation oneRowRelation, List<Expression> exprs) {
                processProject(oneRowRelation.getProjects(), exprs);
                return null;
            }

            @Override
            public Void visitLogicalProject(LogicalProject<? extends Plan> logicalProject, List<Expression> exprs) {
                processProject(logicalProject.getProjects(), exprs);
                return null;
            }

            private void processProject(List<NamedExpression> namedExpressions, List<Expression> exprs) {
                for (NamedExpression expr : namedExpressions) {
                    if (expr instanceof UnboundAlias) {
                        exprs.add(expr.child(0));
                    } else if (expr instanceof UnboundSlot) {
                        exprs.add(expr);
                    } else if (expr instanceof Alias && expr.child(0) instanceof Literal) {
                        exprs.add(expr.child(0));
                    } else {
                        // some error happens
                        exprs.clear();
                        break;
                    }
                }
            }
        }, expressions);
        if (expressions.isEmpty()) {
            throw new UserException("parse expression failed: " + expressionSeq);
        }
        return expressions;
    }

    /**
     * create a load plan tree for stream load, routine load and broker load
     */
    public static LogicalPlan createLoadPlan(NereidsFileGroupInfo fileGroupInfo, PartitionNames partitionNames,
            NereidsParamCreateContext context, boolean isPartialUpdate,
            TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy) throws UserException {
        // context.scanSlots represent columns read from external file
        // use LogicalOneRowRelation to hold this info for later use
        LogicalPlan currentRootPlan = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                Lists.newArrayList(context.scanSlots));

        // add prefilter if it exists
        if (context.fileGroup.getPrecedingFilterExpr() != null) {
            Set<Expression> conjuncts = new HashSet<>();
            conjuncts.add(context.fileGroup.getPrecedingFilterExpr());
            currentRootPlan = new LogicalPreFilter<>(conjuncts, currentRootPlan);
        }

        // create a cast project to cast context.scanSlots from varchar type to its correct data type in dest table
        // The scan slot's data type should keep unchanged in the following 2 scenarios:
        // 1. there is no column has same name as the slot, it means the slot is a temporary slot.
        // 2. there is a column mapping expr has same name as the slot, it means the mapping expr would replace the
        // original scan slot use the mapping expr, and its data type will be handled by mapping expr too.
        List<NamedExpression> projects = new ArrayList<>(context.exprMap.size());
        List<String> colNames = new ArrayList<>(context.exprMap.size());
        Set<String> uniqueColNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> exprMapColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Expression> entry : context.exprMap.entrySet()) {
            projects.add(new Alias(entry.getValue(), entry.getKey()));
            colNames.add(entry.getKey());
            uniqueColNames.add(entry.getKey());
            exprMapColumnNames.add(entry.getKey());
        }
        Table targetTable = fileGroupInfo.getTargetTable();
        List<NamedExpression> castScanProjects = new ArrayList<>(context.scanSlots.size());
        for (SlotReference slotReference : context.scanSlots) {
            String colName = slotReference.getName();
            Column col = targetTable.getColumn(colName);
            if (col != null) {
                if (!uniqueColNames.contains(colName)) {
                    projects.add(new UnboundSlot(colName));
                    colNames.add(colName);
                    uniqueColNames.add(colName);
                }
                if (exprMapColumnNames.contains(colName)) {
                    castScanProjects.add(slotReference);
                } else {
                    castScanProjects.add(new Alias(
                            TypeCoercionUtils.castIfNotSameType(slotReference, DataType.fromCatalogType(col.getType())),
                            colName));
                }
            } else {
                castScanProjects.add(slotReference);
            }
        }

        // create a project to case all scan slots to correct data types
        currentRootPlan = new LogicalProject(castScanProjects, currentRootPlan);

        // create a load project to do calculate mapping exprs
        if (!projects.isEmpty()) {
            currentRootPlan = new LogicalLoadProject(projects, currentRootPlan);
        }

        // create a table sink for dest table
        currentRootPlan = UnboundTableSinkCreator.createUnboundTableSink(targetTable.getFullQualifiers(), colNames,
                ImmutableList.of(),
                partitionNames != null && partitionNames.isTemp(),
                partitionNames != null ? partitionNames.getPartitionNames() : ImmutableList.of(), isPartialUpdate,
                partialUpdateNewKeyPolicy, DMLCommandType.LOAD, currentRootPlan);

        CascadesContext cascadesContext = CascadesContext.initContext(new StatementContext(), currentRootPlan,
                PhysicalProperties.ANY);
        ConnectContext ctx = cascadesContext.getConnectContext();
        try {
            ctx.getSessionVariable().setDebugSkipFoldConstant(true);

            Analyzer.buildCustomAnalyzer(cascadesContext, ImmutableList.of(Analyzer.bottomUp(
                    new BindExpression(),
                    new LoadProjectRewrite(fileGroupInfo.getTargetTable()),
                    new BindSink(false),
                    new AddPostFilter(
                            context.fileGroup.getWhereExpr()
                    ),
                    // NOTE: the LogicalOneRowRelation usually not contains slots,
                    //       but NereidsLoadPlanInfoCollector need to parse the slot list.
                    //       load only need merge continued LogicalProject, but not want to
                    //       merge LogicalOneRowRelation by MergeProjectable,
                    //       for example, select cast(id as int), name
                    //       will generate:
                    //
                    //       LogicalProject(projects=[cast(Alias(id#0 as int), name#1)])
                    //                          |
                    //              LogicalOneRowRelation(id#0, name#1)
                    //
                    //      then NereidsLoadPlanInfoCollector can generate collect slots by the
                    //      bottom LogicalOneRowRelation, and provide to upper LogicalProject.
                    //
                    //      but if we use MergeProjectable, it will be
                    //          LogicalOneRowRelation(projects=[Alias(cast(id#0 as int)), name#1)])
                    //
                    //      the NereidsLoadPlanInfoCollector will not generate slot by id#0,
                    //      so we must use MergeProjects here
                    new MergeProjects(),
                    new ExpressionNormalization())
            )).execute();
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext, ImmutableList.of()).execute();
        } catch (Exception exception) {
            throw new UserException(exception.getMessage());
        } finally {
            ctx.getSessionVariable().setDebugSkipFoldConstant(false);
        }

        return (LogicalPlan) cascadesContext.getRewritePlan();
    }

    /** LoadProjectExpressionRewrite */
    private static class LoadProjectRewrite extends OneRewriteRuleFactory {
        private Table tbl;

        public LoadProjectRewrite(Table tbl) {
            this.tbl = tbl;
        }

        @Override
        public Rule build() {
            return logicalLoadProject().thenApply(ctx -> {
                LogicalLoadProject<Plan> project = ctx.root;
                List<NamedExpression> projects = project.getProjects();
                List<NamedExpression> newProjects = new ArrayList<>(projects.size());
                for (NamedExpression expression : projects) {
                    Column column = tbl.getColumn(expression.getName());
                    if (column != null) {
                        // check if the expression has correct data type for bitmap and quantile_state column
                        if (column.getAggregationType() != null) {
                            if (column.getAggregationType() == AggregateType.BITMAP_UNION
                                    && !expression.getDataType().isBitmapType()) {
                                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                        String.format("bitmap column %s require the function return type is BITMAP",
                                                column.getName()));
                            }
                            if (column.getAggregationType() == AggregateType.QUANTILE_UNION
                                    && !expression.getDataType().isQuantileStateType()) {
                                throw new org.apache.doris.nereids.exceptions.AnalysisException(String.format(
                                        "quantile_state column %s require the function return type is QUANTILE_STATE",
                                        column.getName()));
                            }
                        }
                        // rewrite expression for jsonb column
                        if (column.getType().isJsonbType() && expression.getDataType().isStringLikeType()) {
                            Expression realExpr = expression instanceof Alias ? ((Alias) expression).child()
                                    : expression;
                            if (column.isAllowNull() || expression.nullable()) {
                                newProjects.add(
                                        new Alias(new JsonbParseErrorToNull(realExpr), expression.getName()));
                            } else {
                                newProjects.add(
                                        new Alias(new JsonbParseErrorToValue(realExpr), expression.getName()));
                            }
                        } else {
                            newProjects.add(expression);
                        }
                    } else {
                        throw new org.apache.doris.nereids.exceptions.AnalysisException(String
                                .format("can not find column %s in table %s", expression.getName(), tbl.getName()));
                    }
                }
                return new LogicalProject(newProjects, project.child());
            }).toRule(RuleType.REWRITE_LOAD_PROJECT_FOR_STREAM_LOAD);
        }
    }

    /** AddPostFilter
     * The BindSink rule will produce the final project list for load, and the post filter should placed after the
     * final project, so we use a rule to do it instead of adding it to the original plan tree
     * */
    private static class AddPostFilter extends OneRewriteRuleFactory {
        private Expression conjunct;

        public AddPostFilter(Expression conjunct) {
            this.conjunct = conjunct;
        }

        @Override
        public Rule build() {
            return logicalOlapTableSink().whenNot(plan -> plan.child() instanceof LogicalFilter).thenApply(ctx -> {
                if (conjunct != null) {
                    LogicalOlapTableSink logicalOlapTableSink = ctx.root;
                    Set<Expression> conjuncts = new HashSet<>();
                    conjuncts.add(conjunct);
                    return logicalOlapTableSink.withChildren(
                            Lists.newArrayList(
                                    new LogicalFilter(conjuncts, (Plan) logicalOlapTableSink.child(0))));
                } else {
                    return null;
                }
            }).toRule(RuleType.ADD_POST_FILTER_FOR_LOAD);
        }
    }
}
