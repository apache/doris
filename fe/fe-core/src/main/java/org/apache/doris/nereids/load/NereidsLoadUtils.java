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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNotnullErrorToInvalid;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNullableErrorToNull;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLoadProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPreFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
            public Void visitLogicalProject(LogicalProject<? extends Plan> logicalProject, List<Expression> exprs) {
                for (NamedExpression expr : logicalProject.getProjects()) {
                    if (expr instanceof UnboundAlias) {
                        exprs.add(expr.child(0));
                    } else if (expr instanceof UnboundSlot) {
                        exprs.add(expr);
                    } else {
                        // some error happens
                        exprs.clear();
                        break;
                    }
                }
                return super.visitLogicalProject(logicalProject, exprs);
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
            NereidsParamCreateContext context, boolean isPartialUpdate) throws UserException {
        List<NamedExpression> projects = new ArrayList<>(context.exprMap.size());
        List<String> colNames = new ArrayList<>(context.exprMap.size());
        Set<String> uniqueColNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Expression> entry : context.exprMap.entrySet()) {
            projects.add(new Alias(entry.getValue(), entry.getKey()));
            colNames.add(entry.getKey());
            uniqueColNames.add(entry.getKey());
        }
        Table targetTable = fileGroupInfo.getTargetTable();
        for (SlotReference slotReference : context.scanSlots) {
            String colName = slotReference.getName();
            if (targetTable.getColumn(colName) != null && !uniqueColNames.contains(colName)) {
                projects.add(slotReference);
                colNames.add(colName);
                uniqueColNames.add(colName);
            }
        }

        LogicalPlan currentRootPlan = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                Lists.newArrayList(context.scanSlots));
        if (context.fileGroup.getPrecedingFilterExpr() != null) {
            Set<Expression> conjuncts = new HashSet<>();
            conjuncts.add(context.fileGroup.getPrecedingFilterExpr());
            currentRootPlan = new LogicalPreFilter<>(conjuncts, currentRootPlan);
        }
        if (!projects.isEmpty()) {
            currentRootPlan = new LogicalLoadProject(projects, currentRootPlan);
        }
        if (context.fileGroup.getWhereExpr() != null) {
            Set<Expression> conjuncts = new HashSet<>();
            conjuncts.add(context.fileGroup.getWhereExpr());
            currentRootPlan = new LogicalFilter<>(conjuncts, currentRootPlan);
        }
        currentRootPlan = UnboundTableSinkCreator.createUnboundTableSink(targetTable.getFullQualifiers(), colNames,
                ImmutableList.of(),
                partitionNames != null && partitionNames.isTemp(),
                partitionNames != null ? partitionNames.getPartitionNames() : ImmutableList.of(), isPartialUpdate,
                DMLCommandType.LOAD, currentRootPlan);

        CascadesContext cascadesContext = CascadesContext.initContext(new StatementContext(), currentRootPlan,
                PhysicalProperties.ANY);
        ConnectContext ctx = cascadesContext.getConnectContext();
        try {
            ctx.getSessionVariable().setDebugSkipFoldConstant(true);
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                    ImmutableList.of(Rewriter.bottomUp(new BindExpression(),
                            new LoadProjectRewrite(fileGroupInfo.getTargetTable()),
                            new BindSink(), new PushDownProjectThroughFilter(), new MergeProjects(),
                            new ExpressionNormalization())))
                    .execute();
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
                        if (column.getType().isJsonbType() && expression.getDataType().isStringLikeType()) {
                            Expression realExpr = expression instanceof Alias ? ((Alias) expression).child()
                                    : expression;
                            if (column.isAllowNull() || expression.nullable()) {
                                newProjects.add(
                                        new Alias(new JsonbParseNullableErrorToNull(realExpr), expression.getName()));
                            } else {
                                newProjects.add(
                                        new Alias(new JsonbParseNotnullErrorToInvalid(realExpr), expression.getName()));
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

    private static class PushDownProjectThroughFilter extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalProject(logicalFilter()).thenApply(ctx -> {
                LogicalProject<LogicalFilter<Plan>> project = ctx.root;
                Map<Slot, Expression> replaceMap = Maps.newHashMap();
                for (NamedExpression output : project.getOutputs()) {
                    if (output instanceof Alias) {
                        Set<Slot> inputSlots = output.getInputSlots();
                        int size = inputSlots.size();
                        if (size == 1) {
                            replaceMap.put(inputSlots.iterator().next(), output.toSlot());
                        } else if (size > 1) {
                            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                    String.format("expression %s is not supported", output));
                        }
                    }
                }
                LogicalFilter<Plan> filter = project.child();
                Plan filterChild = filter.child();
                return filter.withConjunctsAndChild(ExpressionUtils.replace(filter.getConjuncts(), replaceMap),
                        project.withChildren(filterChild));
            }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_FILTER);
        }
    }
}
