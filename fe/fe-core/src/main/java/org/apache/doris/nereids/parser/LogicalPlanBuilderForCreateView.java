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

package org.apache.doris.nereids.parser;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.AggClauseContext;
import org.apache.doris.nereids.DorisParser.AliasQueryContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.GroupingElementContext;
import org.apache.doris.nereids.DorisParser.HavingClauseContext;
import org.apache.doris.nereids.DorisParser.IdentifierContext;
import org.apache.doris.nereids.DorisParser.LateralViewContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SelectColumnClauseContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.TableAliasContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.Optional;

/**LogicalPlanBuilderForCreateView*/
public class LogicalPlanBuilderForCreateView extends LogicalPlanBuilder {
    @Override
    protected LogicalPlan withGenerate(LogicalPlan plan, LateralViewContext ctx) {
        ConnectContext.get().getStatementContext().addIndexInSqlToString(
                Pair.of(ctx.tableName.start.getStartIndex(),
                        ctx.tableName.stop.getStopIndex()),
                Utils.qualifiedNameWithBackquote(ImmutableList.of(ctx.tableName.getText())));
        for (IdentifierContext colCtx : ctx.columnNames) {
            ConnectContext.get().getStatementContext().addIndexInSqlToString(
                    Pair.of(colCtx.start.getStartIndex(),
                            colCtx.stop.getStopIndex()),
                    Utils.qualifiedNameWithBackquote(ImmutableList.of(colCtx.getText())));
        }
        return super.withGenerate(plan, ctx);
    }

    @Override
    public LogicalSubQueryAlias<Plan> visitAliasQuery(AliasQueryContext ctx) {
        ConnectContext.get().getStatementContext().addIndexInSqlToString(
                Pair.of(ctx.identifier().start.getStartIndex(),
                        ctx.identifier().stop.getStopIndex()),
                Utils.qualifiedNameWithBackquote(ImmutableList.of(ctx.identifier().getText())));
        if (ctx.columnAliases() != null) {
            for (IdentifierContext colCtx : ctx.columnAliases().identifier()) {
                ConnectContext.get().getStatementContext().addIndexInSqlToString(
                        Pair.of(colCtx.start.getStartIndex(),
                                colCtx.stop.getStopIndex()),
                        Utils.qualifiedNameWithBackquote(ImmutableList.of(colCtx.getText())));
            }
        }
        return super.visitAliasQuery(ctx);
    }

    @Override
    protected LogicalPlan withSelectQuerySpecification(
            ParserRuleContext ctx,
            LogicalPlan inputRelation,
            SelectClauseContext selectClause,
            Optional<WhereClauseContext> whereClause,
            Optional<AggClauseContext> aggClause,
            Optional<HavingClauseContext> havingClause) {
        LogicalPlan plan = super.withSelectQuerySpecification(ctx, inputRelation, selectClause, whereClause,
                aggClause, havingClause);
        SelectColumnClauseContext selectColumnCtx = selectClause.selectColumnClause();
        if ((!aggClause.isPresent() || isRepeat(aggClause.get())) && havingClause.isPresent()
                && selectColumnCtx.EXCEPT() != null
                && plan instanceof LogicalHaving && plan.child(0) instanceof LogicalProject) {
            LogicalHaving<LogicalProject<Plan>> having = (LogicalHaving) plan;
            LogicalProject<Plan> project = having.child();
            UnboundStar star = (UnboundStar) project.getProjects().get(0);
            star = star.withIndexInSql(Pair.of(selectColumnCtx.start.getStartIndex(),
                    selectColumnCtx.stop.getStopIndex()));
            project = project.withProjects(ImmutableList.of(star));
            return (LogicalPlan) plan.withChildren(project);
        } else {
            return plan;
        }
    }

    @Override
    protected LogicalPlan withProjection(LogicalPlan input, SelectColumnClauseContext selectCtx,
            Optional<AggClauseContext> aggCtx, boolean isDistinct) {
        LogicalPlan plan = super.withProjection(input, selectCtx, aggCtx, isDistinct);
        if (!aggCtx.isPresent() && selectCtx.EXCEPT() != null && plan instanceof LogicalProject) {
            LogicalProject<Plan> project = (LogicalProject) plan;
            UnboundStar star = (UnboundStar) project.getProjects().get(0);
            star = star.withIndexInSql(Pair.of(selectCtx.start.getStartIndex(), selectCtx.stop.getStopIndex()));
            return project.withProjects(ImmutableList.of(star));
        } else {
            return plan;
        }
    }

    @Override
    protected LogicalPlan withTableAlias(LogicalPlan plan, TableAliasContext ctx) {
        if (ctx.strictIdentifier() == null) {
            return plan;
        }
        String alias = ctx.strictIdentifier().getText();
        ConnectContext.get().getStatementContext().addIndexInSqlToString(
                Pair.of(ctx.strictIdentifier().start.getStartIndex(),
                        ctx.strictIdentifier().stop.getStopIndex()),
                Utils.qualifiedNameWithBackquote(ImmutableList.of(alias)));
        return super.withTableAlias(plan, ctx);
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        LogicalPlan plan = super.visitTableName(ctx);
        MultipartIdentifierContext identifier = ctx.multipartIdentifier();
        return (LogicalPlan) plan.rewriteDownShortCircuit(node -> {
            if (node instanceof UnboundRelation) {
                UnboundRelation relation = (UnboundRelation) node;
                return relation.withIndexInSql(Pair.of(identifier.start.getStartIndex(),
                        identifier.stop.getStopIndex()));
            }
            return node;
        });
    }

    @Override
    public Expression visitStar(StarContext ctx) {
        Expression expr = super.visitStar(ctx);
        UnboundStar star = (UnboundStar) expr;
        return star.withIndexInSql(Pair.of(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    @Override
    public NamedExpression visitNamedExpression(NamedExpressionContext ctx) {
        if (ctx.identifierOrText() != null) {
            String alias = visitIdentifierOrText(ctx.identifierOrText());
            ConnectContext.get().getStatementContext().addIndexInSqlToString(
                    Pair.of(ctx.identifierOrText().start.getStartIndex(),
                            ctx.identifierOrText().stop.getStopIndex()),
                    Utils.qualifiedNameWithBackquote(ImmutableList.of(alias)));
        }
        return super.visitNamedExpression(ctx);
    }

    @Override
    public Expression visitFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx) {
        Expression expr = super.visitFunctionCallExpression(ctx);
        if (expr instanceof UnboundFunction) {
            UnboundFunction function = (UnboundFunction) expr;
            function = function.withIndexInSql(Pair.of(
                    ctx.functionIdentifier().start.getStartIndex(),
                    ctx.functionIdentifier().stop.getStopIndex()));
            return function;
        } else {
            return expr;
        }
    }

    @Override
    public Expression visitDereference(DereferenceContext ctx) {
        UnboundSlot slot = (UnboundSlot) super.visitDereference(ctx);
        return slot.withIndexInSql(Pair.of(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        Expression expr = super.visitColumnReference(ctx);
        UnboundSlot slot = (UnboundSlot) expr;
        return slot.withIndexInSql(Pair.of(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    private boolean isRepeat(AggClauseContext ctx) {
        GroupingElementContext groupingElementContext = ctx.groupingElement();
        return groupingElementContext.GROUPING() != null || groupingElementContext.CUBE() != null
                || groupingElementContext.ROLLUP() != null;
    }
}
