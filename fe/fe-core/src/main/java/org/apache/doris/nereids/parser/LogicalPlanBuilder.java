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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.AggClauseContext;
import org.apache.doris.nereids.DorisParser.AliasQueryContext;
import org.apache.doris.nereids.DorisParser.AliasedQueryContext;
import org.apache.doris.nereids.DorisParser.ArithmeticBinaryContext;
import org.apache.doris.nereids.DorisParser.ArithmeticUnaryContext;
import org.apache.doris.nereids.DorisParser.BitOperationContext;
import org.apache.doris.nereids.DorisParser.BooleanExpressionContext;
import org.apache.doris.nereids.DorisParser.BooleanLiteralContext;
import org.apache.doris.nereids.DorisParser.BracketJoinHintContext;
import org.apache.doris.nereids.DorisParser.BracketRelationHintContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.CommentJoinHintContext;
import org.apache.doris.nereids.DorisParser.CommentRelationHintContext;
import org.apache.doris.nereids.DorisParser.ComparisonContext;
import org.apache.doris.nereids.DorisParser.ConstantContext;
import org.apache.doris.nereids.DorisParser.CreateRowPolicyContext;
import org.apache.doris.nereids.DorisParser.CteContext;
import org.apache.doris.nereids.DorisParser.Date_addContext;
import org.apache.doris.nereids.DorisParser.Date_subContext;
import org.apache.doris.nereids.DorisParser.DecimalLiteralContext;
import org.apache.doris.nereids.DorisParser.DeleteContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.ExistContext;
import org.apache.doris.nereids.DorisParser.ExplainContext;
import org.apache.doris.nereids.DorisParser.FromClauseContext;
import org.apache.doris.nereids.DorisParser.GroupingElementContext;
import org.apache.doris.nereids.DorisParser.GroupingSetContext;
import org.apache.doris.nereids.DorisParser.HavingClauseContext;
import org.apache.doris.nereids.DorisParser.HintAssignmentContext;
import org.apache.doris.nereids.DorisParser.HintStatementContext;
import org.apache.doris.nereids.DorisParser.IdentifierListContext;
import org.apache.doris.nereids.DorisParser.IdentifierOrTextContext;
import org.apache.doris.nereids.DorisParser.IdentifierSeqContext;
import org.apache.doris.nereids.DorisParser.InsertIntoQueryContext;
import org.apache.doris.nereids.DorisParser.IntegerLiteralContext;
import org.apache.doris.nereids.DorisParser.IntervalContext;
import org.apache.doris.nereids.DorisParser.Is_not_null_predContext;
import org.apache.doris.nereids.DorisParser.IsnullContext;
import org.apache.doris.nereids.DorisParser.JoinCriteriaContext;
import org.apache.doris.nereids.DorisParser.JoinRelationContext;
import org.apache.doris.nereids.DorisParser.LateralViewContext;
import org.apache.doris.nereids.DorisParser.LimitClauseContext;
import org.apache.doris.nereids.DorisParser.LogicalBinaryContext;
import org.apache.doris.nereids.DorisParser.LogicalNotContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParser.NullLiteralContext;
import org.apache.doris.nereids.DorisParser.OutFileClauseContext;
import org.apache.doris.nereids.DorisParser.ParenthesizedExpressionContext;
import org.apache.doris.nereids.DorisParser.PlanTypeContext;
import org.apache.doris.nereids.DorisParser.PredicateContext;
import org.apache.doris.nereids.DorisParser.PredicatedContext;
import org.apache.doris.nereids.DorisParser.PrimitiveDataTypeContext;
import org.apache.doris.nereids.DorisParser.QualifiedNameContext;
import org.apache.doris.nereids.DorisParser.QueryContext;
import org.apache.doris.nereids.DorisParser.QueryOrganizationContext;
import org.apache.doris.nereids.DorisParser.QueryTermContext;
import org.apache.doris.nereids.DorisParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.DorisParser.RelationContext;
import org.apache.doris.nereids.DorisParser.SampleByPercentileContext;
import org.apache.doris.nereids.DorisParser.SampleByRowsContext;
import org.apache.doris.nereids.DorisParser.SampleContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SelectColumnClauseContext;
import org.apache.doris.nereids.DorisParser.SelectHintContext;
import org.apache.doris.nereids.DorisParser.SetOperationContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.DorisParser.SortClauseContext;
import org.apache.doris.nereids.DorisParser.SortItemContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.StatementDefaultContext;
import org.apache.doris.nereids.DorisParser.StringLiteralContext;
import org.apache.doris.nereids.DorisParser.SubqueryContext;
import org.apache.doris.nereids.DorisParser.SubqueryExpressionContext;
import org.apache.doris.nereids.DorisParser.SystemVariableContext;
import org.apache.doris.nereids.DorisParser.TableAliasContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.DorisParser.TableValuedFunctionContext;
import org.apache.doris.nereids.DorisParser.TimestampaddContext;
import org.apache.doris.nereids.DorisParser.TimestampdiffContext;
import org.apache.doris.nereids.DorisParser.TvfPropertyContext;
import org.apache.doris.nereids.DorisParser.TvfPropertyItemContext;
import org.apache.doris.nereids.DorisParser.TypeConstructorContext;
import org.apache.doris.nereids.DorisParser.UnitIdentifierContext;
import org.apache.doris.nereids.DorisParser.UpdateAssignmentContext;
import org.apache.doris.nereids.DorisParser.UpdateAssignmentSeqContext;
import org.apache.doris.nereids.DorisParser.UpdateContext;
import org.apache.doris.nereids.DorisParser.UserIdentifyContext;
import org.apache.doris.nereids.DorisParser.UserVariableContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.DorisParser.WindowFrameContext;
import org.apache.doris.nereids.DorisParser.WindowSpecContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Regexp;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TVFProperties;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsSub;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.UsingJoin;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Build a logical plan tree with unbounded nodes.
 */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "OptionalGetWithoutIsPresent"})
public class LogicalPlanBuilder extends DorisParserBaseVisitor<Object> {

    @SuppressWarnings("unchecked")
    protected <T> T typedVisit(ParseTree ctx) {
        return (T) ctx.accept(this);
    }

    /**
     * Override the default behavior for all visit methods. This will only return a non-null result
     * when the context has only one child. This is done because there is no generic method to
     * combine the results of the context children. In all other cases null is returned.
     */
    @Override
    public Object visitChildren(RuleNode node) {
        if (node.getChildCount() == 1) {
            return node.getChild(0).accept(this);
        } else {
            return null;
        }
    }

    @Override
    public LogicalPlan visitSingleStatement(SingleStatementContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> (LogicalPlan) visit(ctx.statement()));
    }

    @Override
    public LogicalPlan visitStatementDefault(StatementDefaultContext ctx) {
        LogicalPlan plan = plan(ctx.query());
        if (ctx.outFileClause() != null) {
            plan = withOutFile(plan, ctx.outFileClause());
        } else {
            plan = new UnboundResultSink<>(plan);
        }
        return withExplain(plan, ctx.explain());
    }

    @Override
    public LogicalPlan visitInsertIntoQuery(InsertIntoQueryContext ctx) {
        List<String> tableName = visitMultipartIdentifier(ctx.tableName);
        String labelName = ctx.labelName == null ? null : ctx.labelName.getText();
        List<String> colNames = ctx.cols == null ? ImmutableList.of() : visitIdentifierList(ctx.cols);
        List<String> partitions = ctx.partition == null ? ImmutableList.of() : visitIdentifierList(ctx.partition);
        UnboundOlapTableSink<?> sink = new UnboundOlapTableSink<>(
                tableName,
                colNames,
                ImmutableList.of(),
                partitions,
                ConnectContext.get().getSessionVariable().isEnableUniqueKeyPartialUpdate(),
                true,
                visitQuery(ctx.query()));
        if (ctx.explain() != null) {
            return withExplain(sink, ctx.explain());
        }
        return new InsertIntoTableCommand(sink, Optional.ofNullable(labelName));
    }

    @Override
    public LogicalPlan visitUpdate(UpdateContext ctx) {
        LogicalPlan query = withCheckPolicy(new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(), visitMultipartIdentifier(ctx.tableName)));
        query = withTableAlias(query, ctx.tableAlias());
        if (ctx.fromClause() != null) {
            query = withRelations(query, ctx.fromClause().relation());
        }
        query = withFilter(query, Optional.of(ctx.whereClause()));
        String tableAlias = null;
        if (ctx.tableAlias().strictIdentifier() != null) {
            tableAlias = ctx.tableAlias().getText();
        }
        Optional<LogicalPlan> cte = Optional.empty();
        if (ctx.cte() != null) {
            cte = Optional.ofNullable(withCte(query, ctx.cte()));
        }
        return withExplain(new UpdateCommand(visitMultipartIdentifier(ctx.tableName), tableAlias,
                visitUpdateAssignmentSeq(ctx.updateAssignmentSeq()), query, cte), ctx.explain());
    }

    @Override
    public LogicalPlan visitDelete(DeleteContext ctx) {
        List<String> tableName = visitMultipartIdentifier(ctx.tableName);
        List<String> partitions = ctx.partition == null ? ImmutableList.of() : visitIdentifierList(ctx.partition);
        LogicalPlan query = withTableAlias(withCheckPolicy(
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), tableName)), ctx.tableAlias());
        if (ctx.USING() != null) {
            query = withRelations(query, ctx.relation());
        }
        query = withFilter(query, Optional.of(ctx.whereClause()));
        String tableAlias = null;
        if (ctx.tableAlias().strictIdentifier() != null) {
            tableAlias = ctx.tableAlias().getText();
        }
        Optional<LogicalPlan> cte = Optional.empty();
        if (ctx.cte() != null) {
            cte = Optional.ofNullable(withCte(query, ctx.cte()));
        }
        return withExplain(new DeleteCommand(tableName, tableAlias, partitions, query, cte), ctx.explain());
    }

    /**
     * Visit multi-statements.
     */
    @Override
    public List<Pair<LogicalPlan, StatementContext>> visitMultiStatements(MultiStatementsContext ctx) {
        List<Pair<LogicalPlan, StatementContext>> logicalPlans = Lists.newArrayList();
        for (org.apache.doris.nereids.DorisParser.StatementContext statement : ctx.statement()) {
            StatementContext statementContext = new StatementContext();
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                connectContext.setStatementContext(statementContext);
                statementContext.setConnectContext(connectContext);
            }
            logicalPlans.add(Pair.of(
                    ParserUtils.withOrigin(ctx, () -> (LogicalPlan) visit(statement)), statementContext));
        }
        return logicalPlans;
    }

    /* ********************************************************************************************
     * Plan parsing
     * ******************************************************************************************** */

    /**
     * process lateral view, add a {@link org.apache.doris.nereids.trees.plans.logical.LogicalGenerate} on plan.
     */
    private LogicalPlan withGenerate(LogicalPlan plan, LateralViewContext ctx) {
        if (ctx.LATERAL() == null) {
            return plan;
        }
        String generateName = ctx.tableName.getText();
        String columnName = ctx.columnName.getText();
        String functionName = ctx.functionName.getText();
        List<Expression> arguments = ctx.expression().stream()
                .<Expression>map(this::typedVisit)
                .collect(ImmutableList.toImmutableList());
        Function unboundFunction = new UnboundFunction(functionName, arguments);
        return new LogicalGenerate<>(ImmutableList.of(unboundFunction),
                ImmutableList.of(new UnboundSlot(generateName, columnName)), plan);
    }

    /**
     * process CTE and store the results in a logical plan node LogicalCTE
     */
    private LogicalPlan withCte(LogicalPlan plan, CteContext ctx) {
        if (ctx == null) {
            return plan;
        }
        return new LogicalCTE<>((List) visit(ctx.aliasQuery(), LogicalSubQueryAlias.class), plan);
    }

    /**
     * process CTE's alias queries and column aliases
     */
    @Override
    public LogicalSubQueryAlias<Plan> visitAliasQuery(AliasQueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            LogicalPlan queryPlan = plan(ctx.query());
            Optional<List<String>> columnNames = optionalVisit(ctx.columnAliases(), () ->
                    ctx.columnAliases().identifier().stream()
                    .map(RuleContext::getText)
                    .collect(ImmutableList.toImmutableList())
            );
            return new LogicalSubQueryAlias<>(ctx.identifier().getText(), columnNames, queryPlan);
        });
    }

    @Override
    public Command visitCreateRowPolicy(CreateRowPolicyContext ctx) {
        FilterType filterType = FilterType.of(ctx.type.getText());
        List<String> nameParts = visitMultipartIdentifier(ctx.table);
        return new CreatePolicyCommand(PolicyTypeEnum.ROW, ctx.name.getText(),
                ctx.EXISTS() != null, nameParts, Optional.of(filterType),
                ctx.user == null ? null : visitUserIdentify(ctx.user),
                ctx.roleName == null ? null : ctx.roleName.getText(),
                Optional.of(getExpression(ctx.booleanExpression())), ImmutableMap.of());
    }

    @Override
    public String visitIdentifierOrText(IdentifierOrTextContext ctx) {
        if (ctx.STRING_LITERAL() != null) {
            return ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1);
        } else {
            return ctx.errorCapturingIdentifier().getText();
        }
    }

    @Override
    public UserIdentity visitUserIdentify(UserIdentifyContext ctx) {
        String user = visitIdentifierOrText(ctx.user);
        String host = null;
        if (ctx.host != null) {
            host = visitIdentifierOrText(ctx.host);
        }
        if (host == null) {
            host = "%";
        }
        boolean isDomain = ctx.LEFT_PAREN() != null;
        return new UserIdentity(user, host, isDomain);
    }

    @Override
    public LogicalPlan visitQuery(QueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // TODO: need to add withQueryResultClauses and withCTE
            LogicalPlan query = plan(ctx.queryTerm());
            query = withCte(query, ctx.cte());
            return withQueryOrganization(query, ctx.queryOrganization());
        });
    }

    @Override
    public LogicalPlan visitSetOperation(SetOperationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {

            if (ctx.UNION() != null) {
                Qualifier qualifier = getQualifier(ctx);
                List<QueryTermContext> contexts = Lists.newArrayList(ctx.right);
                QueryTermContext current = ctx.left;
                while (true) {
                    if (current instanceof SetOperationContext
                            && getQualifier((SetOperationContext) current) == qualifier
                            && ((SetOperationContext) current).UNION() != null) {
                        contexts.add(((SetOperationContext) current).right);
                        current = ((SetOperationContext) current).left;
                    } else {
                        contexts.add(current);
                        break;
                    }
                }
                Collections.reverse(contexts);
                List<LogicalPlan> logicalPlans = contexts.stream().map(this::plan).collect(Collectors.toList());
                return reduceToLogicalPlanTree(0, logicalPlans.size() - 1, logicalPlans, qualifier);
            } else {
                LogicalPlan leftQuery = plan(ctx.left);
                LogicalPlan rightQuery = plan(ctx.right);
                Qualifier qualifier = getQualifier(ctx);

                List<Plan> newChildren = ImmutableList.of(leftQuery, rightQuery);
                LogicalPlan plan;
                if (ctx.UNION() != null) {
                    plan = new LogicalUnion(qualifier, newChildren);
                } else if (ctx.EXCEPT() != null) {
                    plan = new LogicalExcept(qualifier, newChildren);
                } else if (ctx.INTERSECT() != null) {
                    plan = new LogicalIntersect(qualifier, newChildren);
                } else {
                    throw new ParseException("not support", ctx);
                }
                return plan;
            }
        });
    }

    private Qualifier getQualifier(SetOperationContext ctx) {
        if (ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null) {
            return Qualifier.DISTINCT;
        } else {
            return Qualifier.ALL;
        }
    }

    private LogicalPlan logicalPlanCombiner(LogicalPlan left, LogicalPlan right, Qualifier qualifier) {
        return new LogicalUnion(qualifier, ImmutableList.of(left, right));
    }

    private LogicalPlan reduceToLogicalPlanTree(int low, int high,
            List<LogicalPlan> logicalPlans, Qualifier qualifier) {
        switch (high - low) {
            case 0:
                return logicalPlans.get(low);
            case 1:
                return logicalPlanCombiner(logicalPlans.get(low), logicalPlans.get(high), qualifier);
            default:
                int mid = low + (high - low) / 2;
                return logicalPlanCombiner(
                        reduceToLogicalPlanTree(low, mid, logicalPlans, qualifier),
                        reduceToLogicalPlanTree(mid + 1, high, logicalPlans, qualifier),
                        qualifier
                );
        }
    }

    @Override
    public LogicalPlan visitSubquery(SubqueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> plan(ctx.query()));
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(RegularQuerySpecificationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            SelectClauseContext selectCtx = ctx.selectClause();
            LogicalPlan selectPlan;
            if (ctx.fromClause() == null) {
                SelectColumnClauseContext columnCtx = selectCtx.selectColumnClause();
                if (columnCtx.EXCEPT() != null) {
                    throw new ParseException("select-except cannot be used in one row relation", selectCtx);
                }
                selectPlan = withOneRowRelation(columnCtx);
            } else {
                LogicalPlan relation = visitFromClause(ctx.fromClause());
                selectPlan = withSelectQuerySpecification(
                        ctx, relation,
                        selectCtx,
                        Optional.ofNullable(ctx.whereClause()),
                        Optional.ofNullable(ctx.aggClause()),
                        Optional.ofNullable(ctx.havingClause())
                );
            }
            selectPlan = withQueryOrganization(selectPlan, ctx.queryOrganization());
            return withSelectHint(selectPlan, selectCtx.selectHint());
        });
    }

    /**
     * Create an aliased table reference. This is typically used in FROM clauses.
     */
    private LogicalPlan withTableAlias(LogicalPlan plan, TableAliasContext ctx) {
        if (ctx.strictIdentifier() == null) {
            return plan;
        }
        return ParserUtils.withOrigin(ctx.strictIdentifier(), () -> {
            String alias = ctx.strictIdentifier().getText();
            if (null != ctx.identifierList()) {
                throw new ParseException("Do not implemented", ctx);
                // TODO: multi-colName
            }
            return new LogicalSubQueryAlias<>(alias, plan);
        });
    }

    private LogicalPlan withCheckPolicy(LogicalPlan plan) {
        return new LogicalCheckPolicy<>(plan);
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        List<String> tableId = visitMultipartIdentifier(ctx.multipartIdentifier());
        List<String> partitionNames = new ArrayList<>();
        boolean isTempPart = false;
        if (ctx.specifiedPartition() != null) {
            isTempPart = ctx.specifiedPartition().TEMPORARY() != null;
            if (ctx.specifiedPartition().identifier() != null) {
                partitionNames.add(ctx.specifiedPartition().identifier().getText());
            } else {
                partitionNames.addAll(visitIdentifierList(ctx.specifiedPartition().identifierList()));
            }
        }

        final List<String> relationHints;
        if (ctx.relationHint() != null) {
            relationHints = typedVisit(ctx.relationHint());
        } else {
            relationHints = ImmutableList.of();
        }

        TableSample tableSample = ctx.sample() == null ? null : (TableSample) visit(ctx.sample());
        LogicalPlan checkedRelation = withCheckPolicy(
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                        tableId, partitionNames, isTempPart, relationHints,
                        Optional.ofNullable(tableSample)));
        LogicalPlan plan = withTableAlias(checkedRelation, ctx.tableAlias());
        for (LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    @Override
    public LogicalPlan visitAliasedQuery(AliasedQueryContext ctx) {
        if (ctx.tableAlias().getText().equals("")) {
            throw new ParseException("Every derived table must have its own alias", ctx);
        }
        LogicalPlan plan = withTableAlias(visitQuery(ctx.query()), ctx.tableAlias());
        for (LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    @Override
    public LogicalPlan visitTableValuedFunction(TableValuedFunctionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.tvfName.getText();

            Builder<String, String> map = ImmutableMap.builder();
            for (TvfPropertyContext argument : ctx.properties) {
                String key = parseTVFPropertyItem(argument.key);
                String value = parseTVFPropertyItem(argument.value);
                map.put(key, value);
            }
            LogicalPlan relation = new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(),
                    functionName, new TVFProperties(map.build()));
            return withTableAlias(relation, ctx.tableAlias());
        });
    }

    /**
     * Create a star (i.e. all) expression; this selects all elements (in the specified object).
     * Both un-targeted (global) and targeted aliases are supported.
     */
    @Override
    public Expression visitStar(StarContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            final QualifiedNameContext qualifiedNameContext = ctx.qualifiedName();
            List<String> target;
            if (qualifiedNameContext != null) {
                target = qualifiedNameContext.identifier()
                        .stream()
                        .map(RuleContext::getText)
                        .collect(ImmutableList.toImmutableList());
            } else {
                target = ImmutableList.of();
            }
            return new UnboundStar(target);
        });
    }

    /**
     * Create an aliased expression if an alias is specified. Both single and multi-aliases are
     * supported.
     */
    @Override
    public Expression visitNamedExpression(NamedExpressionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression expression = getExpression(ctx.expression());
            if (ctx.identifierOrText() == null) {
                return expression;
            }
            String alias = visitIdentifierOrText(ctx.identifierOrText());
            return new UnboundAlias(expression, alias);
        });
    }

    @Override
    public Expression visitSystemVariable(SystemVariableContext ctx) {
        VariableType type = null;
        if (ctx.kind == null) {
            type = VariableType.DEFAULT;
        } else if (ctx.kind.getType() == DorisParser.SESSION) {
            type = VariableType.SESSION;
        } else if (ctx.kind.getType() == DorisParser.GLOBAL) {
            type = VariableType.GLOBAL;
        }
        if (type == null) {
            throw new ParseException("Unsupported system variable: " + ctx.getText(), ctx);
        }
        return new UnboundVariable(ctx.identifier().getText(), type);
    }

    @Override
    public Expression visitUserVariable(UserVariableContext ctx) {
        return new UnboundVariable(ctx.identifierOrText().getText(), VariableType.USER);
    }

    /**
     * Create a comparison expression. This compares two expressions. The following comparison
     * operators are supported:
     * - Equal: '=' or '=='
     * - Null-safe Equal: '<=>'
     * - Not Equal: '<>' or '!='
     * - Less than: '<'
     * - Less then or Equal: '<='
     * - Greater than: '>'
     * - Greater then or Equal: '>='
     */
    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            TerminalNode operator = (TerminalNode) ctx.comparisonOperator().getChild(0);
            switch (operator.getSymbol().getType()) {
                case DorisParser.EQ:
                    return new EqualTo(left, right);
                case DorisParser.NEQ:
                    return new Not(new EqualTo(left, right));
                case DorisParser.LT:
                    return new LessThan(left, right);
                case DorisParser.GT:
                    return new GreaterThan(left, right);
                case DorisParser.LTE:
                    return new LessThanEqual(left, right);
                case DorisParser.GTE:
                    return new GreaterThanEqual(left, right);
                case DorisParser.NSEQ:
                    return new NullSafeEqual(left, right);
                default:
                    throw new ParseException("Unsupported comparison expression: "
                        + operator.getSymbol().getText(), ctx);
            }
        });
    }

    /**
     * Create a not expression.
     * format: NOT Expression
     * for example:
     * not 1
     * not 1=1
     */
    @Override
    public Expression visitLogicalNot(LogicalNotContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> new Not(getExpression(ctx.booleanExpression())));
    }

    @Override
    public Expression visitLogicalBinary(LogicalBinaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // Code block copy from Spark
            // sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala

            // Collect all similar left hand contexts.
            List<BooleanExpressionContext> contexts = Lists.newArrayList(ctx.right);
            BooleanExpressionContext current = ctx.left;
            while (true) {
                if (current instanceof LogicalBinaryContext
                        && ((LogicalBinaryContext) current).operator.getType() == ctx.operator.getType()) {
                    contexts.add(((LogicalBinaryContext) current).right);
                    current = ((LogicalBinaryContext) current).left;
                } else {
                    contexts.add(current);
                    break;
                }
            }
            // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
            // into expressions.
            Collections.reverse(contexts);
            List<Expression> expressions = contexts.stream().map(this::getExpression).collect(Collectors.toList());
            // Create a balanced tree.
            return reduceToExpressionTree(0, expressions.size() - 1, expressions, ctx);
        });
    }

    private Expression expressionCombiner(Expression left, Expression right, LogicalBinaryContext ctx) {
        switch (ctx.operator.getType()) {
            case DorisParser.LOGICALAND:
            case DorisParser.AND:
                return new And(left, right);
            case DorisParser.OR:
                return new Or(left, right);
            default:
                throw new ParseException("Unsupported logical binary type: " + ctx.operator.getText(), ctx);
        }
    }

    private Expression reduceToExpressionTree(int low, int high,
            List<Expression> expressions, LogicalBinaryContext ctx) {
        switch (high - low) {
            case 0:
                return expressions.get(low);
            case 1:
                return expressionCombiner(expressions.get(low), expressions.get(high), ctx);
            default:
                int mid = low + (high - low) / 2;
                return expressionCombiner(
                        reduceToExpressionTree(low, mid, expressions, ctx),
                        reduceToExpressionTree(mid + 1, high, expressions, ctx),
                        ctx
                );
        }
    }

    /**
     * Create a predicated expression. A predicated expression is a normal expression with a
     * predicate attached to it, for example:
     * {{{
     * a + 1 IS NULL
     * }}}
     */
    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = getExpression(ctx.valueExpression());
            return ctx.predicate() == null ? e : withPredicate(e, ctx.predicate());
        });
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = typedVisit(ctx.valueExpression());
            switch (ctx.operator.getType()) {
                case DorisParser.PLUS:
                    return e;
                case DorisParser.SUBTRACT:
                    IntegerLiteral zero = new IntegerLiteral(0);
                    return new Subtract(zero, e);
                case DorisParser.TILDE:
                    return new BitNot(e);
                default:
                    throw new ParseException("Unsupported arithmetic unary type: " + ctx.operator.getText(), ctx);
            }
        });
    }

    @Override
    public Expression visitBitOperation(BitOperationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            if (ctx.operator.getType() == DorisParser.BITAND) {
                return new BitAnd(left, right);
            } else if (ctx.operator.getType() == DorisParser.BITOR) {
                return new BitOr(left, right);
            } else if (ctx.operator.getType() == DorisParser.BITXOR) {
                return new BitXor(left, right);
            }
            throw new ParseException(" not supported", ctx);
        });
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);

            int type = ctx.operator.getType();
            if (left instanceof Interval) {
                if (type != DorisParser.PLUS) {
                    throw new ParseException("Only supported: " + Operator.ADD, ctx);
                }
                Interval interval = (Interval) left;
                return new TimestampArithmetic(Operator.ADD, right, interval.value(), interval.timeUnit(), true);
            }

            if (right instanceof Interval) {
                Operator op;
                if (type == DorisParser.PLUS) {
                    op = Operator.ADD;
                } else if (type == DorisParser.SUBTRACT) {
                    op = Operator.SUBTRACT;
                } else {
                    throw new ParseException("Only supported: " + Operator.ADD + " and " + Operator.SUBTRACT, ctx);
                }
                Interval interval = (Interval) right;
                return new TimestampArithmetic(op, left, interval.value(), interval.timeUnit(), false);
            }

            return ParserUtils.withOrigin(ctx, () -> {
                switch (type) {
                    case DorisParser.ASTERISK:
                        return new Multiply(left, right);
                    case DorisParser.SLASH:
                        return new Divide(left, right);
                    case DorisParser.MOD:
                        return new Mod(left, right);
                    case DorisParser.PLUS:
                        return new Add(left, right);
                    case DorisParser.SUBTRACT:
                        return new Subtract(left, right);
                    case DorisParser.DIV:
                        return new IntegralDivide(left, right);
                    case DorisParser.HAT:
                        return new BitXor(left, right);
                    case DorisParser.PIPE:
                        return new BitOr(left, right);
                    case DorisParser.AMPERSAND:
                        return new BitAnd(left, right);
                    default:
                        throw new ParseException(
                                "Unsupported arithmetic binary type: " + ctx.operator.getText(), ctx);
                }
            });
        });
    }

    @Override
    public Expression visitTimestampdiff(TimestampdiffContext ctx) {
        Expression start = (Expression) visit(ctx.startTimestamp);
        Expression end = (Expression) visit(ctx.endTimestamp);
        String unit = ctx.unit.getText();
        if ("YEAR".equalsIgnoreCase(unit)) {
            return new YearsDiff(end, start);
        } else if ("MONTH".equalsIgnoreCase(unit)) {
            return new MonthsDiff(end, start);
        } else if ("WEEK".equalsIgnoreCase(unit)) {
            return new WeeksDiff(end, start);
        } else if ("DAY".equalsIgnoreCase(unit)) {
            return new DaysDiff(end, start);
        } else if ("HOUR".equalsIgnoreCase(unit)) {
            return new HoursDiff(end, start);
        } else if ("MINUTE".equalsIgnoreCase(unit)) {
            return new MinutesDiff(end, start);
        } else if ("SECOND".equalsIgnoreCase(unit)) {
            return new SecondsDiff(end, start);
        }
        throw new ParseException("Unsupported time stamp diff time unit: " + unit
                + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND", ctx);

    }

    @Override
    public Expression visitTimestampadd(TimestampaddContext ctx) {
        Expression start = (Expression) visit(ctx.startTimestamp);
        Expression end = (Expression) visit(ctx.endTimestamp);
        String unit = ctx.unit.getText();
        if ("YEAR".equalsIgnoreCase(unit)) {
            return new YearsAdd(end, start);
        } else if ("MONTH".equalsIgnoreCase(unit)) {
            return new MonthsAdd(end, start);
        } else if ("WEEK".equalsIgnoreCase(unit)) {
            return new WeeksAdd(end, start);
        } else if ("DAY".equalsIgnoreCase(unit)) {
            return new DaysAdd(end, start);
        } else if ("HOUR".equalsIgnoreCase(unit)) {
            return new HoursAdd(end, start);
        } else if ("MINUTE".equalsIgnoreCase(unit)) {
            return new MinutesAdd(end, start);
        } else if ("SECOND".equalsIgnoreCase(unit)) {
            return new SecondsAdd(end, start);
        }
        throw new ParseException("Unsupported time stamp add time unit: " + unit
                + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND", ctx);

    }

    @Override
    public Expression visitDate_add(Date_addContext ctx) {
        Expression timeStamp = (Expression) visit(ctx.timestamp);
        Expression amount = (Expression) visit(ctx.unitsAmount);
        if (ctx.unit == null) {
            //use "DAY" as unit by default
            return new DaysAdd(timeStamp, amount);
        }

        if ("Year".equalsIgnoreCase(ctx.unit.getText())) {
            return new YearsAdd(timeStamp, amount);
        } else if ("MONTH".equalsIgnoreCase(ctx.unit.getText())) {
            return new MonthsAdd(timeStamp, amount);
        } else if ("WEEK".equalsIgnoreCase(ctx.unit.getText())) {
            return new WeeksAdd(timeStamp, amount);
        } else if ("DAY".equalsIgnoreCase(ctx.unit.getText())) {
            return new DaysAdd(timeStamp, amount);
        } else if ("Hour".equalsIgnoreCase(ctx.unit.getText())) {
            return new HoursAdd(timeStamp, amount);
        } else if ("Minute".equalsIgnoreCase(ctx.unit.getText())) {
            return new MinutesAdd(timeStamp, amount);
        } else if ("Second".equalsIgnoreCase(ctx.unit.getText())) {
            return new SecondsAdd(timeStamp, amount);
        }
        throw new ParseException("Unsupported time unit: " + ctx.unit
                + ", supported time unit: YEAR/MONTH/DAY/HOUR/MINUTE/SECOND", ctx);
    }

    @Override
    public Expression visitDate_sub(Date_subContext ctx) {
        Expression timeStamp = (Expression) visit(ctx.timestamp);
        Expression amount = (Expression) visit(ctx.unitsAmount);
        if (ctx.unit == null) {
            //use "DAY" as unit by default
            return new DaysSub(timeStamp, amount);
        }

        if ("Year".equalsIgnoreCase(ctx.unit.getText())) {
            return new YearsSub(timeStamp, amount);
        } else if ("MONTH".equalsIgnoreCase(ctx.unit.getText())) {
            return new MonthsSub(timeStamp, amount);
        } else if ("WEEK".equalsIgnoreCase(ctx.unit.getText())) {
            return new WeeksSub(timeStamp, amount);
        } else if ("DAY".equalsIgnoreCase(ctx.unit.getText())) {
            return new DaysSub(timeStamp, amount);
        } else if ("Hour".equalsIgnoreCase(ctx.unit.getText())) {
            return new HoursSub(timeStamp, amount);
        } else if ("Minute".equalsIgnoreCase(ctx.unit.getText())) {
            return new MinutesSub(timeStamp, amount);
        } else if ("Second".equalsIgnoreCase(ctx.unit.getText())) {
            return new SecondsSub(timeStamp, amount);
        }
        throw new ParseException("Unsupported time unit: " + ctx.unit
                + ", supported time unit: YEAR/MONTH/DAY/HOUR/MINUTE/SECOND", ctx);
    }

    @Override
    public Expression visitDoublePipes(DorisParser.DoublePipesContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            if (ConnectContext.get().getSessionVariable().getSqlMode() == SqlModeHelper.MODE_PIPES_AS_CONCAT) {
                return new UnboundFunction("concat", Lists.newArrayList(left, right));
            } else {
                return new Or(left, right);
            }
        });
    }

    /**
     * Create a value based [[CaseWhen]] expression. This has the following SQL form:
     * {{{
     *   CASE [expression]
     *    WHEN [value] THEN [expression]
     *    ...
     *    ELSE [expression]
     *   END
     * }}}
     */
    @Override
    public Expression visitSimpleCase(DorisParser.SimpleCaseContext context) {
        Expression e = getExpression(context.value);
        List<WhenClause> whenClauses = context.whenClause().stream()
                .map(w -> new WhenClause(new EqualTo(e, getExpression(w.condition)), getExpression(w.result)))
                .collect(ImmutableList.toImmutableList());
        if (context.elseExpression == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, getExpression(context.elseExpression));
    }

    /**
     * Create a condition based [[CaseWhen]] expression. This has the following SQL syntax:
     * {{{
     *   CASE
     *    WHEN [predicate] THEN [expression]
     *    ...
     *    ELSE [expression]
     *   END
     * }}}
     *
     * @param context the parse tree
     */
    @Override
    public Expression visitSearchedCase(DorisParser.SearchedCaseContext context) {
        List<WhenClause> whenClauses = context.whenClause().stream()
                .map(w -> new WhenClause(getExpression(w.condition), getExpression(w.result)))
                .collect(ImmutableList.toImmutableList());
        if (context.elseExpression == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, getExpression(context.elseExpression));
    }

    @Override
    public Expression visitCast(DorisParser.CastContext ctx) {
        List<String> types = typedVisit(ctx.dataType());
        DataType dataType = DataType.convertPrimitiveFromStrings(types, true);
        Expression cast = ParserUtils.withOrigin(ctx, () ->
                new Cast(getExpression(ctx.expression()), dataType, true));
        if (dataType.isStringLikeType() && ((CharacterType) dataType).getLen() >= 0) {
            List<Expression> args = ImmutableList.of(
                    cast,
                    new TinyIntLiteral((byte) 1),
                    Literal.of(((CharacterType) dataType).getLen())
            );
            return new UnboundFunction("substr", args);
        } else {
            return cast;
        }
    }

    @Override
    public UnboundFunction visitExtract(DorisParser.ExtractContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.field.getText();
            return new UnboundFunction(functionName, false,
                    Collections.singletonList(getExpression(ctx.source)));
        });
    }

    @Override
    public Expression visitFunctionCall(DorisParser.FunctionCallContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.functionIdentifier().functionNameIdentifier().getText();
            boolean isDistinct = ctx.DISTINCT() != null;
            List<Expression> params = visit(ctx.expression(), Expression.class);

            List<OrderKey> orderKeys = visit(ctx.sortItem(), OrderKey.class);
            if (!orderKeys.isEmpty()) {
                return parseFunctionWithOrderKeys(functionName, isDistinct, params, orderKeys, ctx);
            }

            List<UnboundStar> unboundStars = ExpressionUtils.collectAll(params, UnboundStar.class::isInstance);
            if (unboundStars.size() > 0) {
                if (functionName.equalsIgnoreCase("count")) {
                    if (unboundStars.size() > 1) {
                        throw new ParseException(
                                "'*' can only be used once in conjunction with COUNT: " + functionName, ctx);
                    }
                    if (!unboundStars.get(0).getQualifier().isEmpty()) {
                        throw new ParseException("'*' can not has qualifier: " + unboundStars.size(), ctx);
                    }
                    if (ctx.windowSpec() != null) {
                        // todo: support count(*) as window function
                        throw new ParseException(
                                "COUNT(*) isn't supported as window function; can use COUNT(col)", ctx);
                    }
                    return new Count();
                }
                throw new ParseException("'*' can only be used in conjunction with COUNT: " + functionName, ctx);
            } else {
                String dbName = null;
                if (ctx.functionIdentifier().dbName != null) {
                    dbName = ctx.functionIdentifier().dbName.getText();
                }
                UnboundFunction function = new UnboundFunction(dbName, functionName, isDistinct, params);
                if (ctx.windowSpec() != null) {
                    if (isDistinct) {
                        throw new ParseException("DISTINCT not allowed in analytic function: " + functionName, ctx);
                    }
                    return withWindowSpec(ctx.windowSpec(), function);
                }
                return function;
            }
        });
    }

    /**
     * deal with window function definition
     */
    private WindowExpression withWindowSpec(WindowSpecContext ctx, Expression function) {
        List<Expression> partitionKeyList = Lists.newArrayList();
        if (ctx.partitionClause() != null) {
            partitionKeyList = visit(ctx.partitionClause().expression(), Expression.class);
        }

        List<OrderExpression> orderKeyList = Lists.newArrayList();
        if (ctx.sortClause() != null) {
            orderKeyList = visit(ctx.sortClause().sortItem(), OrderKey.class).stream()
                .map(orderKey -> new OrderExpression(orderKey))
                .collect(Collectors.toList());
        }

        if (ctx.windowFrame() != null) {
            return new WindowExpression(function, partitionKeyList, orderKeyList, withWindowFrame(ctx.windowFrame()));
        }
        return new WindowExpression(function, partitionKeyList, orderKeyList);
    }

    /**
     * deal with optional expressions
     */
    private <T, C> Optional<C> optionalVisit(T ctx, Supplier<C> func) {
        return Optional.ofNullable(ctx).map(a -> func.get());
    }

    /**
     * deal with window frame
     */
    private WindowFrame withWindowFrame(WindowFrameContext ctx) {
        WindowFrame.FrameUnitsType frameUnitsType = WindowFrame.FrameUnitsType.valueOf(
                ctx.frameUnits().getText().toUpperCase());
        WindowFrame.FrameBoundary leftBoundary = withFrameBound(ctx.start);
        if (ctx.end != null) {
            WindowFrame.FrameBoundary rightBoundary = withFrameBound(ctx.end);
            return new WindowFrame(frameUnitsType, leftBoundary, rightBoundary);
        }
        return new WindowFrame(frameUnitsType, leftBoundary);
    }

    private WindowFrame.FrameBoundary withFrameBound(DorisParser.FrameBoundaryContext ctx) {
        Optional<Expression> expression = Optional.empty();
        if (ctx.expression() != null) {
            expression = Optional.of(getExpression(ctx.expression()));
            // todo: use isConstant() to resolve Function in expression; currently we only
            //  support literal expression
            if (!expression.get().isLiteral()) {
                throw new ParseException("Unsupported expression in WindowFrame : " + expression, ctx);
            }
        }

        WindowFrame.FrameBoundType frameBoundType = null;
        switch (ctx.boundType.getType()) {
            case DorisParser.PRECEDING:
                if (ctx.UNBOUNDED() != null) {
                    frameBoundType = WindowFrame.FrameBoundType.UNBOUNDED_PRECEDING;
                } else {
                    frameBoundType = WindowFrame.FrameBoundType.PRECEDING;
                }
                break;
            case DorisParser.CURRENT:
                frameBoundType = WindowFrame.FrameBoundType.CURRENT_ROW;
                break;
            case DorisParser.FOLLOWING:
                if (ctx.UNBOUNDED() != null) {
                    frameBoundType = WindowFrame.FrameBoundType.UNBOUNDED_FOLLOWING;
                } else {
                    frameBoundType = WindowFrame.FrameBoundType.FOLLOWING;
                }
                break;
            default:
        }
        return new WindowFrame.FrameBoundary(expression, frameBoundType);
    }

    @Override
    public Expression visitInterval(IntervalContext ctx) {
        return new Interval(getExpression(ctx.value), visitUnitIdentifier(ctx.unit));
    }

    @Override
    public String visitUnitIdentifier(UnitIdentifierContext ctx) {
        return ctx.getText();
    }

    @Override
    public Expression visitTypeConstructor(TypeConstructorContext ctx) {
        String value = ctx.STRING_LITERAL().getText();
        value = value.substring(1, value.length() - 1);
        String type = ctx.type.getText().toUpperCase();
        switch (type) {
            case "DATE":
                return Config.enable_date_conversion ? new DateV2Literal(value) : new DateLiteral(value);
            case "TIMESTAMP":
                return Config.enable_date_conversion ? new DateTimeV2Literal(value) : new DateTimeLiteral(value);
            case "DATEV2":
                return new DateV2Literal(value);
            default:
                throw new ParseException("Unsupported data type : " + type, ctx);
        }
    }

    @Override
    public Expression visitDereference(DereferenceContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = getExpression(ctx.base);
            if (e instanceof UnboundSlot) {
                UnboundSlot unboundAttribute = (UnboundSlot) e;
                List<String> nameParts = Lists.newArrayList(unboundAttribute.getNameParts());
                nameParts.add(ctx.fieldName.getText());
                return new UnboundSlot(nameParts);
            } else {
                // todo: base is an expression, may be not a table name.
                throw new ParseException("Unsupported dereference expression: " + ctx.getText(), ctx);
            }
        });
    }

    @Override
    public UnboundSlot visitColumnReference(ColumnReferenceContext ctx) {
        // todo: handle quoted and unquoted
        return UnboundSlot.quoted(ctx.getText());
    }

    /**
     * Create a NULL literal expression.
     */
    @Override
    public Expression visitNullLiteral(NullLiteralContext ctx) {
        return new NullLiteral();
    }

    @Override
    public Literal visitBooleanLiteral(BooleanLiteralContext ctx) {
        Boolean b = Boolean.valueOf(ctx.getText());
        return BooleanLiteral.of(b);
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        BigInteger bigInt = new BigInteger(ctx.getText());
        if (BigInteger.valueOf(bigInt.byteValue()).equals(bigInt)) {
            return new TinyIntLiteral(bigInt.byteValue());
        } else if (BigInteger.valueOf(bigInt.shortValue()).equals(bigInt)) {
            return new SmallIntLiteral(bigInt.shortValue());
        } else if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
            return new IntegerLiteral(bigInt.intValue());
        } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
            return new BigIntLiteral(bigInt.longValueExact());
        } else {
            return new LargeIntLiteral(bigInt);
        }
    }

    @Override
    public Literal visitStringLiteral(StringLiteralContext ctx) {
        // TODO: add unescapeSQLString.
        String txt = ctx.STRING_LITERAL().getText();
        String s = escapeBackSlash(txt.substring(1, txt.length() - 1));
        return new VarcharLiteral(s);
    }

    private String escapeBackSlash(String str) {
        StringBuilder sb = new StringBuilder();
        int strLen = str.length();
        for (int i = 0; i < strLen; ++i) {
            char c = str.charAt(i);
            if (c == '\\' && (i + 1) < strLen) {
                switch (str.charAt(i + 1)) {
                    case 'n':
                        sb.append('\n');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case '0':
                        sb.append('\0'); // Ascii null
                        break;
                    case 'Z': // ^Z must be escaped on Win32
                        sb.append('\032');
                        break;
                    case '_':
                    case '%':
                        sb.append('\\'); // remember prefix for wildcard
                        sb.append(str.charAt(i + 1));
                        break;
                    default:
                        sb.append(str.charAt(i + 1));
                        break;
                }
                i++;
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    @Override
    public Expression visitParenthesizedExpression(ParenthesizedExpressionContext ctx) {
        return getExpression(ctx.expression());
    }

    @Override
    public List<Expression> visitNamedExpressionSeq(NamedExpressionSeqContext namedCtx) {
        return visit(namedCtx.namedExpression(), Expression.class);
    }

    @Override
    public LogicalPlan visitRelation(RelationContext ctx) {
        return plan(ctx.relationPrimary());
    }

    @Override
    public LogicalPlan visitFromClause(FromClauseContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> withRelations(null, ctx.relation()));
    }

    /* ********************************************************************************************
     * Table Identifier parsing
     * ******************************************************************************************** */

    @Override
    public List<String> visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        return ctx.parts.stream()
            .map(RuleContext::getText)
            .collect(ImmutableList.toImmutableList());
    }

    /**
     * Create a Sequence of Strings for a parenthesis enclosed alias list.
     */
    @Override
    public List<String> visitIdentifierList(IdentifierListContext ctx) {
        return visitIdentifierSeq(ctx.identifierSeq());
    }

    /**
     * Create a Sequence of Strings for an identifier list.
     */
    @Override
    public List<String> visitIdentifierSeq(IdentifierSeqContext ctx) {
        return ctx.ident.stream()
            .map(RuleContext::getText)
            .collect(ImmutableList.toImmutableList());
    }

    @Override
    public EqualTo visitUpdateAssignment(UpdateAssignmentContext ctx) {
        return new EqualTo(new UnboundSlot(visitMultipartIdentifier(ctx.multipartIdentifier())),
                getExpression(ctx.expression()));
    }

    @Override
    public List<EqualTo> visitUpdateAssignmentSeq(UpdateAssignmentSeqContext ctx) {
        return ctx.assignments.stream()
                .map(this::visitUpdateAssignment)
                .collect(Collectors.toList());
    }

    /**
     * get OrderKey.
     *
     * @param ctx SortItemContext
     * @return SortItems
     */
    @Override
    public OrderKey visitSortItem(SortItemContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            boolean isAsc = ctx.DESC() == null;
            boolean isNullFirst = ctx.FIRST() != null || (ctx.LAST() == null && isAsc);
            Expression expression = typedVisit(ctx.expression());
            return new OrderKey(expression, isAsc, isNullFirst);
        });
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(ImmutableList.toImmutableList());
    }

    private LogicalPlan plan(ParserRuleContext tree) {
        return (LogicalPlan) tree.accept(this);
    }

    /* ********************************************************************************************
     * Expression parsing
     * ******************************************************************************************** */

    /**
     * Create an expression from the given context. This method just passes the context on to the
     * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
     */
    private Expression getExpression(ParserRuleContext ctx) {
        return typedVisit(ctx);
    }

    private LogicalPlan withExplain(LogicalPlan inputPlan, ExplainContext ctx) {
        if (ctx == null) {
            return inputPlan;
        }
        return ParserUtils.withOrigin(ctx, () -> {
            ExplainLevel explainLevel = ExplainLevel.NORMAL;

            if (ctx.planType() != null) {
                if (ctx.level == null || !ctx.level.getText().equalsIgnoreCase("plan")) {
                    throw new ParseException("Only explain plan can use plan type: " + ctx.planType().getText(), ctx);
                }
            }

            if (ctx.level != null) {
                if (!ctx.level.getText().equalsIgnoreCase("plan")) {
                    explainLevel = ExplainLevel.valueOf(ctx.level.getText().toUpperCase(Locale.ROOT));
                } else {
                    explainLevel = parseExplainPlanType(ctx.planType());
                }
            }
            return new ExplainCommand(explainLevel, inputPlan);
        });
    }

    private LogicalPlan withOutFile(LogicalPlan plan, OutFileClauseContext ctx) {
        if (ctx == null) {
            return plan;
        }
        String format = "csv";
        if (ctx.format != null) {
            format = ctx.format.getText();
        }
        Map<String, String> properties = Maps.newHashMap();
        for (TvfPropertyContext argument : ctx.properties) {
            String key = parseConstant(argument.key.constant());
            String value = parseConstant(argument.value.constant());
            properties.put(key, value);
        }
        Literal filePath = (Literal) visit(ctx.filePath);
        return new LogicalFileSink<>(filePath.getStringValue(), format, properties, ImmutableList.of(), plan);
    }

    private LogicalPlan withQueryOrganization(LogicalPlan inputPlan, QueryOrganizationContext ctx) {
        if (ctx == null) {
            return inputPlan;
        }
        Optional<SortClauseContext> sortClauseContext = Optional.ofNullable(ctx.sortClause());
        Optional<LimitClauseContext> limitClauseContext = Optional.ofNullable(ctx.limitClause());
        LogicalPlan sort = withSort(inputPlan, sortClauseContext);
        return withLimit(sort, limitClauseContext);
    }

    private LogicalPlan withSort(LogicalPlan input, Optional<SortClauseContext> sortCtx) {
        return input.optionalMap(sortCtx, () -> {
            List<OrderKey> orderKeys = visit(sortCtx.get().sortItem(), OrderKey.class);
            return new LogicalSort<>(orderKeys, input);
        });
    }

    private LogicalPlan withLimit(LogicalPlan input, Optional<LimitClauseContext> limitCtx) {
        return input.optionalMap(limitCtx, () -> {
            long limit = Long.parseLong(limitCtx.get().limit.getText());
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", limitCtx.get());
            }
            long offset = 0;
            Token offsetToken = limitCtx.get().offset;
            if (offsetToken != null) {
                offset = Long.parseLong(offsetToken.getText());
            }
            return new LogicalLimit<>(limit, offset, LimitPhase.ORIGIN, input);
        });
    }

    private UnboundOneRowRelation withOneRowRelation(SelectColumnClauseContext selectCtx) {
        return ParserUtils.withOrigin(selectCtx, () -> {
            // fromClause does not exists.
            List<NamedExpression> projects = getNamedExpressions(selectCtx.namedExpressionSeq());
            return new UnboundOneRowRelation(StatementScopeIdGenerator.newRelationId(), projects);
        });
    }

    /**
     * Add a regular (SELECT) query specification to a logical plan. The query specification
     * is the core of the logical plan, this is where sourcing (FROM clause), projection (SELECT),
     * aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
     *
     * <p>Note that query hints are ignored (both by the parser and the builder).
     */
    private LogicalPlan withSelectQuerySpecification(
            ParserRuleContext ctx,
            LogicalPlan inputRelation,
            SelectClauseContext selectClause,
            Optional<WhereClauseContext> whereClause,
            Optional<AggClauseContext> aggClause,
            Optional<HavingClauseContext> havingClause) {
        return ParserUtils.withOrigin(ctx, () -> {
            // from -> where -> group by -> having -> select
            LogicalPlan filter = withFilter(inputRelation, whereClause);
            SelectColumnClauseContext selectColumnCtx = selectClause.selectColumnClause();
            LogicalPlan aggregate = withAggregate(filter, selectColumnCtx, aggClause);
            boolean isDistinct = (selectClause.DISTINCT() != null);
            if (isDistinct && aggregate instanceof Aggregate) {
                throw new ParseException("cannot combine SELECT DISTINCT with aggregate functions or GROUP BY",
                        selectClause);
            }
            if (!(aggregate instanceof Aggregate) && havingClause.isPresent()) {
                // create a project node for pattern match of ProjectToGlobalAggregate rule
                // then ProjectToGlobalAggregate rule can insert agg node as LogicalHaving node's child
                LogicalPlan project;
                if (selectColumnCtx.EXCEPT() != null) {
                    List<NamedExpression> expressions = getNamedExpressions(selectColumnCtx.namedExpressionSeq());
                    if (!expressions.stream().allMatch(UnboundSlot.class::isInstance)) {
                        throw new ParseException("only column name is supported in except clause", selectColumnCtx);
                    }
                    project = new LogicalProject<>(ImmutableList.of(new UnboundStar(ImmutableList.of())),
                        expressions, isDistinct, aggregate);
                } else {
                    List<NamedExpression> projects = getNamedExpressions(selectColumnCtx.namedExpressionSeq());
                    project = new LogicalProject<>(projects, ImmutableList.of(), isDistinct, aggregate);
                }
                return new LogicalHaving<>(ExpressionUtils.extractConjunctionToSet(
                        getExpression((havingClause.get().booleanExpression()))), project);
            } else {
                LogicalPlan having = withHaving(aggregate, havingClause);
                return withProjection(having, selectColumnCtx, aggClause, isDistinct);
            }
        });
    }

    /**
     * Join one more [[LogicalPlan]]s to the current logical plan.
     */
    private LogicalPlan withJoinRelations(LogicalPlan input, RelationContext ctx) {
        LogicalPlan last = input;
        for (JoinRelationContext join : ctx.joinRelation()) {
            JoinType joinType;
            if (join.joinType().CROSS() != null) {
                joinType = JoinType.CROSS_JOIN;
            } else if (join.joinType().FULL() != null) {
                joinType = JoinType.FULL_OUTER_JOIN;
            } else if (join.joinType().SEMI() != null) {
                if (join.joinType().LEFT() != null) {
                    joinType = JoinType.LEFT_SEMI_JOIN;
                } else {
                    joinType = JoinType.RIGHT_SEMI_JOIN;
                }
            } else if (join.joinType().ANTI() != null) {
                if (join.joinType().LEFT() != null) {
                    joinType = JoinType.LEFT_ANTI_JOIN;
                } else {
                    joinType = JoinType.RIGHT_ANTI_JOIN;
                }
            } else if (join.joinType().LEFT() != null) {
                joinType = JoinType.LEFT_OUTER_JOIN;
            } else if (join.joinType().RIGHT() != null) {
                joinType = JoinType.RIGHT_OUTER_JOIN;
            } else if (join.joinType().INNER() != null) {
                joinType = JoinType.INNER_JOIN;
            } else if (join.joinCriteria() != null) {
                joinType = JoinType.INNER_JOIN;
            } else {
                joinType = JoinType.CROSS_JOIN;
            }
            JoinHint joinHint = Optional.ofNullable(join.joinHint()).map(hintCtx -> {
                String hint = typedVisit(join.joinHint());
                if (JoinHint.JoinHintType.SHUFFLE.toString().equalsIgnoreCase(hint)) {
                    return JoinHint.SHUFFLE_RIGHT;
                } else if (JoinHint.JoinHintType.BROADCAST.toString().equalsIgnoreCase(hint)) {
                    return JoinHint.BROADCAST_RIGHT;
                } else {
                    throw new ParseException("Invalid join hint: " + hint, hintCtx);
                }
            }).orElse(JoinHint.NONE);
            // TODO: natural join, lateral join, union join
            JoinCriteriaContext joinCriteria = join.joinCriteria();
            Optional<Expression> condition = Optional.empty();
            List<Expression> ids = null;
            if (joinCriteria != null) {
                if (join.joinType().CROSS() != null) {
                    throw new ParseException("Cross join can't be used with ON clause", joinCriteria);
                }
                if (joinCriteria.booleanExpression() != null) {
                    condition = Optional.ofNullable(getExpression(joinCriteria.booleanExpression()));
                } else if (joinCriteria.USING() != null) {
                    ids = visitIdentifierList(joinCriteria.identifierList())
                            .stream().map(UnboundSlot::quoted)
                            .collect(ImmutableList.toImmutableList());
                }
            } else {
                // keep same with original planner, allow cross/inner join
                if (!joinType.isInnerOrCrossJoin()) {
                    throw new ParseException("on mustn't be empty except for cross/inner join", join);
                }
            }
            if (ids == null) {
                last = new LogicalJoin<>(joinType, ExpressionUtils.EMPTY_CONDITION,
                        condition.map(ExpressionUtils::extractConjunction)
                                .orElse(ExpressionUtils.EMPTY_CONDITION),
                        joinHint,
                        Optional.empty(),
                        last,
                        plan(join.relationPrimary()));
            } else {
                last = new UsingJoin<>(joinType, last,
                        plan(join.relationPrimary()), ImmutableList.of(), ids, joinHint);

            }
        }
        return last;
    }

    private LogicalPlan withSelectHint(LogicalPlan logicalPlan, SelectHintContext hintContext) {
        if (hintContext == null) {
            return logicalPlan;
        }
        Map<String, SelectHint> hints = Maps.newLinkedHashMap();
        for (HintStatementContext hintStatement : hintContext.hintStatements) {
            String hintName = hintStatement.hintName.getText().toLowerCase(Locale.ROOT);
            Map<String, Optional<String>> parameters = Maps.newLinkedHashMap();
            for (HintAssignmentContext kv : hintStatement.parameters) {
                String parameterName = visitIdentifierOrText(kv.key);
                Optional<String> value = Optional.empty();
                if (kv.constantValue != null) {
                    Literal literal = (Literal) visit(kv.constantValue);
                    value = Optional.ofNullable(literal.toLegacyLiteral().getStringValue());
                } else if (kv.identifierValue != null) {
                    // maybe we should throw exception when the identifierValue is quoted identifier
                    value = Optional.ofNullable(kv.identifierValue.getText());
                }
                parameters.put(parameterName, value);
            }
            hints.put(hintName, new SelectHint(hintName, parameters));
        }
        return new LogicalSelectHint<>(hints, logicalPlan);
    }

    @Override
    public String visitBracketJoinHint(BracketJoinHintContext ctx) {
        return ctx.identifier().getText();
    }

    @Override
    public String visitCommentJoinHint(CommentJoinHintContext ctx) {
        return ctx.identifier().getText();
    }

    @Override
    public List<String> visitBracketRelationHint(BracketRelationHintContext ctx) {
        return ctx.identifier().stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Object visitCommentRelationHint(CommentRelationHintContext ctx) {
        return ctx.identifier().stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    private LogicalPlan withProjection(LogicalPlan input, SelectColumnClauseContext selectCtx,
                                       Optional<AggClauseContext> aggCtx, boolean isDistinct) {
        return ParserUtils.withOrigin(selectCtx, () -> {
            if (aggCtx.isPresent()) {
                return input;
            } else {
                if (selectCtx.EXCEPT() != null) {
                    List<NamedExpression> expressions = getNamedExpressions(selectCtx.namedExpressionSeq());
                    if (!expressions.stream().allMatch(UnboundSlot.class::isInstance)) {
                        throw new ParseException("only column name is supported in except clause", selectCtx);
                    }
                    return new LogicalProject<>(ImmutableList.of(new UnboundStar(ImmutableList.of())),
                            expressions, isDistinct, input);
                } else {
                    List<NamedExpression> projects = getNamedExpressions(selectCtx.namedExpressionSeq());
                    return new LogicalProject<>(projects, Collections.emptyList(), isDistinct, input);
                }
            }
        });
    }

    private LogicalPlan withRelations(LogicalPlan inputPlan, List<RelationContext> relations) {
        LogicalPlan left = inputPlan;
        for (RelationContext relation : relations) {
            // build left deep join tree
            LogicalPlan right = visitRelation(relation);
            left = (left == null) ? right :
                    new LogicalJoin<>(
                            JoinType.CROSS_JOIN,
                            ExpressionUtils.EMPTY_CONDITION,
                            ExpressionUtils.EMPTY_CONDITION,
                            JoinHint.NONE,
                            Optional.empty(),
                            left,
                            right);
            left = withJoinRelations(left, relation);
            // TODO: pivot and lateral view
        }
        return left;
    }

    private LogicalPlan withFilter(LogicalPlan input, Optional<WhereClauseContext> whereCtx) {
        return input.optionalMap(whereCtx, () ->
            new LogicalFilter<>(ExpressionUtils.extractConjunctionToSet(
                    getExpression(whereCtx.get().booleanExpression())), input));
    }

    private LogicalPlan withAggregate(LogicalPlan input, SelectColumnClauseContext selectCtx,
                                      Optional<AggClauseContext> aggCtx) {
        return input.optionalMap(aggCtx, () -> {
            GroupingElementContext groupingElementContext = aggCtx.get().groupingElement();
            List<NamedExpression> namedExpressions = getNamedExpressions(selectCtx.namedExpressionSeq());
            if (groupingElementContext.GROUPING() != null) {
                ImmutableList.Builder<List<Expression>> groupingSets = ImmutableList.builder();
                for (GroupingSetContext groupingSetContext : groupingElementContext.groupingSet()) {
                    groupingSets.add(visit(groupingSetContext.expression(), Expression.class));
                }
                return new LogicalRepeat<>(groupingSets.build(), namedExpressions, input);
            } else if (groupingElementContext.CUBE() != null) {
                List<Expression> cubeExpressions = visit(groupingElementContext.expression(), Expression.class);
                List<List<Expression>> groupingSets = ExpressionUtils.cubeToGroupingSets(cubeExpressions);
                return new LogicalRepeat<>(groupingSets, namedExpressions, input);
            } else if (groupingElementContext.ROLLUP() != null) {
                List<Expression> rollupExpressions = visit(groupingElementContext.expression(), Expression.class);
                List<List<Expression>> groupingSets = ExpressionUtils.rollupToGroupingSets(rollupExpressions);
                return new LogicalRepeat<>(groupingSets, namedExpressions, input);
            } else {
                List<Expression> groupByExpressions = visit(groupingElementContext.expression(), Expression.class);
                return new LogicalAggregate<>(groupByExpressions, namedExpressions, input);
            }
        });
    }

    private LogicalPlan withHaving(LogicalPlan input, Optional<HavingClauseContext> havingCtx) {
        return input.optionalMap(havingCtx, () -> {
            if (!(input instanceof Aggregate)) {
                throw new ParseException("Having clause should be applied against an aggregation.", havingCtx.get());
            }
            return new LogicalHaving<>(ExpressionUtils.extractConjunctionToSet(
                    getExpression((havingCtx.get().booleanExpression()))), input);
        });
    }

    /**
     * match predicate type and generate different predicates.
     *
     * @param ctx PredicateContext
     * @param valueExpression valueExpression
     * @return Expression
     */
    private Expression withPredicate(Expression valueExpression, PredicateContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression outExpression;
            switch (ctx.kind.getType()) {
                case DorisParser.BETWEEN:
                    outExpression = new And(
                            new GreaterThanEqual(valueExpression, getExpression(ctx.lower)),
                            new LessThanEqual(valueExpression, getExpression(ctx.upper))
                    );
                    break;
                case DorisParser.LIKE:
                    outExpression = new Like(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.RLIKE:
                case DorisParser.REGEXP:
                    outExpression = new Regexp(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.IN:
                    if (ctx.query() == null) {
                        outExpression = new InPredicate(
                                valueExpression,
                                withInList(ctx)
                        );
                    } else {
                        outExpression = new InSubquery(
                                valueExpression,
                                new ListQuery(typedVisit(ctx.query())),
                                ctx.NOT() != null
                        );
                    }
                    break;
                case DorisParser.NULL:
                    outExpression = new IsNull(valueExpression);
                    break;
                case DorisParser.MATCH:
                case DorisParser.MATCH_ANY:
                    outExpression = new MatchAny(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_ALL:
                    outExpression = new MatchAll(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_PHRASE:
                    outExpression = new MatchPhrase(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                default:
                    throw new ParseException("Unsupported predicate type: " + ctx.kind.getText(), ctx);
            }
            return ctx.NOT() != null ? new Not(outExpression) : outExpression;
        });
    }

    private List<NamedExpression> getNamedExpressions(NamedExpressionSeqContext namedCtx) {
        return ParserUtils.withOrigin(namedCtx, () -> {
            List<Expression> expressions = visit(namedCtx.namedExpression(), Expression.class);
            return expressions.stream().map(expression -> {
                if (expression instanceof NamedExpression) {
                    return (NamedExpression) expression;
                } else {
                    return new UnboundAlias(expression);
                }
            }).collect(ImmutableList.toImmutableList());
        });
    }

    @Override
    public Expression visitSubqueryExpression(SubqueryExpressionContext subqueryExprCtx) {
        return ParserUtils.withOrigin(subqueryExprCtx, () -> new ScalarSubquery(typedVisit(subqueryExprCtx.query())));
    }

    @Override
    public Expression visitExist(ExistContext context) {
        return ParserUtils.withOrigin(context, () -> new Exists(typedVisit(context.query()), false));
    }

    @Override
    public Expression visitIsnull(IsnullContext context) {
        return ParserUtils.withOrigin(context, () -> new IsNull(typedVisit(context.valueExpression())));
    }

    @Override
    public Expression visitIs_not_null_pred(Is_not_null_predContext context) {
        return ParserUtils.withOrigin(context, () -> new Not(new IsNull(typedVisit(context.valueExpression()))));
    }

    public List<Expression> withInList(PredicateContext ctx) {
        return ctx.expression().stream().map(this::getExpression).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Literal visitDecimalLiteral(DecimalLiteralContext ctx) {
        if (Config.enable_decimal_conversion) {
            return new DecimalV3Literal(new BigDecimal(ctx.getText()));
        } else {
            return new DecimalLiteral(new BigDecimal(ctx.getText()));
        }
    }

    private String parseTVFPropertyItem(TvfPropertyItemContext item) {
        if (item.constant() != null) {
            return parseConstant(item.constant());
        }
        return item.getText();
    }

    private ExplainLevel parseExplainPlanType(PlanTypeContext planTypeContext) {
        if (planTypeContext == null || planTypeContext.ALL() != null) {
            return ExplainLevel.ALL_PLAN;
        }
        if (planTypeContext.PHYSICAL() != null || planTypeContext.OPTIMIZED() != null) {
            return ExplainLevel.OPTIMIZED_PLAN;
        }
        if (planTypeContext.REWRITTEN() != null || planTypeContext.LOGICAL() != null) {
            return ExplainLevel.REWRITTEN_PLAN;
        }
        if (planTypeContext.ANALYZED() != null) {
            return ExplainLevel.ANALYZED_PLAN;
        }
        if (planTypeContext.PARSED() != null) {
            return ExplainLevel.PARSED_PLAN;
        }
        if (planTypeContext.SHAPE() != null) {
            return ExplainLevel.SHAPE_PLAN;
        }
        if (planTypeContext.MEMO() != null) {
            return ExplainLevel.MEMO_PLAN;
        }
        return ExplainLevel.ALL_PLAN;
    }

    @Override
    public List<String> visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        String dataType = ctx.primitiveColType().getText().toLowerCase(Locale.ROOT);
        List<String> l = Lists.newArrayList(dataType);
        ctx.INTEGER_VALUE().stream().map(ParseTree::getText).forEach(l::add);
        return l;
    }

    private Expression parseFunctionWithOrderKeys(String functionName, boolean isDistinct,
            List<Expression> params, List<OrderKey> orderKeys, ParserRuleContext ctx) {
        if (functionName.equalsIgnoreCase("group_concat")) {
            OrderExpression[] orderExpressions = orderKeys.stream()
                    .map(OrderExpression::new)
                    .toArray(OrderExpression[]::new);
            if (params.size() == 1) {
                return new GroupConcat(isDistinct, params.get(0), orderExpressions);
            } else if (params.size() == 2) {
                return new GroupConcat(isDistinct, params.get(0), params.get(1), orderExpressions);
            } else {
                throw new ParseException("group_concat requires one or two parameters: " + params, ctx);
            }
        }
        throw new ParseException("Unsupported function with order expressions" + ctx.getText(), ctx);
    }

    private String parseConstant(ConstantContext context) {
        Object constant = visit(context);
        if (constant instanceof Literal && ((Literal) constant).isStringLikeLiteral()) {
            return ((Literal) constant).getStringValue();
        }
        return context.getText();
    }

    @Override
    public Object visitSample(SampleContext ctx) {
        long seek = ctx.seed == null ? -1L : Long.parseLong(ctx.seed.getText());
        DorisParser.SampleMethodContext sampleContext = ctx.sampleMethod();
        if (sampleContext instanceof SampleByPercentileContext) {
            SampleByPercentileContext sampleByPercentileContext = (SampleByPercentileContext) sampleContext;
            long percent = Long.parseLong(sampleByPercentileContext.INTEGER_VALUE().getText());
            return new TableSample(percent, true, seek);
        }
        SampleByRowsContext sampleByRowsContext = (SampleByRowsContext) sampleContext;
        long rows = Long.parseLong(sampleByRowsContext.ROWS().getText());
        return new TableSample(rows, false, seek);
    }

}
