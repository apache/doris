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


import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.AggClauseContext;
import org.apache.doris.nereids.DorisParser.AggFunctionsContext;
import org.apache.doris.nereids.DorisParser.ArithmeticBinaryContext;
import org.apache.doris.nereids.DorisParser.ArithmeticUnaryContext;
import org.apache.doris.nereids.DorisParser.BooleanLiteralContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.ComparisonContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.ExpressionContext;
import org.apache.doris.nereids.DorisParser.FromClauseContext;
import org.apache.doris.nereids.DorisParser.GroupByItemContext;
import org.apache.doris.nereids.DorisParser.IdentifierListContext;
import org.apache.doris.nereids.DorisParser.IdentifierSeqContext;
import org.apache.doris.nereids.DorisParser.IntegerLiteralContext;
import org.apache.doris.nereids.DorisParser.JoinCriteriaContext;
import org.apache.doris.nereids.DorisParser.JoinRelationContext;
import org.apache.doris.nereids.DorisParser.LogicalBinaryContext;
import org.apache.doris.nereids.DorisParser.LogicalNotContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParser.NullLiteralContext;
import org.apache.doris.nereids.DorisParser.PredicateContext;
import org.apache.doris.nereids.DorisParser.PredicatedContext;
import org.apache.doris.nereids.DorisParser.QualifiedNameContext;
import org.apache.doris.nereids.DorisParser.QueryContext;
import org.apache.doris.nereids.DorisParser.QueryOrganizationContext;
import org.apache.doris.nereids.DorisParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.DorisParser.RelationContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.DorisParser.SortItemContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.StatementContext;
import org.apache.doris.nereids.DorisParser.StringLiteralContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregation;
import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.operators.plans.logical.LogicalSort;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.analysis.FunctionParams;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Arithmetic;
import org.apache.doris.nereids.trees.expressions.BetweenPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.FunctionCall;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Build an logical plan tree with unbounded nodes.
 */
public class LogicalPlanBuilder extends DorisParserBaseVisitor<Object> {

    /**
     * Create a logical plan using a where clause.
     */
    private final BiFunction<WhereClauseContext, LogicalPlan, LogicalPlan> withWhereClause
            = (WhereClauseContext ctx, LogicalPlan plan) -> new LogicalUnaryPlan(
                    new LogicalFilter(expression((ctx.booleanExpression()))), plan);

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
        Supplier<LogicalPlan> f = () -> (LogicalPlan) visit(ctx.statement());
        return ParserUtils.withOrigin(ctx, f);
    }

    /**
     * Visit multi-statements.
     */
    public Object visitMultiStatements(MultiStatementsContext ctx) {
        List<LogicalPlan> logicalPlanList = new ArrayList<>();
        for (StatementContext stmtCtx : ctx.statement()) {
            LogicalPlan logicalPlan = (LogicalPlan) visit(stmtCtx);
            logicalPlanList.add(logicalPlan);
        }
        return logicalPlanList;
    }

    /* ********************************************************************************************
     * Plan parsing
     * ******************************************************************************************** */
    private LogicalPlan plan(ParserRuleContext tree) {
        return (LogicalPlan) tree.accept(this);
    }

    @Override
    public LogicalPlan visitQuery(QueryContext ctx) {
        Supplier<LogicalPlan> f = () -> {
            // TODO: need to add withQueryResultClauses and withCTE
            LogicalPlan query = plan(ctx.queryTerm());
            LogicalPlan queryOrganization = withQueryOrganization(ctx.queryOrganization(), query);
            return queryOrganization;
        };
        return ParserUtils.withOrigin(ctx, f);
    }

    private LogicalPlan withQueryOrganization(QueryOrganizationContext ctx, LogicalPlan children) {
        List<OrderKey> orderKeys = visitQueryOrganization(ctx);
        return orderKeys == null ? children : new LogicalUnaryPlan(new LogicalSort(orderKeys), children);
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(RegularQuerySpecificationContext ctx) {
        Supplier<LogicalPlan> f = () -> {
            // TODO: support on row relation
            LogicalPlan from = visitFromClause(ctx.fromClause());
            return withSelectQuerySpecification(ctx, ctx.selectClause(), ctx.whereClause(), from, ctx.aggClause());
        };
        return ParserUtils.withOrigin(ctx, f);
    }

    @Override
    public Expression visitExpression(ExpressionContext ctx) {
        Supplier<Expression> f = () -> (Expression) visit(ctx.booleanExpression());
        return ParserUtils.withOrigin(ctx, f);
    }

    @Override
    public List<Expression> visitNamedExpressionSeq(NamedExpressionSeqContext ctx) {
        List<Expression> expressions = Lists.newArrayList();
        if (ctx != null) {
            for (NamedExpressionContext namedExpressionContext : ctx.namedExpression()) {
                Expression expression = typedVisit(namedExpressionContext);
                expressions.add(expression);
            }
        }
        return expressions;
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
            SelectClauseContext selectClause,
            WhereClauseContext whereClause,
            LogicalPlan relation,
            AggClauseContext aggClause) {
        Supplier<LogicalPlan> f = () -> {
            //        Filter(expression(ctx.booleanExpression), plan);
            LogicalPlan plan = visitCommonSelectQueryClausePlan(relation,
                    visitNamedExpressionSeq(selectClause.namedExpressionSeq()), whereClause, aggClause);
            // TODO: process hint
            return plan;
        };
        return ParserUtils.withOrigin(ctx, f);
    }

    private LogicalPlan visitCommonSelectQueryClausePlan(
            LogicalPlan relation,
            List<Expression> expressions,
            WhereClauseContext whereClause,
            AggClauseContext aggClause) {
        // TODO: add lateral views
        // val withLateralView = lateralView.asScala.foldLeft(relation)(withGenerate)

        // add where
        LogicalPlan withFilter = relation.optionalMap(whereClause, withWhereClause);

        List<NamedExpression> namedExpressions = expressions.stream().map(expression -> {
            if (expression instanceof NamedExpression) {
                return (NamedExpression) expression;
            } else {
                return new UnboundAlias(expression);
            }
        }).collect(Collectors.toList());

        LogicalPlan withProject;
        if (CollectionUtils.isNotEmpty(namedExpressions)) {
            withProject = new LogicalUnaryPlan(new LogicalProject(namedExpressions), withFilter);
        } else {
            withProject = withFilter;
        }

        LogicalPlan withAgg;
        if (aggClause != null) {
            withAgg = withAggClause(namedExpressions, aggClause.groupByItem(), withFilter);
        } else {
            withAgg = withProject;
        }

        return withAgg;
    }

    @Override
    public LogicalPlan visitFromClause(FromClauseContext ctx) {
        LogicalPlan left = null;
        for (RelationContext relation : ctx.relation()) {
            LogicalPlan right = plan(relation.relationPrimary());
            if (left == null) {
                left = right;
            } else {
                left = new LogicalBinaryPlan(new LogicalJoin(JoinType.INNER_JOIN, Optional.empty()), left, right);
            }
            left = withJoinRelations(left, relation);
        }
        // TODO: pivot and lateral view
        return left;
    }

    /**
     * Join one more [[LogicalPlan]]s to the current logical plan.
     */
    private LogicalPlan withJoinRelations(LogicalPlan base, RelationContext ctx) {
        LogicalPlan last = base;
        for (JoinRelationContext join : ctx.joinRelation()) {
            JoinType joinType;
            if (join.joinType().LEFT() != null) {
                joinType = JoinType.LEFT_OUTER_JOIN;
            } else if (join.joinType().RIGHT() != null) {
                joinType = JoinType.RIGHT_OUTER_JOIN;
            } else if (join.joinType().FULL() != null) {
                joinType = JoinType.FULL_OUTER_JOIN;
            } else if (join.joinType().SEMI() != null) {
                joinType = JoinType.LEFT_SEMI_JOIN;
            } else if (join.joinType().ANTI() != null) {
                joinType = JoinType.LEFT_ANTI_JOIN;
            } else if (join.joinType().CROSS() != null) {
                joinType = JoinType.CROSS_JOIN;
            } else {
                joinType = JoinType.INNER_JOIN;
            }

            // TODO: natural join, lateral join, using join
            JoinCriteriaContext joinCriteria = join.joinCriteria();
            Expression condition;
            if (joinCriteria == null) {
                condition = null;
            } else {
                condition = expression(joinCriteria.booleanExpression());
            }

            last = new LogicalBinaryPlan(new LogicalJoin(joinType, Optional.ofNullable(condition)), last,
                    plan(join.relationPrimary()));
        }
        return last;
    }

    private LogicalPlan withAggClause(List<NamedExpression> aggExpressions, GroupByItemContext ctx,
            LogicalPlan aggClause) {
        List<Expression> tmpExpressions = new ArrayList<>();
        for (ExpressionContext expressionCtx : ctx.expression()) {
            tmpExpressions.add(typedVisit(expressionCtx));
        }
        return new LogicalUnaryPlan(new LogicalAggregation(tmpExpressions, aggExpressions), aggClause);
    }

    /**
     * Generate OrderKey.
     *
     * @param ctx SortItemContext
     * @return OrderKey
     */
    public OrderKey genOrderKeys(SortItemContext ctx) {
        boolean isAsc = ctx.DESC() == null;
        // TODO(wj): isNullFirst
        boolean isNullFirst = true;
        Expression expression = typedVisit(ctx.expression());
        return new OrderKey(expression, isAsc, isNullFirst);
    }

    /**
     * Create OrderKey list.
     *
     * @param ctx QueryOrganizationContext
     * @return List of OrderKey
     */
    public List<OrderKey> visitQueryOrganization(QueryOrganizationContext ctx) {
        List<OrderKey> orderKeys = new ArrayList<>();
        if (ctx.sortClause().ORDER() != null) {
            for (SortItemContext sortItemContext : ctx.sortClause().sortItem()) {
                orderKeys.add(genOrderKeys(sortItemContext));
            }
            return new ArrayList<>(orderKeys);
        } else {
            return null;
        }
    }

    /**
     * Create an aliased table reference. This is typically used in FROM clauses.
     */
    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        List<String> tableId = visitMultipartIdentifier(ctx.multipartIdentifier());
        UnboundRelation relation = new UnboundRelation(tableId);
        // TODO: sample and time travel, alias, sub query
        return new LogicalLeafPlan(relation);
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
        return ctx.ident.stream().map(RuleContext::getText).collect(Collectors.toList());
    }

    /* ********************************************************************************************
     * Table Identifier parsing
     * ******************************************************************************************** */

    @Override
    public List<String> visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        return ctx.parts.stream().map(RuleContext::getText).collect(Collectors.toList());
    }

    /* ********************************************************************************************
     * Expression parsing
     * ******************************************************************************************** */

    /**
     * Create an expression from the given context. This method just passes the context on to the
     * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
     */
    private Expression expression(ParserRuleContext ctx) {
        return typedVisit(ctx);
    }

    /**
     * Create a star (i.e. all) expression; this selects all elements (in the specified object).
     * Both un-targeted (global) and targeted aliases are supported.
     */
    @Override
    public Expression visitStar(StarContext ctx) {
        Supplier<Expression> f = () -> {
            final QualifiedNameContext qualifiedNameContext = ctx.qualifiedName();
            List<String> target;
            if (qualifiedNameContext != null) {
                target = qualifiedNameContext.identifier().stream().map(RuleContext::getText)
                        .collect(Collectors.toList());
            } else {
                target = Lists.newArrayList();
            }
            return new UnboundStar(target);
        };
        return ParserUtils.withOrigin(ctx, f);
    }

    /**
     * Create an aliased expression if an alias is specified. Both single and multi-aliases are
     * supported.
     */
    @Override
    public Expression visitNamedExpression(NamedExpressionContext ctx) {
        final Expression expression = expression(ctx.expression());
        if (ctx.name != null) {
            return new Alias(expression, ctx.name.getText());
        } else {
            return expression;
        }
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
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
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
                return null;
        }
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
        Expression child = expression(ctx.booleanExpression());
        return new Not(child);
    }

    @Override
    public Expression visitLogicalBinary(LogicalBinaryContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        switch (ctx.operator.getType()) {
            case DorisParser.AND:
                return new And(left, right);
            case DorisParser.OR:
                return new Or(left, right);
            default:
                return null;
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
        Expression e = expression(ctx.valueExpression());
        // TODO: add predicate(is not null ...)
        if (ctx.predicate() != null) {
            return withPredicate(ctx.predicate(), e);
        }
        return e;
    }

    /**
     * match predicate type and generate different predicates.
     *
     * @param ctx PredicateContext
     * @param e Expression
     * @return Expression
     */
    public Expression withPredicate(PredicateContext ctx, Expression e) {
        switch (ctx.kind.getType()) {
            case DorisParser.BETWEEN:
                return withBetween(ctx, e);
            default:
                return null;
        }
    }

    /**
     * Generate between predicate.
     *
     * @param ctx PredicateContext
     * @param e Expression
     * @return Expression
     */
    public Expression withBetween(PredicateContext ctx, Expression e) {
        boolean isNotBetween = ctx.NOT() != null ? true : false;
        BetweenPredicate betweenPredicate = new BetweenPredicate(e, expression(ctx.lower), expression(ctx.upper));
        return isNotBetween ? new Not(betweenPredicate) : betweenPredicate;
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Expression e = expression(ctx);
        switch (ctx.operator.getType()) {
            case DorisParser.PLUS:
                return e;
            case DorisParser.MINUS:
                //TODO: Add single operator subtraction
            default:
                return null;
        }
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        return genArithmetic(ctx.operator, left, right);
    }

    private Arithmetic genArithmetic(Token token, Expression left, Expression right) {
        switch (token.getType()) {
            case DorisParser.ASTERISK:
                return new Multiply(left, right);
            case DorisParser.SLASH:
                return new Divide(left, right);
            case DorisParser.PERCENT:
                return new Mod(left, right);
            case DorisParser.PLUS:
                return new Add(left, right);
            case DorisParser.MINUS:
                return new Subtract(left, right);
            default:
                return null;
        }
    }

    @Override
    public Expression visitAggFunctions(AggFunctionsContext ctx) {
        // TODO:In the future, instead of specifying the function name,
        //      the function information is obtained by parsing the catalog. This method is more scalable.
        String functionName = "";
        if (ctx.aggFunction().SUM() != null) {
            functionName = "sum";
        } else if (ctx.aggFunction().AVG() != null) {
            functionName = "avg";
        }

        return new FunctionCall(functionName,
                new FunctionParams(ctx.aggFunction().DISTINCT() != null, expression(ctx.aggFunction().expression())));
    }

    @Override
    public Expression visitDereference(DereferenceContext ctx) {
        Expression e = expression(ctx.base);
        if (e instanceof UnboundSlot) {
            UnboundSlot unboundAttribute = (UnboundSlot) e;
            List<String> nameParts = Lists.newArrayList(unboundAttribute.getNameParts());
            nameParts.add(ctx.fieldName.getText());
            return new UnboundSlot(nameParts);
        } else {
            // todo: base is an expression, may be not a table name.
            return null;
        }
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
        return new Literal(null);
    }

    @Override
    public Literal visitBooleanLiteral(BooleanLiteralContext ctx) {
        Boolean b = Boolean.valueOf(ctx.getText());
        return new Literal(b);
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        // TODO: throw NumberFormatException
        Integer l = Integer.valueOf(ctx.getText());
        return new Literal(l);
    }

    @Override
    public Literal visitStringLiteral(StringLiteralContext ctx) {
        String s = ctx.STRING().stream().map(ParseTree::getText).reduce((s1, s2) -> s1 + s2).orElse("");
        return new Literal(s);
    }
}
