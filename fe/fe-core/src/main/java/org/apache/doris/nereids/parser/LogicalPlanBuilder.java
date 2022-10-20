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
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.AggClauseContext;
import org.apache.doris.nereids.DorisParser.AliasedQueryContext;
import org.apache.doris.nereids.DorisParser.AliasedRelationContext;
import org.apache.doris.nereids.DorisParser.ArithmeticBinaryContext;
import org.apache.doris.nereids.DorisParser.ArithmeticUnaryContext;
import org.apache.doris.nereids.DorisParser.BooleanLiteralContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.ComparisonContext;
import org.apache.doris.nereids.DorisParser.DecimalLiteralContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.ExistContext;
import org.apache.doris.nereids.DorisParser.ExplainContext;
import org.apache.doris.nereids.DorisParser.FromClauseContext;
import org.apache.doris.nereids.DorisParser.HavingClauseContext;
import org.apache.doris.nereids.DorisParser.HintAssignmentContext;
import org.apache.doris.nereids.DorisParser.HintStatementContext;
import org.apache.doris.nereids.DorisParser.IdentifierListContext;
import org.apache.doris.nereids.DorisParser.IdentifierSeqContext;
import org.apache.doris.nereids.DorisParser.IntegerLiteralContext;
import org.apache.doris.nereids.DorisParser.IntervalContext;
import org.apache.doris.nereids.DorisParser.JoinCriteriaContext;
import org.apache.doris.nereids.DorisParser.JoinRelationContext;
import org.apache.doris.nereids.DorisParser.LimitClauseContext;
import org.apache.doris.nereids.DorisParser.LogicalBinaryContext;
import org.apache.doris.nereids.DorisParser.LogicalNotContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParser.NullLiteralContext;
import org.apache.doris.nereids.DorisParser.ParenthesizedExpressionContext;
import org.apache.doris.nereids.DorisParser.PredicateContext;
import org.apache.doris.nereids.DorisParser.PredicatedContext;
import org.apache.doris.nereids.DorisParser.QualifiedNameContext;
import org.apache.doris.nereids.DorisParser.QueryContext;
import org.apache.doris.nereids.DorisParser.QueryOrganizationContext;
import org.apache.doris.nereids.DorisParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.DorisParser.RelationContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SelectHintContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.DorisParser.SortClauseContext;
import org.apache.doris.nereids.DorisParser.SortItemContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.StringLiteralContext;
import org.apache.doris.nereids.DorisParser.SubqueryExpressionContext;
import org.apache.doris.nereids.DorisParser.TableAliasContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.DorisParser.TypeConstructorContext;
import org.apache.doris.nereids.DorisParser.UnitIdentifierContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Regexp;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntervalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Build a logical plan tree with unbounded nodes.
 */
public class LogicalPlanBuilder extends DorisParserBaseVisitor<Object> {

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

    /**
     * Visit multi-statements.
     */
    @Override
    public List<LogicalPlan> visitMultiStatements(MultiStatementsContext ctx) {
        return visit(ctx.statement(), LogicalPlan.class);
    }

    /* ********************************************************************************************
     * Plan parsing
     * ******************************************************************************************** */

    @Override
    public Command visitExplain(ExplainContext ctx) {
        LogicalPlan logicalPlan = plan(ctx.query());
        ExplainLevel explainLevel = ExplainLevel.NORMAL;
        if (ctx.level != null) {
            explainLevel = ExplainLevel.valueOf(ctx.level.getText().toUpperCase(Locale.ROOT));
        }
        return new ExplainCommand(explainLevel, logicalPlan);
    }

    @Override
    public LogicalPlan visitQuery(QueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // TODO: need to add withQueryResultClauses and withCTE
            LogicalPlan query = plan(ctx.queryTerm());
            return withQueryOrganization(query, ctx.queryOrganization());
        });
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(RegularQuerySpecificationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            if (ctx.fromClause() == null) {
                return withOneRowRelation(ctx.selectClause());
            }

            LogicalPlan relation = visitFromClause(ctx.fromClause());
            return withSelectQuerySpecification(
                ctx, relation,
                ctx.selectClause(),
                Optional.ofNullable(ctx.whereClause()),
                Optional.ofNullable(ctx.aggClause()),
                Optional.ofNullable(ctx.havingClause())
            );
        });
    }

    /**
     * Create an aliased table reference. This is typically used in FROM clauses.
     */
    @Developing
    private LogicalPlan withTableAlias(LogicalPlan plan, TableAliasContext ctx) {
        String alias = ctx.strictIdentifier().getText();
        if (null != ctx.identifierList()) {
            throw new ParseException("Do not implemented", ctx);
            // TODO: multi-colName
        }
        return new LogicalSubQueryAlias<>(alias, plan);
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        List<String> tableId = visitMultipartIdentifier(ctx.multipartIdentifier());
        if (null == ctx.tableAlias().strictIdentifier()) {
            return new UnboundRelation(tableId);
        }
        return withTableAlias(new UnboundRelation(tableId), ctx.tableAlias());
    }

    @Override
    public LogicalPlan visitAliasedQuery(AliasedQueryContext ctx) {
        return withTableAlias(visitQuery(ctx.query()), ctx.tableAlias());
    }

    @Override
    public LogicalPlan visitAliasedRelation(AliasedRelationContext ctx) {
        return withTableAlias(visitRelation(ctx.relation()), ctx.tableAlias());
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
                target = Collections.emptyList();
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
            if (ctx.name != null) {
                return new Alias(expression, ctx.name.getText());
            } else {
                return expression;
            }
        });
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
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);

            switch (ctx.operator.getType()) {
                case DorisParser.AND:
                    return new And(left, right);
                case DorisParser.OR:
                    return new Or(left, right);
                default:
                    throw new ParseException("Unsupported logical binary type: " + ctx.operator.getText(), ctx);
            }
        });
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
            Expression e = getExpression(ctx);
            switch (ctx.operator.getType()) {
                case DorisParser.PLUS:
                    return e;
                case DorisParser.MINUS:
                    // TODO: Add single operator subtraction
                default:
                    throw new ParseException("Unsupported arithmetic unary type: " + ctx.operator.getText(), ctx);
            }
        });
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);

            int type = ctx.operator.getType();
            if (left instanceof IntervalLiteral) {
                if (type != DorisParser.PLUS) {
                    throw new ParseException("Only supported: " + Operator.ADD, ctx);
                }
                IntervalLiteral interval = (IntervalLiteral) left;
                return new TimestampArithmetic(Operator.ADD, right, interval.value(), interval.timeUnit(), true);
            }

            if (right instanceof IntervalLiteral) {
                Operator op;
                if (type == DorisParser.PLUS) {
                    op = Operator.ADD;
                } else if (type == DorisParser.MINUS) {
                    op = Operator.SUBTRACT;
                } else {
                    throw new ParseException("Only supported: " + Operator.ADD + " and " + Operator.SUBTRACT, ctx);
                }
                IntervalLiteral interval = (IntervalLiteral) right;
                return new TimestampArithmetic(op, left, interval.value(), interval.timeUnit(), false);
            }

            return ParserUtils.withOrigin(ctx, () -> {
                switch (type) {
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
                        throw new ParseException(
                                "Unsupported arithmetic binary type: " + ctx.operator.getText(), ctx);
                }
            });
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
                .collect(Collectors.toList());
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
                .collect(Collectors.toList());
        if (context.elseExpression == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, getExpression(context.elseExpression));
    }

    @Override
    public Expression visitCast(DorisParser.CastContext ctx) {
        return ParserUtils.withOrigin(ctx, () ->
                new Cast(getExpression(ctx.expression()), DataType.convertFromString(ctx.identifier().getText())));
    }

    @Override
    public UnboundFunction visitExtract(DorisParser.ExtractContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.field.getText();
            return new UnboundFunction(functionName, false, false, Arrays.asList(getExpression(ctx.source)));
        });
    }

    @Override
    public UnboundFunction visitFunctionCall(DorisParser.FunctionCallContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // TODO:In the future, instead of specifying the function name,
            //      the function information is obtained by parsing the catalog. This method is more scalable.
            String functionName = ctx.identifier().getText();
            boolean isDistinct = ctx.DISTINCT() != null;
            List<Expression> params = visit(ctx.expression(), Expression.class);
            for (Expression expression : params) {
                if (expression instanceof UnboundStar && functionName.equalsIgnoreCase("count") && !isDistinct) {
                    return new UnboundFunction(functionName, false, true, new ArrayList<>());
                }
            }
            return new UnboundFunction(functionName, isDistinct, false, params);
        });
    }

    @Override
    public Expression visitInterval(IntervalContext ctx) {
        return new IntervalLiteral(getExpression(ctx.value), visitUnitIdentifier(ctx.unit));
    }

    @Override
    public String visitUnitIdentifier(UnitIdentifierContext ctx) {
        return ctx.getText();
    }

    @Override
    public Expression visitTypeConstructor(TypeConstructorContext ctx) {
        String value = ctx.STRING().getText();
        value = value.substring(1, value.length() - 1);
        String type = ctx.identifier().getText().toUpperCase();
        switch (type) {
            case "DATE":
                return new DateLiteral(value);
            case "DATETIME":
                return new DateTimeLiteral(value);
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
        String s = ctx.STRING().stream()
                .map(ParseTree::getText)
                .map(str -> str.substring(1, str.length() - 1))
                .reduce((s1, s2) -> s1 + s2)
                .orElse("");
        return new VarcharLiteral(s);
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
        LogicalPlan right = plan(ctx.relationPrimary());
        if (ctx.LATERAL() != null) {
            if (!(right instanceof LogicalSubQueryAlias)) {
                throw new ParseException("lateral join right table should be sub-query", ctx);
            }
        }
        return right;
    }

    @Override
    public LogicalPlan visitFromClause(FromClauseContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            LogicalPlan left = null;
            for (RelationContext relation : ctx.relation()) {
                // build left deep join tree
                LogicalPlan right = visitRelation(relation);
                left = (left == null) ? right :
                        new LogicalJoin<>(
                                JoinType.CROSS_JOIN,
                                ExpressionUtils.EMPTY_CONDITION,
                                ExpressionUtils.EMPTY_CONDITION,
                                left,
                                right);
                left = withJoinRelations(left, relation);
                // TODO: pivot and lateral view
            }
            return left;
        });
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
            // TODO(wj): isNullFirst
            boolean isNullFirst = true;
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

    private LogicalPlan withQueryOrganization(LogicalPlan inputPlan, QueryOrganizationContext ctx) {
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
                if (input instanceof LogicalSort) {
                    offset = Long.parseLong(offsetToken.getText());
                } else {
                    throw new ParseException("OFFSET requires an ORDER BY clause", limitCtx.get());
                }
            }
            return new LogicalLimit<>(limit, offset, input);
        });
    }

    private UnboundOneRowRelation withOneRowRelation(SelectClauseContext selectCtx) {
        return ParserUtils.withOrigin(selectCtx, () -> {
            List<NamedExpression> projects = getNamedExpressions(selectCtx.namedExpressionSeq());
            return new UnboundOneRowRelation(projects);
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
            // TODO: add lateral views

            // from -> where -> group by -> having -> select

            LogicalPlan filter = withFilter(inputRelation, whereClause);
            LogicalPlan aggregate = withAggregate(filter, selectClause, aggClause);
            // TODO: replace and process having at this position
            LogicalPlan having = withHaving(aggregate, havingClause);
            LogicalPlan projection = withProjection(having, selectClause, aggClause);
            return withSelectHint(projection, selectClause.selectHint());
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
            } else {
                joinType = JoinType.INNER_JOIN;
            }

            // TODO: natural join, lateral join, using join, union join
            JoinCriteriaContext joinCriteria = join.joinCriteria();
            Optional<Expression> condition;
            if (joinCriteria == null) {
                condition = Optional.empty();
            } else {
                condition = Optional.ofNullable(getExpression(joinCriteria.booleanExpression()));
            }

            last = new LogicalJoin<>(joinType, ExpressionUtils.EMPTY_CONDITION,
                    condition.map(ExpressionUtils::extractConjunction)
                            .orElse(ExpressionUtils.EMPTY_CONDITION),
                    last, plan(join.relationPrimary()));
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
                String parameterName = kv.key.getText();
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

    private LogicalPlan withProjection(LogicalPlan input, SelectClauseContext selectCtx,
                                       Optional<AggClauseContext> aggCtx) {
        return ParserUtils.withOrigin(selectCtx, () -> {
            // TODO: skip if havingClause exists
            if (aggCtx.isPresent()) {
                return input;
            } else {
                List<NamedExpression> projects = getNamedExpressions(selectCtx.namedExpressionSeq());
                return new LogicalProject<>(projects, input);
            }
        });
    }

    private LogicalPlan withFilter(LogicalPlan input, Optional<WhereClauseContext> whereCtx) {
        return input.optionalMap(whereCtx, () ->
            new LogicalFilter<>(getExpression((whereCtx.get().booleanExpression())), input)
        );
    }

    private LogicalPlan withAggregate(LogicalPlan input, SelectClauseContext selectCtx,
                                      Optional<AggClauseContext> aggCtx) {
        return input.optionalMap(aggCtx, () -> {
            List<Expression> groupByExpressions = visit(aggCtx.get().groupByItem().expression(), Expression.class);
            List<NamedExpression> namedExpressions = getNamedExpressions(selectCtx.namedExpressionSeq());
            return new LogicalAggregate<>(groupByExpressions, namedExpressions, input);
        });
    }

    private LogicalPlan withHaving(LogicalPlan input, Optional<HavingClauseContext> havingCtx) {
        return input.optionalMap(havingCtx, () -> {
            if (!(input instanceof LogicalAggregate)) {
                throw new ParseException("Having clause should be applied against an aggregation.", havingCtx.get());
            }
            return new LogicalHaving<>(getExpression((havingCtx.get().booleanExpression())), input);
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
                    outExpression = new Between(
                            valueExpression,
                            getExpression(ctx.lower),
                            getExpression(ctx.upper)
                    );
                    break;
                case DorisParser.LIKE:
                    outExpression = new Like(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
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

    public List<Expression> withInList(PredicateContext ctx) {
        return ctx.expression().stream().map(this::getExpression).collect(ImmutableList.toImmutableList());
    }

    @Override
    public DecimalLiteral visitDecimalLiteral(DecimalLiteralContext ctx) {
        return new DecimalLiteral(new BigDecimal(ctx.getText()));
    }
}
