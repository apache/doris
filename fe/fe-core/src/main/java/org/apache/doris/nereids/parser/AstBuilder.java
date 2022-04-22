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
import org.apache.doris.nereids.DorisParser.BooleanLiteralContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.ComparisonContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.FromClauseContext;
import org.apache.doris.nereids.DorisParser.IdentifierListContext;
import org.apache.doris.nereids.DorisParser.IdentifierSeqContext;
import org.apache.doris.nereids.DorisParser.IntegerLiteralContext;
import org.apache.doris.nereids.DorisParser.JoinCriteriaContext;
import org.apache.doris.nereids.DorisParser.JoinRelationContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParser.NullLiteralContext;
import org.apache.doris.nereids.DorisParser.PredicatedContext;
import org.apache.doris.nereids.DorisParser.QualifiedNameContext;
import org.apache.doris.nereids.DorisParser.QueryContext;
import org.apache.doris.nereids.DorisParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.DorisParser.RelationContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.StringLiteralContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.clearspring.analytics.util.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Build an AST that consisting of logical plans.
 */
public class AstBuilder extends DorisParserBaseVisitor<Object> {

    /**
     * Create a logical plan using a where clause.
     */
    private final BiFunction<WhereClauseContext, LogicalPlan, LogicalPlan> withWhereClause =
            (WhereClauseContext ctx, LogicalPlan plan)
                    -> new LogicalFilter(expression((ctx.booleanExpression())), plan);

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
            return plan(ctx.queryTerm());
        };
        return ParserUtils.withOrigin(ctx, f);
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(RegularQuerySpecificationContext ctx) {
        Supplier<LogicalPlan> f = () -> {
            // TODO: support on row relation
            LogicalPlan from = visitFromClause(ctx.fromClause());
            return withSelectQuerySpecification(
                    ctx,
                    ctx.selectClause(),
                    ctx.whereClause(),
                    from);
        };
        return ParserUtils.withOrigin(ctx, f);
    }

    @Override
    public List<Expression> visitNamedExpressionSeq(NamedExpressionSeqContext ctx) {
        List<Expression> expressions = Lists.newArrayList();
        if (ctx != null) {
            for (NamedExpressionContext namedExpressionContext : ctx.namedExpression()) {
                NamedExpression namedExpression = typedVisit(namedExpressionContext);
                expressions.add(namedExpression);
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
            LogicalPlan relation) {
        Supplier<LogicalPlan> f = () -> {
            //        Filter(expression(ctx.booleanExpression), plan);
            LogicalPlan plan = visitCommonSelectQueryClausePlan(
                    relation,
                    visitNamedExpressionSeq(selectClause.namedExpressionSeq()),
                    whereClause);
            // TODO: process hint
            return plan;
        };
        return ParserUtils.withOrigin(ctx, f);
    }

    private LogicalPlan visitCommonSelectQueryClausePlan(
            LogicalPlan relation,
            List<Expression> expressions,
            WhereClauseContext whereClause) {
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
            withProject = new LogicalProject(namedExpressions, withFilter);
        } else {
            withProject = withFilter;
        }

        return withProject;
    }

    @Override
    public LogicalPlan visitFromClause(FromClauseContext ctx) {
        LogicalPlan left = null;
        for (RelationContext relation : ctx.relation()) {
            LogicalPlan right = plan(relation.relationPrimary());
            if (left == null) {
                left = right;
            } else {
                left = new LogicalJoin(JoinType.INNER_JOIN, null, left, right);
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

            last = new LogicalJoin(joinType, condition, last, plan(join.relationPrimary()));
        }
        return last;
    }

    /**
     * Create an aliased table reference. This is typically used in FROM clauses.
     */
    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        List<String> tableId = visitMultipartIdentifier(ctx.multipartIdentifier());
        UnboundRelation relation = new UnboundRelation(tableId);
        // TODO: sample and time travel, alias, sub query
        return relation;
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
                target = qualifiedNameContext.identifier().stream()
                        .map(RuleContext::getText).collect(Collectors.toList());
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
                return new BinaryPredicate(left, right, BinaryPredicate.Operator.EQ);
            case DorisParser.NSEQ:
                return new BinaryPredicate(left, right, BinaryPredicate.Operator.NSEQ);
            case DorisParser.LT:
                return new BinaryPredicate(left, right, BinaryPredicate.Operator.LT);
            case DorisParser.GT:
                return new BinaryPredicate(left, right, BinaryPredicate.Operator.GT);
            case DorisParser.LTE:
                return new BinaryPredicate(left, right, BinaryPredicate.Operator.LE);
            case DorisParser.GTE:
                return new BinaryPredicate(left, right, BinaryPredicate.Operator.GE);
            case DorisParser.NEQ:
            case DorisParser.NEQJ:
                return new BinaryPredicate(left, right, BinaryPredicate.Operator.EQ);
            default:
                return null;
        }
    }

    /**
     * Create a predicated expression. A predicated expression is a normal expression with a
     * predicate attached to it, for example:
     * {{{
     *    a + 1 IS NULL
     * }}}
     */
    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        Expression e = expression(ctx.valueExpression());
        // TODO: add predicate(is not null ...)
        return e;
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
