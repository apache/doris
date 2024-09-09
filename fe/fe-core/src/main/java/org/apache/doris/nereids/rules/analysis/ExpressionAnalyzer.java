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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.SetType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BoundStar;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.UdfBuilder;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.qe.VariableVarConverters;
import org.apache.doris.qe.cache.CacheAnalyzer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** ExpressionAnalyzer */
public class ExpressionAnalyzer extends SubExprAnalyzer<ExpressionRewriteContext> {
    @VisibleForTesting
    public static final AbstractExpressionRewriteRule FUNCTION_ANALYZER_RULE = new AbstractExpressionRewriteRule() {
        @Override
        public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
            return new ExpressionAnalyzer(
                    null, new Scope(ImmutableList.of()), null, false, false
            ).analyze(expr, ctx);
        }
    };

    private final Plan currentPlan;
    /*
    bounded={table.a, a}
    unbound=a
    if enableExactMatch, 'a' is bound to bounded 'a',
    if not enableExactMatch, 'a' is ambiguous
    in order to be compatible to original planner,
    exact match mode is not enabled for having clause
    but enabled for order by clause
    TODO after remove original planner, always enable exact match mode.
     */
    private final boolean enableExactMatch;
    private final boolean bindSlotInOuterScope;
    private final boolean wantToParseSqlFromSqlCache;
    private boolean currentInLambda;
    private boolean hasNondeterministic;

    /** ExpressionAnalyzer */
    public ExpressionAnalyzer(Plan currentPlan, Scope scope,
            @Nullable CascadesContext cascadesContext, boolean enableExactMatch, boolean bindSlotInOuterScope) {
        super(scope, cascadesContext);
        this.currentPlan = currentPlan;
        this.enableExactMatch = enableExactMatch;
        this.bindSlotInOuterScope = bindSlotInOuterScope;
        this.wantToParseSqlFromSqlCache = cascadesContext != null
                && CacheAnalyzer.canUseSqlCache(cascadesContext.getConnectContext().getSessionVariable());
    }

    /** analyzeFunction */
    public static Expression analyzeFunction(
            @Nullable LogicalPlan plan, @Nullable CascadesContext cascadesContext, Expression expression) {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(plan, new Scope(ImmutableList.of()),
                cascadesContext, false, false);
        return analyzer.analyze(
                expression,
                cascadesContext == null ? null : new ExpressionRewriteContext(cascadesContext)
        );
    }

    public Expression analyze(Expression expression) {
        CascadesContext cascadesContext = getCascadesContext();
        return analyze(expression, cascadesContext == null ? null : new ExpressionRewriteContext(cascadesContext));
    }

    /** analyze */
    public Expression analyze(Expression expression, ExpressionRewriteContext context) {
        hasNondeterministic = false;
        Expression analyzeResult = expression.accept(this, context);
        if (wantToParseSqlFromSqlCache && hasNondeterministic
                && context.cascadesContext.getStatementContext().getSqlCacheContext().isPresent()) {
            hasNondeterministic = false;
            StatementContext statementContext = context.cascadesContext.getStatementContext();
            SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().get();
            Expression foldNondeterministic = new FoldConstantRuleOnFE(true) {
                @Override
                public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
                    Expression fold = super.visitBoundFunction(boundFunction, context);
                    boolean unfold = !fold.isDeterministic();
                    if (unfold) {
                        sqlCacheContext.setCannotProcessExpression(true);
                    }
                    if (!boundFunction.isDeterministic() && !unfold) {
                        sqlCacheContext.addFoldNondeterministicPair(boundFunction, fold);
                    }
                    return fold;
                }
            }.rewrite(analyzeResult, context);

            sqlCacheContext.addFoldFullNondeterministicPair(analyzeResult, foldNondeterministic);
            return foldNondeterministic;
        }
        return analyzeResult;
    }

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext context) {
        expr = super.visit(expr, context);

        expr.checkLegalityBeforeTypeCoercion();
        // this cannot be removed, because some function already construct in parser.
        if (expr instanceof ImplicitCastInputTypes) {
            List<DataType> expectedInputTypes = ((ImplicitCastInputTypes) expr).expectedInputTypes();
            if (!expectedInputTypes.isEmpty()) {
                return TypeCoercionUtils.implicitCastInputTypes(expr, expectedInputTypes);
            }
        }
        return expr;
    }

    @Override
    public Expression visitLambda(Lambda lambda, ExpressionRewriteContext context) {
        boolean originInLambda = currentInLambda;
        try {
            currentInLambda = true;
            return super.visitLambda(lambda, context);
        } finally {
            currentInLambda = originInLambda;
        }
    }

    /* ********************************************************************************************
     * bind slot
     * ******************************************************************************************** */
    @Override
    public Expression visitUnboundVariable(UnboundVariable unboundVariable, ExpressionRewriteContext context) {
        return resolveUnboundVariable(unboundVariable);
    }

    /** resolveUnboundVariable */
    public static Variable resolveUnboundVariable(UnboundVariable unboundVariable) throws AnalysisException {
        String name = unboundVariable.getName();
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        Literal literal = null;
        if (unboundVariable.getType() == VariableType.DEFAULT) {
            literal = VariableMgr.getLiteral(sessionVariable, name, SetType.DEFAULT);
        } else if (unboundVariable.getType() == VariableType.SESSION) {
            literal = VariableMgr.getLiteral(sessionVariable, name, SetType.SESSION);
        } else if (unboundVariable.getType() == VariableType.GLOBAL) {
            literal = VariableMgr.getLiteral(sessionVariable, name, SetType.GLOBAL);
        } else if (unboundVariable.getType() == VariableType.USER) {
            literal = ConnectContext.get().getLiteralForUserVar(name);
        }
        if (literal == null) {
            throw new AnalysisException("Unsupported system variable: " + unboundVariable.getName());
        }
        if (!Strings.isNullOrEmpty(name) && VariableVarConverters.hasConverter(name)) {
            try {
                Preconditions.checkArgument(literal instanceof IntegerLikeLiteral);
                IntegerLikeLiteral integerLikeLiteral = (IntegerLikeLiteral) literal;
                literal = new StringLiteral(VariableVarConverters.decode(name, integerLikeLiteral.getLongValue()));
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage());
            }
        }
        return new Variable(unboundVariable.getName(), unboundVariable.getType(), literal);
    }

    @Override
    public Expression visitUnboundAlias(UnboundAlias unboundAlias, ExpressionRewriteContext context) {
        Expression child = unboundAlias.child().accept(this, context);
        if (unboundAlias.getAlias().isPresent()) {
            return new Alias(child, unboundAlias.getAlias().get());
            // TODO: the variant bind element_at(slot, 'name') will return a slot, and we should
            //       assign an Alias to this function, this is trick and should refactor it
        } else if (!(unboundAlias.child() instanceof ElementAt) && child instanceof NamedExpression) {
            return new Alias(child, ((NamedExpression) child).getName());
        } else {
            return new Alias(child);
        }
    }

    @Override
    public Expression visitUnboundSlot(UnboundSlot unboundSlot, ExpressionRewriteContext context) {
        Optional<Scope> outerScope = getScope().getOuterScope();
        Optional<List<? extends Expression>> boundedOpt = Optional.of(bindSlotByThisScope(unboundSlot));
        boolean foundInThisScope = !boundedOpt.get().isEmpty();
        // Currently only looking for symbols on the previous level.
        if (bindSlotInOuterScope && !foundInThisScope && outerScope.isPresent()) {
            boundedOpt = Optional.of(bindSlotByScope(unboundSlot, outerScope.get()));
        }
        List<? extends Expression> bounded = boundedOpt.get();
        switch (bounded.size()) {
            case 0:
                if (!currentInLambda) {
                    String tableName = StringUtils.join(unboundSlot.getQualifier(), ".");
                    if (tableName.isEmpty()) {
                        tableName = "table list";
                    }
                    couldNotFoundColumn(unboundSlot, tableName);
                }
                return unboundSlot;
            case 1:
                Expression firstBound = bounded.get(0);
                if (!foundInThisScope && firstBound instanceof Slot
                        && !outerScope.get().getCorrelatedSlots().contains(firstBound)) {
                    outerScope.get().getCorrelatedSlots().add((Slot) firstBound);
                }
                return firstBound;
            default:
                if (enableExactMatch) {
                    // select t1.k k, t2.k
                    // from t1 join t2 order by k
                    //
                    // 't1.k k' is denoted by alias_k, its full name is 'k'
                    // 'order by k' is denoted as order_k, it full name is 'k'
                    // 't2.k' in select list, its full name is 't2.k'
                    //
                    // order_k can be bound on alias_k and t2.k
                    // alias_k is exactly matched, since its full name is exactly match full name of order_k
                    // t2.k is not exactly matched, since t2.k's full name is larger than order_k
                    List<Slot> exactMatch = bounded.stream()
                            .filter(Slot.class::isInstance)
                            .map(Slot.class::cast)
                            .filter(bound -> unboundSlot.getNameParts().size() == bound.getQualifier().size() + 1)
                            .collect(Collectors.toList());
                    if (exactMatch.size() == 1) {
                        return exactMatch.get(0);
                    }
                }
                throw new AnalysisException(String.format("%s is ambiguous: %s.",
                        unboundSlot.toSql(),
                        bounded.stream()
                                .map(Expression::toString)
                                .collect(Collectors.joining(", "))));
        }
    }

    protected void couldNotFoundColumn(UnboundSlot unboundSlot, String tableName) {
        String message = "Unknown column '"
                + unboundSlot.getNameParts().get(unboundSlot.getNameParts().size() - 1)
                + "' in '" + tableName;
        if (currentPlan != null) {
            message += "' in " + currentPlan.getType().toString().substring("LOGICAL_".length()) + " clause";
        }
        throw new AnalysisException(message);
    }

    @Override
    public Expression visitUnboundStar(UnboundStar unboundStar, ExpressionRewriteContext context) {
        List<String> qualifier = unboundStar.getQualifier();
        boolean showHidden = Util.showHiddenColumns();
        List<Slot> slots = getScope().getSlots()
                .stream()
                .filter(slot -> !(slot instanceof SlotReference)
                        || (((SlotReference) slot).isVisible()) || showHidden)
                .collect(Collectors.toList());
        switch (qualifier.size()) {
            case 0: // select *
                return new BoundStar(slots);
            case 1: // select table.*
            case 2: // select db.table.*
            case 3: // select catalog.db.table.*
                return bindQualifiedStar(qualifier, slots);
            default:
                throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
        }
    }

    /* ********************************************************************************************
     * bind function
     * ******************************************************************************************** */
    @Override
    public Expression visitUnboundFunction(UnboundFunction unboundFunction, ExpressionRewriteContext context) {
        if (unboundFunction.isHighOrder()) {
            unboundFunction = bindHighOrderFunction(unboundFunction, context);
        } else {
            unboundFunction = (UnboundFunction) super.visit(unboundFunction, context);
        }

        // bind function
        FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
        List<Object> arguments = unboundFunction.isDistinct()
                ? ImmutableList.builderWithExpectedSize(unboundFunction.arity() + 1)
                    .add(unboundFunction.isDistinct())
                    .addAll(unboundFunction.getArguments())
                    .build()
                : (List) unboundFunction.getArguments();

        String dbName = unboundFunction.getDbName();
        if (StringUtils.isEmpty(dbName)) {
            // we will change arithmetic function like add(), subtract(), bitnot()
            // to the corresponding objects rather than BoundFunction.
            ArithmeticFunctionBinder functionBinder = new ArithmeticFunctionBinder();
            if (functionBinder.isBinaryArithmetic(unboundFunction.getName())) {
                return functionBinder.bindBinaryArithmetic(unboundFunction.getName(), unboundFunction.children())
                        .accept(this, context);
            }
        }

        String functionName = unboundFunction.getName();
        FunctionBuilder builder = functionRegistry.findFunctionBuilder(
                dbName, functionName, arguments);
        // for create view stmt
        if (builder instanceof UdfBuilder) {
            unboundFunction.getIndexInSqlString().ifPresent(index -> {
                ConnectContext.get().getStatementContext().addIndexInSqlToString(index,
                        Utils.qualifiedNameWithBackquote(ImmutableList.of(null == dbName
                                ? ConnectContext.get().getDatabase() : dbName, functionName)));
            });
        }

        Pair<? extends Expression, ? extends BoundFunction> buildResult = builder.build(functionName, arguments);
        buildResult.second.checkOrderExprIsValid();
        Optional<SqlCacheContext> sqlCacheContext = Optional.empty();
        if (wantToParseSqlFromSqlCache) {
            StatementContext statementContext = context.cascadesContext.getStatementContext();
            if (!buildResult.second.isDeterministic()) {
                hasNondeterministic = true;
            }
            sqlCacheContext = statementContext.getSqlCacheContext();
            if (builder instanceof AliasUdfBuilder
                    || buildResult.second instanceof JavaUdf || buildResult.second instanceof JavaUdaf) {
                if (sqlCacheContext.isPresent()) {
                    sqlCacheContext.get().setCannotProcessExpression(true);
                }
            }
        }
        if (builder instanceof AliasUdfBuilder) {
            if (sqlCacheContext.isPresent()) {
                sqlCacheContext.get().setCannotProcessExpression(true);
            }
            // we do type coercion in build function in alias function, so it's ok to return directly.
            return buildResult.first;
        } else {
            Expression castFunction = TypeCoercionUtils.processBoundFunction((BoundFunction) buildResult.first);
            if (castFunction instanceof Count
                    && context != null
                    && context.cascadesContext.getOuterScope().isPresent()
                    && !context.cascadesContext.getOuterScope().get().getCorrelatedSlots().isEmpty()) {
                // consider sql: SELECT * FROM t1 WHERE t1.a <= (SELECT COUNT(t2.a) FROM t2 WHERE (t1.b = t2.b));
                // when unnest correlated subquery, we create a left join node.
                // outer query is left table and subquery is right one
                // if there is no match, the row from right table is filled with nulls
                // but COUNT function is always not nullable.
                // so wrap COUNT with Nvl to ensure it's result is 0 instead of null to get the correct result
                castFunction = new Nvl(castFunction, new BigIntLiteral(0));
            }
            return castFunction;
        }
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        boundFunction = (BoundFunction) super.visitBoundFunction(boundFunction, context);
        return TypeCoercionUtils.processBoundFunction(boundFunction);
    }

    /**
     * gets the method for calculating the time.
     * e.g. YEARS_ADD、YEARS_SUB、DAYS_ADD 、DAYS_SUB
     */
    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        Expression left = arithmetic.left().accept(this, context);
        Expression right = arithmetic.right().accept(this, context);

        arithmetic = (TimestampArithmetic) arithmetic.withChildren(left, right);
        // bind function
        String funcOpName;
        if (arithmetic.getFuncName() == null) {
            // e.g. YEARS_ADD, MONTHS_SUB
            funcOpName = String.format("%sS_%s", arithmetic.getTimeUnit(),
                    (arithmetic.getOp() == Operator.ADD) ? "ADD" : "SUB");
        } else {
            funcOpName = arithmetic.getFuncName();
        }
        arithmetic = (TimestampArithmetic) arithmetic.withFuncName(funcOpName.toLowerCase(Locale.ROOT));

        // type coercion
        return TypeCoercionUtils.processTimestampArithmetic(arithmetic);
    }

    /* ********************************************************************************************
     * type coercion
     * ******************************************************************************************** */

    @Override
    public Expression visitBitNot(BitNot bitNot, ExpressionRewriteContext context) {
        Expression child = bitNot.child().accept(this, context);
        // type coercion
        if (!(child.getDataType().isIntegralType() || child.getDataType().isBooleanType())) {
            child = new Cast(child, BigIntType.INSTANCE);
        }
        return bitNot.withChildren(child);
    }

    @Override
    public Expression visitDivide(Divide divide, ExpressionRewriteContext context) {
        Expression left = divide.left().accept(this, context);
        Expression right = divide.right().accept(this, context);
        divide = (Divide) divide.withChildren(left, right);
        // type coercion
        return TypeCoercionUtils.processDivide(divide);
    }

    @Override
    public Expression visitIntegralDivide(IntegralDivide integralDivide, ExpressionRewriteContext context) {
        Expression left = integralDivide.left().accept(this, context);
        Expression right = integralDivide.right().accept(this, context);
        integralDivide = (IntegralDivide) integralDivide.withChildren(left, right);
        // type coercion
        return TypeCoercionUtils.processIntegralDivide(integralDivide);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, ExpressionRewriteContext context) {
        Expression left = binaryArithmetic.left().accept(this, context);
        Expression right = binaryArithmetic.right().accept(this, context);
        binaryArithmetic = (BinaryArithmetic) binaryArithmetic.withChildren(left, right);
        return TypeCoercionUtils.processBinaryArithmetic(binaryArithmetic);
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate compoundPredicate, ExpressionRewriteContext context) {
        Expression left = compoundPredicate.left().accept(this, context);
        Expression right = compoundPredicate.right().accept(this, context);
        CompoundPredicate ret = (CompoundPredicate) compoundPredicate.withChildren(left, right);
        return TypeCoercionUtils.processCompoundPredicate(ret);
    }

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        // maybe is `not subquery`, we should bind it first
        Expression expr = super.visitNot(not, context);

        // expression is not subquery
        if (expr instanceof Not) {
            Expression child = not.child().accept(this, context);
            Expression newChild = TypeCoercionUtils.castIfNotSameType(child, BooleanType.INSTANCE);
            if (child != newChild) {
                return expr.withChildren(newChild);
            }
        }
        return expr;
    }

    @Override
    public Expression visitPlaceholder(Placeholder placeholder, ExpressionRewriteContext context) {
        if (context == null) {
            return super.visitPlaceholder(placeholder, context);
        }
        Expression realExpr = context.cascadesContext.getStatementContext()
                    .getIdToPlaceholderRealExpr().get(placeholder.getPlaceholderId());
        return visit(realExpr, context);
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        Expression left = cp.left().accept(this, context);
        Expression right = cp.right().accept(this, context);
        cp = (ComparisonPredicate) cp.withChildren(left, right);
        return TypeCoercionUtils.processComparisonPredicate(cp);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        Builder<Expression> rewrittenChildren = ImmutableList.builderWithExpectedSize(caseWhen.arity());
        for (Expression child : caseWhen.children()) {
            rewrittenChildren.add(child.accept(this, context));
        }
        CaseWhen newCaseWhen = caseWhen.withChildren(rewrittenChildren.build());
        newCaseWhen.checkLegalityBeforeTypeCoercion();
        return TypeCoercionUtils.processCaseWhen(newCaseWhen);
    }

    @Override
    public Expression visitWhenClause(WhenClause whenClause, ExpressionRewriteContext context) {
        return whenClause.withChildren(TypeCoercionUtils.castIfNotSameType(
                        whenClause.getOperand().accept(this, context), BooleanType.INSTANCE),
                whenClause.getResult().accept(this, context));
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        List<Expression> rewrittenChildren = inPredicate.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        InPredicate newInPredicate = inPredicate.withChildren(rewrittenChildren);
        return TypeCoercionUtils.processInPredicate(newInPredicate);
    }

    @Override
    public Expression visitInSubquery(InSubquery inSubquery, ExpressionRewriteContext context) {
        // analyze subquery
        inSubquery = (InSubquery) super.visitInSubquery(inSubquery, context);

        // compareExpr already analyze when invoke super.visitInSubquery
        Expression newCompareExpr = inSubquery.getCompareExpr();
        // but ListQuery does not analyze
        Expression newListQuery = inSubquery.getListQuery().accept(this, context);

        ComparisonPredicate afterTypeCoercion = (ComparisonPredicate) TypeCoercionUtils.processComparisonPredicate(
                new EqualTo(newCompareExpr, newListQuery));
        if (newListQuery.getDataType().isBitmapType()) {
            if (!newCompareExpr.getDataType().isBigIntType()) {
                newCompareExpr = new Cast(newCompareExpr, BigIntType.INSTANCE);
            }
        } else {
            newCompareExpr = afterTypeCoercion.left();
        }
        return new InSubquery(newCompareExpr, (ListQuery) afterTypeCoercion.right(),
                inSubquery.getCorrelateSlots(), ((ListQuery) afterTypeCoercion.right()).getTypeCoercionExpr(),
                inSubquery.isNot());
    }

    @Override
    public Expression visitMatch(Match match, ExpressionRewriteContext context) {
        Expression left = match.left().accept(this, context);
        Expression right = match.right().accept(this, context);
        // check child type
        if (!left.getDataType().isStringLikeType()
                && !(left.getDataType() instanceof ArrayType
                && ((ArrayType) left.getDataType()).getItemType().isStringLikeType())
                && !left.getDataType().isVariantType()) {
            throw new AnalysisException(String.format(
                    "left operand '%s' part of predicate "
                            + "'%s' should return type 'STRING', 'ARRAY<STRING> or VARIANT' but "
                            + "returns type '%s'.",
                    left.toSql(), match.toSql(), left.getDataType()));
        }

        if (!right.getDataType().isStringLikeType() && !right.getDataType().isNullType()) {
            throw new AnalysisException(String.format(
                    "right operand '%s' part of predicate " + "'%s' should return type 'STRING' but "
                            + "returns type '%s'.",
                    right.toSql(), match.toSql(), right.getDataType()));
        }

        if (left.getDataType().isVariantType()) {
            left = new Cast(left, right.getDataType());
        }
        return match.withChildren(left, right);
    }

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        cast = (Cast) super.visitCast(cast, context);
        // NOTICE: just for compatibility with legacy planner.
        if (cast.child().getDataType().isComplexType() || cast.getDataType().isComplexType()) {
            TypeCoercionUtils.checkCanCastTo(cast.child().getDataType(), cast.getDataType());
        }
        return cast;
    }

    private BoundStar bindQualifiedStar(List<String> qualifierStar, List<Slot> boundSlots) {
        // FIXME: compatible with previous behavior:
        // https://github.com/apache/doris/pull/10415/files/3fe9cb0c3f805ab3a9678033b281b16ad93ec60a#r910239452
        List<Slot> slots = boundSlots.stream().filter(boundSlot -> {
            switch (qualifierStar.size()) {
                // table.*
                case 1:
                    List<String> boundSlotQualifier = boundSlot.getQualifier();
                    switch (boundSlotQualifier.size()) {
                        // bound slot is `column` and no qualified
                        case 0:
                            return false;
                        case 1: // bound slot is `table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0));
                        case 2:// bound slot is `db`.`table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(1));
                        case 3:// bound slot is `catalog`.`db`.`table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(2));
                        default:
                            throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, "."));
                    }
                case 2: // db.table.*
                    boundSlotQualifier = boundSlot.getQualifier();
                    switch (boundSlotQualifier.size()) {
                        // bound slot is `column` and no qualified
                        case 0:
                        case 1: // bound slot is `table`.`column`
                            return false;
                        case 2:// bound slot is `db`.`table`.`column`
                            return compareDbNameIgnoreClusterName(qualifierStar.get(0), boundSlotQualifier.get(0))
                                    && qualifierStar.get(1).equalsIgnoreCase(boundSlotQualifier.get(1));
                        case 3:// bound slot is `catalog`.`db`.`table`.`column`
                            return compareDbNameIgnoreClusterName(qualifierStar.get(0), boundSlotQualifier.get(1))
                                    && qualifierStar.get(1).equalsIgnoreCase(boundSlotQualifier.get(2));
                        default:
                            throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, ".") + ".*");
                    }
                case 3: // catalog.db.table.*
                    boundSlotQualifier = boundSlot.getQualifier();
                    switch (boundSlotQualifier.size()) {
                        // bound slot is `column` and no qualified
                        case 0:
                        case 1: // bound slot is `table`.`column`
                        case 2: // bound slot is `db`.`table`.`column`
                            return false;
                        case 3:// bound slot is `catalog`.`db`.`table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0))
                                    && compareDbNameIgnoreClusterName(qualifierStar.get(1), boundSlotQualifier.get(1))
                                    && qualifierStar.get(2).equalsIgnoreCase(boundSlotQualifier.get(2));
                        default:
                            throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, ".") + ".*");
                    }
                default:
                    throw new AnalysisException("Not supported name: "
                            + StringUtils.join(qualifierStar, ".") + ".*");
            }
        }).collect(Collectors.toList());

        if (slots.isEmpty()) {
            throw new AnalysisException("unknown qualifier: " + StringUtils.join(qualifierStar, ".") + ".*");
        }
        return new BoundStar(slots);
    }

    protected List<? extends Expression> bindSlotByThisScope(UnboundSlot unboundSlot) {
        return bindSlotByScope(unboundSlot, getScope());
    }

    protected List<Slot> bindExactSlotsByThisScope(UnboundSlot unboundSlot, Scope scope) {
        List<Slot> candidates = bindSlotByScope(unboundSlot, scope);
        if (candidates.size() == 1) {
            return candidates;
        }
        List<Slot> extractSlots = Utils.filterImmutableList(candidates, bound ->
                unboundSlot.getNameParts().size() == bound.getQualifier().size() + 1
        );
        // we should return origin candidates slots if extract slots is empty,
        // and then throw an ambiguous exception
        return !extractSlots.isEmpty() ? extractSlots : candidates;
    }

    private List<Slot> addSqlIndexInfo(List<Slot> slots, Optional<Pair<Integer, Integer>> indexInSql) {
        if (!indexInSql.isPresent()) {
            return slots;
        }
        List<Slot> newSlots = new ArrayList<>();
        for (Slot slot : slots) {
            newSlots.add(slot.withIndexInSql(indexInSql.get()));
        }
        return newSlots;
    }

    /** bindSlotByScope */
    public List<Slot> bindSlotByScope(UnboundSlot unboundSlot, Scope scope) {
        List<String> nameParts = unboundSlot.getNameParts();
        Optional<Pair<Integer, Integer>> idxInSql = unboundSlot.getIndexInSqlString();
        int namePartSize = nameParts.size();
        switch (namePartSize) {
            // column
            case 1: {
                return addSqlIndexInfo(bindSingleSlotByName(nameParts.get(0), scope), idxInSql);
            }
            // table.column
            case 2: {
                return addSqlIndexInfo(bindSingleSlotByTable(nameParts.get(0), nameParts.get(1), scope), idxInSql);
            }
            // db.table.column
            case 3: {
                return addSqlIndexInfo(bindSingleSlotByDb(nameParts.get(0), nameParts.get(1), nameParts.get(2), scope),
                        idxInSql);
            }
            // catalog.db.table.column
            case 4: {
                return addSqlIndexInfo(bindSingleSlotByCatalog(
                        nameParts.get(0), nameParts.get(1), nameParts.get(2), nameParts.get(3), scope), idxInSql);
            }
            default: {
                throw new AnalysisException("Not supported name: " + StringUtils.join(nameParts, "."));
            }
        }
    }

    public static boolean compareDbName(String boundedDbName, String unBoundDbName) {
        return unBoundDbName.equalsIgnoreCase(boundedDbName);
    }

    public static boolean sameTableName(String boundSlot, String unboundSlot) {
        if (GlobalVariable.lowerCaseTableNames != 1) {
            return boundSlot.equals(unboundSlot);
        } else {
            return boundSlot.equalsIgnoreCase(unboundSlot);
        }
    }

    private UnboundFunction bindHighOrderFunction(UnboundFunction unboundFunction, ExpressionRewriteContext context) {
        int childrenSize = unboundFunction.children().size();
        List<Expression> subChildren = new ArrayList<>();
        for (int i = 1; i < childrenSize; i++) {
            subChildren.add(unboundFunction.child(i).accept(this, context));
        }

        // bindLambdaFunction
        Lambda lambda = (Lambda) unboundFunction.children().get(0);
        Expression lambdaFunction = lambda.getLambdaFunction();
        List<ArrayItemReference> arrayItemReferences = lambda.makeArguments(subChildren);

        List<Slot> boundedSlots = arrayItemReferences.stream()
                .map(ArrayItemReference::toSlot)
                .collect(ImmutableList.toImmutableList());

        ExpressionAnalyzer lambdaAnalyzer = new ExpressionAnalyzer(currentPlan, new Scope(boundedSlots),
                context == null ? null : context.cascadesContext, true, false) {
            @Override
            protected void couldNotFoundColumn(UnboundSlot unboundSlot, String tableName) {
                throw new AnalysisException("Unknown lambda slot '"
                        + unboundSlot.getNameParts().get(unboundSlot.getNameParts().size() - 1)
                        + " in lambda arguments" + lambda.getLambdaArgumentNames());
            }
        };
        lambdaFunction = lambdaAnalyzer.analyze(lambdaFunction, context);

        Lambda lambdaClosure = lambda.withLambdaFunctionArguments(lambdaFunction, arrayItemReferences);

        // We don't add the ArrayExpression in high order function at all
        return unboundFunction.withChildren(ImmutableList.of(lambdaClosure));
    }

    private boolean shouldBindSlotBy(int namePartSize, Slot boundSlot) {
        return namePartSize <= boundSlot.getQualifier().size() + 1;
    }

    private List<Slot> bindSingleSlotByName(String name, Scope scope) {
        int namePartSize = 1;
        Builder<Slot> usedSlots = ImmutableList.builderWithExpectedSize(1);
        for (Slot boundSlot : scope.findSlotIgnoreCase(name)) {
            if (!shouldBindSlotBy(namePartSize, boundSlot)) {
                continue;
            }
            // set sql case as alias
            usedSlots.add(boundSlot.withName(name));
        }
        return usedSlots.build();
    }

    private List<Slot> bindSingleSlotByTable(String table, String name, Scope scope) {
        int namePartSize = 2;
        Builder<Slot> usedSlots = ImmutableList.builderWithExpectedSize(1);
        for (Slot boundSlot : scope.findSlotIgnoreCase(name)) {
            if (!shouldBindSlotBy(namePartSize, boundSlot)) {
                continue;
            }
            List<String> boundSlotQualifier = boundSlot.getQualifier();
            String boundSlotTable = boundSlotQualifier.get(boundSlotQualifier.size() - 1);
            if (!sameTableName(boundSlotTable, table)) {
                continue;
            }
            // set sql case as alias
            usedSlots.add(boundSlot.withName(name));
        }
        return usedSlots.build();
    }

    private List<Slot> bindSingleSlotByDb(String db, String table, String name, Scope scope) {
        int namePartSize = 3;
        Builder<Slot> usedSlots = ImmutableList.builderWithExpectedSize(1);
        for (Slot boundSlot : scope.findSlotIgnoreCase(name)) {
            if (!shouldBindSlotBy(namePartSize, boundSlot)) {
                continue;
            }
            List<String> boundSlotQualifier = boundSlot.getQualifier();
            String boundSlotDb = boundSlotQualifier.get(boundSlotQualifier.size() - 2);
            String boundSlotTable = boundSlotQualifier.get(boundSlotQualifier.size() - 1);
            if (!compareDbNameIgnoreClusterName(boundSlotDb, db) || !sameTableName(boundSlotTable, table)) {
                continue;
            }
            // set sql case as alias
            usedSlots.add(boundSlot.withName(name));
        }
        return usedSlots.build();
    }

    private List<Slot> bindSingleSlotByCatalog(String catalog, String db, String table, String name, Scope scope) {
        int namePartSize = 4;
        Builder<Slot> usedSlots = ImmutableList.builderWithExpectedSize(1);
        for (Slot boundSlot : scope.findSlotIgnoreCase(name)) {
            if (!shouldBindSlotBy(namePartSize, boundSlot)) {
                continue;
            }
            List<String> boundSlotQualifier = boundSlot.getQualifier();
            String boundSlotCatalog = boundSlotQualifier.get(boundSlotQualifier.size() - 3);
            String boundSlotDb = boundSlotQualifier.get(boundSlotQualifier.size() - 2);
            String boundSlotTable = boundSlotQualifier.get(boundSlotQualifier.size() - 1);
            if (!boundSlotCatalog.equalsIgnoreCase(catalog)
                    || !compareDbNameIgnoreClusterName(boundSlotDb, db)
                    || !sameTableName(boundSlotTable, table)) {
                continue;
            }
            // set sql case as alias
            usedSlots.add(boundSlot.withName(name));
        }
        return usedSlots.build();
    }

    /**compareDbNameIgnoreClusterName.*/
    public static boolean compareDbNameIgnoreClusterName(String name1, String name2) {
        if (name1.equalsIgnoreCase(name2)) {
            return true;
        }
        String ignoreClusterName1 = name1;
        int idx1 = name1.indexOf(":");
        if (idx1 > -1) {
            ignoreClusterName1 = name1.substring(idx1 + 1);
        }
        String ignoreClusterName2 = name2;
        int idx2 = name2.indexOf(":");
        if (idx2 > -1) {
            ignoreClusterName2 = name2.substring(idx2 + 1);
        }
        return ignoreClusterName1.equalsIgnoreCase(ignoreClusterName2);
    }
}
