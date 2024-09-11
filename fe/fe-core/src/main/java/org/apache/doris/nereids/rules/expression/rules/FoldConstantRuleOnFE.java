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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionListenerMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionTraverseListener;
import org.apache.doris.nereids.rules.expression.ExpressionTraverseListenerFactory;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentCatalog;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Database;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncryptKeyRef;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Password;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SessionUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.User;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Version;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * evaluate an expression on fe.
 */
public class FoldConstantRuleOnFE extends AbstractExpressionRewriteRule
        implements ExpressionPatternRuleFactory, ExpressionTraverseListenerFactory {

    public static final FoldConstantRuleOnFE VISITOR_INSTANCE = new FoldConstantRuleOnFE(true);
    public static final FoldConstantRuleOnFE PATTERN_MATCH_INSTANCE = new FoldConstantRuleOnFE(false);

    // record whether current expression is in an aggregate function with distinct,
    // if is, we will skip to fold constant
    private static final ListenAggDistinct LISTEN_AGG_DISTINCT = new ListenAggDistinct();
    private static final CheckWhetherUnderAggDistinct NOT_UNDER_AGG_DISTINCT = new CheckWhetherUnderAggDistinct();

    private final boolean deepRewrite;

    public FoldConstantRuleOnFE(boolean deepRewrite) {
        this.deepRewrite = deepRewrite;
    }

    public static Expression evaluate(Expression expression, ExpressionRewriteContext expressionRewriteContext) {
        return VISITOR_INSTANCE.rewrite(expression, expressionRewriteContext);
    }

    @Override
    public List<ExpressionListenerMatcher<? extends Expression>> buildListeners() {
        return ImmutableList.of(
                listenerType(AggregateFunction.class)
                        .when(AggregateFunction::isDistinct)
                        .then(LISTEN_AGG_DISTINCT.as()),

                listenerType(AggregateExpression.class)
                        .when(AggregateExpression::isDistinct)
                        .then(LISTEN_AGG_DISTINCT.as())
        );
    }

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matches(EncryptKeyRef.class, this::visitEncryptKeyRef),
                matches(EqualTo.class, this::visitEqualTo),
                matches(GreaterThan.class, this::visitGreaterThan),
                matches(GreaterThanEqual.class, this::visitGreaterThanEqual),
                matches(LessThan.class, this::visitLessThan),
                matches(LessThanEqual.class, this::visitLessThanEqual),
                matches(NullSafeEqual.class, this::visitNullSafeEqual),
                matches(Not.class, this::visitNot),
                matches(Database.class, this::visitDatabase),
                matches(CurrentUser.class, this::visitCurrentUser),
                matches(CurrentCatalog.class, this::visitCurrentCatalog),
                matches(User.class, this::visitUser),
                matches(ConnectionId.class, this::visitConnectionId),
                matches(And.class, this::visitAnd),
                matches(Or.class, this::visitOr),
                matches(Cast.class, this::visitCast),
                matches(BoundFunction.class, this::visitBoundFunction),
                matches(BinaryArithmetic.class, this::visitBinaryArithmetic),
                matches(CaseWhen.class, this::visitCaseWhen),
                matches(If.class, this::visitIf),
                matches(InPredicate.class, this::visitInPredicate),
                matches(IsNull.class, this::visitIsNull),
                matches(TimestampArithmetic.class, this::visitTimestampArithmetic),
                matches(Password.class, this::visitPassword),
                matches(Array.class, this::visitArray),
                matches(Date.class, this::visitDate),
                matches(Version.class, this::visitVersion),
                matches(SessionUser.class, this::visitSessionUser)
        );
    }

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof AggregateFunction && ((AggregateFunction) expr).isDistinct()) {
            return expr;
        } else if (expr instanceof AggregateExpression && ((AggregateExpression) expr).getFunction().isDistinct()) {
            return expr;
        }
        return expr.accept(this, ctx);
    }

    /**
     * process constant expression.
     */
    @Override
    public Expression visitSlot(Slot slot, ExpressionRewriteContext context) {
        return slot;
    }

    @Override
    public Expression visitLiteral(Literal literal, ExpressionRewriteContext context) {
        return literal;
    }

    @Override
    public Expression visitMatch(Match match, ExpressionRewriteContext context) {
        match = rewriteChildren(match, context);
        Optional<Expression> checkedExpr = preProcess(match);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return super.visitMatch(match, context);
    }

    @Override
    public Expression visitEncryptKeyRef(EncryptKeyRef encryptKeyRef, ExpressionRewriteContext context) {
        String dbName = encryptKeyRef.getDbName();
        ConnectContext connectContext = context.cascadesContext.getConnectContext();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = connectContext.getDatabase();
        }
        if ("".equals(dbName)) {
            throw new AnalysisException("DB " + dbName + "not found");
        }
        org.apache.doris.catalog.Database database =
                Env.getCurrentEnv().getInternalCatalog().getDbNullable(dbName);
        if (database == null) {
            throw new AnalysisException("DB " + dbName + "not found");
        }
        EncryptKey encryptKey = database.getEncryptKey(encryptKeyRef.getEncryptKeyName());
        if (encryptKey == null) {
            throw new AnalysisException("Can not found encryptKey" + encryptKeyRef.getEncryptKeyName());
        }
        return new StringLiteral(encryptKey.getKeyString());
    }

    @Override
    public Expression visitEqualTo(EqualTo equalTo, ExpressionRewriteContext context) {
        equalTo = rewriteChildren(equalTo, context);
        Optional<Expression> checkedExpr = preProcess(equalTo);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) equalTo.left()).compareTo((Literal) equalTo.right()) == 0);
    }

    @Override
    public Expression visitGreaterThan(GreaterThan greaterThan, ExpressionRewriteContext context) {
        greaterThan = rewriteChildren(greaterThan, context);
        Optional<Expression> checkedExpr = preProcess(greaterThan);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) greaterThan.left()).compareTo((Literal) greaterThan.right()) > 0);
    }

    @Override
    public Expression visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, ExpressionRewriteContext context) {
        greaterThanEqual = rewriteChildren(greaterThanEqual, context);
        Optional<Expression> checkedExpr = preProcess(greaterThanEqual);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) greaterThanEqual.left())
                .compareTo((Literal) greaterThanEqual.right()) >= 0);
    }

    @Override
    public Expression visitLessThan(LessThan lessThan, ExpressionRewriteContext context) {
        lessThan = rewriteChildren(lessThan, context);
        Optional<Expression> checkedExpr = preProcess(lessThan);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) lessThan.left()).compareTo((Literal) lessThan.right()) < 0);
    }

    @Override
    public Expression visitLessThanEqual(LessThanEqual lessThanEqual, ExpressionRewriteContext context) {
        lessThanEqual = rewriteChildren(lessThanEqual, context);
        Optional<Expression> checkedExpr = preProcess(lessThanEqual);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(((Literal) lessThanEqual.left()).compareTo((Literal) lessThanEqual.right()) <= 0);
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, ExpressionRewriteContext context) {
        nullSafeEqual = rewriteChildren(nullSafeEqual, context);
        Optional<Expression> checkedExpr = preProcess(nullSafeEqual);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        Literal l = (Literal) nullSafeEqual.left();
        Literal r = (Literal) nullSafeEqual.right();
        if (l.isNullLiteral() && r.isNullLiteral()) {
            return BooleanLiteral.TRUE;
        } else if (!l.isNullLiteral() && !r.isNullLiteral()) {
            return BooleanLiteral.of(l.compareTo(r) == 0);
        } else {
            return BooleanLiteral.FALSE;
        }
    }

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        not = rewriteChildren(not, context);
        Optional<Expression> checkedExpr = preProcess(not);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return BooleanLiteral.of(!((BooleanLiteral) not.child()).getValue());
    }

    @Override
    public Expression visitDatabase(Database database, ExpressionRewriteContext context) {
        String res = ClusterNamespace.getNameFromFullName(context.cascadesContext.getConnectContext().getDatabase());
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitCurrentUser(CurrentUser currentUser, ExpressionRewriteContext context) {
        String res = context.cascadesContext.getConnectContext().getCurrentUserIdentity().toString();
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitCurrentCatalog(CurrentCatalog currentCatalog, ExpressionRewriteContext context) {
        String res = context.cascadesContext.getConnectContext().getDefaultCatalog();
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitUser(User user, ExpressionRewriteContext context) {
        String res = context.cascadesContext.getConnectContext().getUserIdentity().toString();
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitSessionUser(SessionUser user, ExpressionRewriteContext context) {
        String res = context.cascadesContext.getConnectContext().getUserIdentity().toString();
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitConnectionId(ConnectionId connectionId, ExpressionRewriteContext context) {
        return new BigIntLiteral(context.cascadesContext.getConnectContext().getConnectionId());
    }

    @Override
    public Expression visitAnd(And and, ExpressionRewriteContext context) {
        List<Expression> nonTrueLiteral = Lists.newArrayList();
        int nullCount = 0;
        for (Expression e : and.children()) {
            e = deepRewrite ? e.accept(this, context) : e;
            if (BooleanLiteral.FALSE.equals(e)) {
                return BooleanLiteral.FALSE;
            } else if (e instanceof NullLiteral) {
                nullCount++;
                nonTrueLiteral.add(e);
            } else if (!BooleanLiteral.TRUE.equals(e)) {
                nonTrueLiteral.add(e);
            }
        }

        if (nullCount == 0) {
            switch (nonTrueLiteral.size()) {
                case 0:
                    // true and true
                    return BooleanLiteral.TRUE;
                case 1:
                    // true and x
                    return nonTrueLiteral.get(0);
                default:
                    // x and y
                    return and.withChildren(nonTrueLiteral);
            }
        } else if (nullCount == 1) {
            if (nonTrueLiteral.size() == 1) {
                // null and true
                return new NullLiteral(BooleanType.INSTANCE);
            }
            // null and x
            return and.withChildren(nonTrueLiteral);
        } else {
            // null and null
            return new NullLiteral(BooleanType.INSTANCE);
        }
    }

    @Override
    public Expression visitOr(Or or, ExpressionRewriteContext context) {
        List<Expression> nonFalseLiteral = Lists.newArrayList();
        int nullCount = 0;
        for (Expression e : or.children()) {
            e = deepRewrite ? e.accept(this, context) : e;
            if (BooleanLiteral.TRUE.equals(e)) {
                return BooleanLiteral.TRUE;
            } else if (e instanceof NullLiteral) {
                nullCount++;
                nonFalseLiteral.add(e);
            } else if (!BooleanLiteral.FALSE.equals(e)) {
                nonFalseLiteral.add(e);
            }
        }

        if (nullCount == 0) {
            switch (nonFalseLiteral.size()) {
                case 0:
                    // false or false
                    return BooleanLiteral.FALSE;
                case 1:
                    // false or x
                    return nonFalseLiteral.get(0);
                default:
                    // x or y
                    return or.withChildren(nonFalseLiteral);
            }
        } else if (nullCount == 1) {
            if (nonFalseLiteral.size() == 1) {
                // null or false
                return new NullLiteral(BooleanType.INSTANCE);
            }
            // null or x
            return or.withChildren(nonFalseLiteral);
        } else {
            // null or null
            return new NullLiteral(BooleanType.INSTANCE);
        }
    }

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        cast = rewriteChildren(cast, context);
        Optional<Expression> checkedExpr = preProcess(cast);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        Expression child = cast.child();
        DataType dataType = cast.getDataType();
        // todo: process other null case
        if (child.isNullLiteral()) {
            return new NullLiteral(dataType);
        } else if (child instanceof StringLikeLiteral && dataType instanceof DateLikeType) {
            try {
                return ((DateLikeType) dataType).fromString(((StringLikeLiteral) child).getStringValue());
            } catch (AnalysisException t) {
                if (cast.isExplicitType()) {
                    return new NullLiteral(dataType);
                } else {
                    // If cast is from type coercion, we don't use NULL literal and will throw exception.
                    throw t;
                }
            } catch (DateTimeException e) {
                return new NullLiteral(dataType);
            }
        }
        try {
            Expression castResult = child.checkedCastTo(dataType);
            if (!Objects.equals(castResult, cast) && !Objects.equals(castResult, child)) {
                castResult = rewrite(castResult, context);
            }
            return castResult;
        } catch (Throwable t) {
            return cast;
        }
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        if (!boundFunction.foldable()) {
            return boundFunction;
        }
        boundFunction = rewriteChildren(boundFunction, context);
        Optional<Expression> checkedExpr = preProcess(boundFunction);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return ExpressionEvaluator.INSTANCE.eval(boundFunction);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, ExpressionRewriteContext context) {
        binaryArithmetic = rewriteChildren(binaryArithmetic, context);
        Optional<Expression> checkedExpr = preProcess(binaryArithmetic);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return ExpressionEvaluator.INSTANCE.eval(binaryArithmetic);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        caseWhen = rewriteChildren(caseWhen, context);
        Expression newDefault = null;
        boolean foundNewDefault = false;

        List<WhenClause> whenClauses = new ArrayList<>();
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            Expression whenOperand = whenClause.getOperand();

            if (!(whenOperand.isLiteral())) {
                whenClauses.add(new WhenClause(whenOperand, whenClause.getResult()));
            } else if (BooleanLiteral.TRUE.equals(whenOperand)) {
                foundNewDefault = true;
                newDefault = whenClause.getResult();
                break;
            }
        }

        Expression defaultResult = null;
        if (caseWhen.getDefaultValue().isPresent()) {
            defaultResult = caseWhen.getDefaultValue().get();
            if (deepRewrite) {
                defaultResult = rewrite(defaultResult, context);
            }
        }
        if (foundNewDefault) {
            defaultResult = newDefault;
        }
        if (whenClauses.isEmpty()) {
            return defaultResult == null ? new NullLiteral(caseWhen.getDataType()) : defaultResult;
        }
        if (defaultResult == null) {
            if (caseWhen.getDataType().isNullType()) {
                // if caseWhen's type is NULL_TYPE, means all possible return values are nulls
                // it's safe to return null literal here
                return new NullLiteral();
            } else {
                return new CaseWhen(whenClauses);
            }
        }
        return new CaseWhen(whenClauses, defaultResult);
    }

    @Override
    public Expression visitIf(If ifExpr, ExpressionRewriteContext context) {
        ifExpr = rewriteChildren(ifExpr, context);
        if (ifExpr.child(0) instanceof NullLiteral || ifExpr.child(0).equals(BooleanLiteral.FALSE)) {
            return ifExpr.child(2);
        } else if (ifExpr.child(0).equals(BooleanLiteral.TRUE)) {
            return ifExpr.child(1);
        }
        return ifExpr;
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        inPredicate = rewriteChildren(inPredicate, context);
        Optional<Expression> checkedExpr = preProcess(inPredicate);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        // now the inPredicate contains literal only.
        Expression value = inPredicate.child(0);
        if (value.isNullLiteral()) {
            return new NullLiteral(BooleanType.INSTANCE);
        }
        boolean isOptionContainsNull = false;
        for (Expression item : inPredicate.getOptions()) {
            if (value.equals(item)) {
                return BooleanLiteral.TRUE;
            } else if (item.isNullLiteral()) {
                isOptionContainsNull = true;
            }
        }
        return isOptionContainsNull
                ? new NullLiteral(BooleanType.INSTANCE)
                : BooleanLiteral.FALSE;
    }

    @Override
    public Expression visitIsNull(IsNull isNull, ExpressionRewriteContext context) {
        isNull = rewriteChildren(isNull, context);
        Optional<Expression> checkedExpr = preProcess(isNull);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return Literal.of(isNull.child().nullable());
    }

    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        arithmetic = rewriteChildren(arithmetic, context);
        Optional<Expression> checkedExpr = preProcess(arithmetic);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        return ExpressionEvaluator.INSTANCE.eval(arithmetic);
    }

    @Override
    public Expression visitPassword(Password password, ExpressionRewriteContext context) {
        Preconditions.checkArgument(password.child(0) instanceof StringLikeLiteral,
                "argument of password must be string literal");
        String s = ((StringLikeLiteral) password.child()).value;
        return new StringLiteral("*" + DigestUtils.sha1Hex(
                DigestUtils.sha1(s.getBytes())).toUpperCase());
    }

    @Override
    public Expression visitArray(Array array, ExpressionRewriteContext context) {
        array = rewriteChildren(array, context);
        Optional<Expression> checkedExpr = preProcess(array);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        List<Literal> arguments = (List) array.getArguments();
        // we should pass dataType to constructor because arguments maybe empty
        return new ArrayLiteral(arguments, array.getDataType());
    }

    @Override
    public Expression visitDate(Date date, ExpressionRewriteContext context) {
        date = rewriteChildren(date, context);
        Optional<Expression> checkedExpr = preProcess(date);
        if (checkedExpr.isPresent()) {
            return checkedExpr.get();
        }
        Literal child = (Literal) date.child();
        if (child instanceof NullLiteral) {
            return new NullLiteral(date.getDataType());
        }
        DataType dataType = child.getDataType();
        if (dataType.isDateTimeType()) {
            DateTimeLiteral dateTimeLiteral = (DateTimeLiteral) child;
            return new DateLiteral(dateTimeLiteral.getYear(), dateTimeLiteral.getMonth(), dateTimeLiteral.getDay());
        } else if (dataType.isDateTimeV2Type()) {
            DateTimeV2Literal dateTimeLiteral = (DateTimeV2Literal) child;
            return new DateV2Literal(dateTimeLiteral.getYear(), dateTimeLiteral.getMonth(), dateTimeLiteral.getDay());
        }
        return date;
    }

    @Override
    public Expression visitVersion(Version version, ExpressionRewriteContext context) {
        return new StringLiteral(GlobalVariable.version);
    }

    private <E extends Expression> E rewriteChildren(E expr, ExpressionRewriteContext context) {
        if (!deepRewrite) {
            return expr;
        }
        switch (expr.arity()) {
            case 1: {
                Expression originChild = expr.child(0);
                Expression newChild = originChild.accept(this, context);
                return (originChild != newChild) ? (E) expr.withChildren(ImmutableList.of(newChild)) : expr;
            }
            case 2: {
                Expression originLeft = expr.child(0);
                Expression newLeft = originLeft.accept(this, context);
                Expression originRight = expr.child(1);
                Expression newRight = originRight.accept(this, context);
                return (originLeft != newLeft || originRight != newRight)
                        ? (E) expr.withChildren(ImmutableList.of(newLeft, newRight))
                        : expr;
            }
            case 0: {
                return expr;
            }
            default: {
                boolean hasNewChildren = false;
                Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(expr.arity());
                for (Expression child : expr.children()) {
                    Expression newChild = child.accept(this, context);
                    if (newChild != child) {
                        hasNewChildren = true;
                    }
                    newChildren.add(newChild);
                }
                return hasNewChildren ? (E) expr.withChildren(newChildren.build()) : expr;
            }
        }
    }

    private Optional<Expression> preProcess(Expression expression) {
        if (expression instanceof AggregateFunction || expression instanceof TableGeneratingFunction) {
            return Optional.of(expression);
        }
        if (expression instanceof PropagateNullable && ExpressionUtils.hasNullLiteral(expression.getArguments())) {
            return Optional.of(new NullLiteral(expression.getDataType()));
        }
        if (!ExpressionUtils.isAllLiteral(expression.getArguments())) {
            return Optional.of(expression);
        }
        return Optional.empty();
    }

    private static class ListenAggDistinct implements ExpressionTraverseListener<Expression> {
        @Override
        public void onEnter(ExpressionMatchingContext<Expression> context) {
            context.cascadesContext.incrementDistinctAggLevel();
        }

        @Override
        public void onExit(ExpressionMatchingContext<Expression> context, Expression rewritten) {
            context.cascadesContext.decrementDistinctAggLevel();
        }
    }

    private static class CheckWhetherUnderAggDistinct implements Predicate<ExpressionMatchingContext<Expression>> {
        @Override
        public boolean test(ExpressionMatchingContext<Expression> context) {
            return context.cascadesContext.getDistinctAggLevel() == 0;
        }

        public <E extends Expression> Predicate<ExpressionMatchingContext<E>> as() {
            return (Predicate) this;
        }
    }

    private <E extends Expression> ExpressionPatternMatcher<? extends Expression> matches(
            Class<E> clazz, BiFunction<E, ExpressionRewriteContext, Expression> visitMethod) {
        return matchesType(clazz)
                .whenCtx(ctx -> !ctx.cascadesContext.getConnectContext().getSessionVariable()
                        .isDebugSkipFoldConstant())
                .whenCtx(NOT_UNDER_AGG_DISTINCT.as())
                .thenApply(ctx -> visitMethod.apply(ctx.expr, ctx.rewriteContext));
    }
}
