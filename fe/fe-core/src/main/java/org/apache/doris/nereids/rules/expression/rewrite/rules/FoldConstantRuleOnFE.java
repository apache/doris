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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
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
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.VariableDesc;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Database;
import org.apache.doris.nereids.trees.expressions.functions.scalar.User;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * evaluate an expression on fe.
 */
public class FoldConstantRuleOnFE extends AbstractExpressionRewriteRule {
    public static final FoldConstantRuleOnFE INSTANCE = new FoldConstantRuleOnFE();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof AggregateFunction && ((AggregateFunction) expr).isDistinct()) {
            return expr;
        } else if (expr instanceof AggregateExpression && ((AggregateExpression) expr).getFunction().isDistinct()) {
            return expr;
        }

        expr = rewriteChildren(expr, ctx);
        if (expr instanceof PropagateNullable && argsHasNullLiteral(expr)) {
            return new NullLiteral(expr.getDataType());
        }
        return expr.accept(this, ctx);
    }

    /**
     * process constant expression.
     */
    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext ctx) {
        return expr;
    }

    @Override
    public Expression visitSlot(Slot slot, ExpressionRewriteContext context) {
        return slot;
    }

    @Override
    public Expression visitLiteral(Literal literal, ExpressionRewriteContext context) {
        return literal;
    }

    @Override
    public Expression visitEqualTo(EqualTo equalTo, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(equalTo)) {
            return equalTo;
        }
        return BooleanLiteral.of(((Literal) equalTo.left()).compareTo((Literal) equalTo.right()) == 0);
    }

    @Override
    public Expression visitGreaterThan(GreaterThan greaterThan, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(greaterThan)) {
            return greaterThan;
        }
        return BooleanLiteral.of(((Literal) greaterThan.left()).compareTo((Literal) greaterThan.right()) > 0);
    }

    @Override
    public Expression visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(greaterThanEqual)) {
            return greaterThanEqual;
        }
        return BooleanLiteral.of(((Literal) greaterThanEqual.left())
                .compareTo((Literal) greaterThanEqual.right()) >= 0);
    }

    @Override
    public Expression visitLessThan(LessThan lessThan, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(lessThan)) {
            return lessThan;
        }
        return BooleanLiteral.of(((Literal) lessThan.left()).compareTo((Literal) lessThan.right()) < 0);
    }

    @Override
    public Expression visitLessThanEqual(LessThanEqual lessThanEqual, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(lessThanEqual)) {
            return lessThanEqual;
        }
        return BooleanLiteral.of(((Literal) lessThanEqual.left()).compareTo((Literal) lessThanEqual.right()) <= 0);
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(nullSafeEqual)) {
            return nullSafeEqual;
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
        if (!allArgsIsAllLiteral(not)) {
            return not;
        }
        return BooleanLiteral.of(!((BooleanLiteral) not.child()).getValue());
    }

    @Override
    public Expression visitDatabase(Database database, ExpressionRewriteContext context) {
        String res = ClusterNamespace.getNameFromFullName(context.connectContext.getDatabase());
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitCurrentUser(CurrentUser currentUser, ExpressionRewriteContext context) {
        String res = context.connectContext.get().getCurrentUserIdentity().toString();
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitUser(User user, ExpressionRewriteContext context) {
        String res = context.connectContext.get().getUserIdentity().toString();
        return new VarcharLiteral(res);
    }

    @Override
    public Expression visitConnectionId(ConnectionId connectionId, ExpressionRewriteContext context) {
        return new BigIntLiteral(context.connectContext.get().getConnectionId());
    }

    @Override
    public Expression visitAnd(And and, ExpressionRewriteContext context) {
        if (and.getArguments().stream().anyMatch(BooleanLiteral.FALSE::equals)) {
            return BooleanLiteral.FALSE;
        }
        if (argsHasNullLiteral(and)) {
            return Literal.of(null);
        }
        List<Expression> nonTrueLiteral = and.children()
                .stream()
                .filter(conjunct -> !BooleanLiteral.TRUE.equals(conjunct))
                .collect(ImmutableList.toImmutableList());
        if (nonTrueLiteral.isEmpty()) {
            return BooleanLiteral.TRUE;
        }
        if (nonTrueLiteral.size() == and.arity()) {
            return and;
        }
        if (nonTrueLiteral.size() == 1) {
            return nonTrueLiteral.get(0);
        }
        return and.withChildren(nonTrueLiteral);
    }

    @Override
    public Expression visitOr(Or or, ExpressionRewriteContext context) {
        if (or.getArguments().stream().anyMatch(BooleanLiteral.TRUE::equals)) {
            return BooleanLiteral.TRUE;
        }
        if (ExpressionUtils.isAllNullLiteral(or.getArguments())) {
            return Literal.of(null);
        }
        List<Expression> nonFalseLiteral = or.children()
                .stream()
                .filter(conjunct -> !BooleanLiteral.FALSE.equals(conjunct))
                .collect(ImmutableList.toImmutableList());
        if (nonFalseLiteral.isEmpty()) {
            return BooleanLiteral.FALSE;
        }
        if (nonFalseLiteral.size() == or.arity()) {
            return or;
        }
        if (nonFalseLiteral.size() == 1) {
            return nonFalseLiteral.get(0);
        }
        return or.withChildren(nonFalseLiteral);
    }

    @Override
    public Expression visitLike(Like like, ExpressionRewriteContext context) {
        return like;
    }

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(cast)) {
            return cast;
        }
        Expression child = cast.child();
        // todo: process other null case
        if (child.isNullLiteral()) {
            return new NullLiteral(cast.getDataType());
        }
        try {
            return child.castTo(cast.getDataType());
        } catch (Throwable t) {
            return cast;
        }
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        //functions, like current_date, do not have arg
        if (boundFunction.getArguments().isEmpty()) {
            return boundFunction;
        }
        if (!ExpressionUtils.isAllLiteral(boundFunction.getArguments())) {
            return boundFunction;
        }
        return ExpressionEvaluator.INSTANCE.eval(boundFunction);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, ExpressionRewriteContext context) {
        return ExpressionEvaluator.INSTANCE.eval(binaryArithmetic);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
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

        Expression defaultResult = caseWhen.getDefaultValue().isPresent() ? rewrite(caseWhen.getDefaultValue().get(),
                context) : null;
        if (foundNewDefault) {
            defaultResult = newDefault;
        }
        if (whenClauses.isEmpty()) {
            return defaultResult == null ? Literal.of(null) : defaultResult;
        }
        if (defaultResult == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, defaultResult);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        Expression value = inPredicate.child(0);
        if (value.isNullLiteral()) {
            return Literal.of(null);
        }

        boolean valueIsLiteral = value.isLiteral();
        if (!valueIsLiteral) {
            return inPredicate;
        }

        for (Expression item : inPredicate.getOptions()) {
            if (valueIsLiteral && value.equals(item)) {
                return BooleanLiteral.TRUE;
            }
        }
        return BooleanLiteral.FALSE;
    }

    @Override
    public Expression visitIsNull(IsNull isNull, ExpressionRewriteContext context) {
        if (!allArgsIsAllLiteral(isNull)) {
            return isNull;
        }
        return Literal.of(isNull.child().nullable());
    }

    @Override
    public Expression visitVariableDesc(VariableDesc variableDesc, ExpressionRewriteContext context) {
        Preconditions.checkArgument(variableDesc.isSystemVariable());
        return new StringLiteral(VariableMgr.getValue(context.connectContext.getSessionVariable(), variableDesc));

    }

    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        return ExpressionEvaluator.INSTANCE.eval(arithmetic);
    }

    private Expression rewriteChildren(Expression expr, ExpressionRewriteContext ctx) {
        List<Expression> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Expression child : expr.children()) {
            Expression newChild = rewrite(child, ctx);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? expr.withChildren(newChildren) : expr;
    }

    private boolean allArgsIsAllLiteral(Expression expression) {
        return ExpressionUtils.isAllLiteral(expression.getArguments());
    }

    private boolean argsHasNullLiteral(Expression expression) {
        return ExpressionUtils.hasNullLiteral(expression.getArguments());
    }
}

