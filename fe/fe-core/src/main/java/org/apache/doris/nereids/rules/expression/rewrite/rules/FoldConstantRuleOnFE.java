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

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
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
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * evaluate an expression on fe.
 */
public class FoldConstantRuleOnFE extends AbstractExpressionRewriteRule {
    public static final FoldConstantRuleOnFE INSTANCE = new FoldConstantRuleOnFE();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        return process(expr, ctx);
    }

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext context) {
        return expr;
    }

    /**
     * process constant expression.
     */
    public Expression process(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof PropagateNullable) {
            List<Expression> children = expr.children()
                    .stream()
                    .map(child -> process(child, ctx))
                    .collect(Collectors.toList());

            if (ExpressionUtils.hasNullLiteral(children)) {
                return Literal.of(null);
            }

            if (!ExpressionUtils.isAllLiteral(children)) {
                return expr.withChildren(children);
            }
            return expr.withChildren(children).accept(this, ctx);
        } else {
            return expr.accept(this, ctx);
        }
    }

    @Override
    public Expression visitEqualTo(EqualTo equalTo, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) equalTo.left()).compareTo((Literal) equalTo.right()) == 0);
    }

    @Override
    public Expression visitGreaterThan(GreaterThan greaterThan, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) greaterThan.left()).compareTo((Literal) greaterThan.right()) > 0);
    }

    @Override
    public Expression visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) greaterThanEqual.left())
                .compareTo((Literal) greaterThanEqual.right()) >= 0);
    }

    @Override
    public Expression visitLessThan(LessThan lessThan, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) lessThan.left()).compareTo((Literal) lessThan.right()) < 0);

    }

    @Override
    public Expression visitLessThanEqual(LessThanEqual lessThanEqual, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) lessThanEqual.left()).compareTo((Literal) lessThanEqual.right()) <= 0);
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, ExpressionRewriteContext context) {
        Expression left = process(nullSafeEqual.left(), context);
        Expression right = process(nullSafeEqual.right(), context);
        if (ExpressionUtils.isAllLiteral(left, right)) {
            Literal l = (Literal) left;
            Literal r = (Literal) right;
            if (l.isNullLiteral() && r.isNullLiteral()) {
                return BooleanLiteral.TRUE;
            } else if (!l.isNullLiteral() && !r.isNullLiteral()) {
                return BooleanLiteral.of(l.compareTo(r) == 0);
            } else {
                return BooleanLiteral.FALSE;
            }
        }
        return nullSafeEqual.withChildren(left, right);
    }

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        return BooleanLiteral.of(!((BooleanLiteral) not.child()).getValue());
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
    public Expression visitAnd(And and, ExpressionRewriteContext context) {
        List<Expression> children = Lists.newArrayList();
        for (Expression child : and.children()) {
            Expression newChild = process(child, context);
            if (newChild.equals(BooleanLiteral.FALSE)) {
                return BooleanLiteral.FALSE;
            }
            if (!newChild.equals(BooleanLiteral.TRUE)) {
                children.add(newChild);
            }
        }
        if (children.isEmpty()) {
            return BooleanLiteral.TRUE;
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        if (ExpressionUtils.isAllNullLiteral(children)) {
            return Literal.of(null);
        }
        return and.withChildren(children);
    }

    @Override
    public Expression visitOr(Or or, ExpressionRewriteContext context) {
        List<Expression> children = Lists.newArrayList();
        for (Expression child : or.children()) {
            Expression newChild = process(child, context);
            if (newChild.equals(BooleanLiteral.TRUE)) {
                return BooleanLiteral.TRUE;
            }
            if (!newChild.equals(BooleanLiteral.FALSE)) {
                children.add(newChild);
            }
        }
        if (children.isEmpty()) {
            return BooleanLiteral.FALSE;
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        if (ExpressionUtils.isAllNullLiteral(children)) {
            return Literal.of(null);
        }
        return or.withChildren(children);
    }

    @Override
    public Expression visitLike(Like like, ExpressionRewriteContext context) {
        return like;
    }

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        Expression child = process(cast.child(), context);
        // todo: process other null case
        if (child.isNullLiteral()) {
            return new NullLiteral(cast.getDataType());
        }
        if (child.isLiteral()) {
            return child.castTo(cast.getDataType());
        }
        return cast.withChildren(child);
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        //functions, like current_date, do not have arg
        if (boundFunction.getArguments().isEmpty()) {
            return boundFunction;
        }
        List<Expression> newArgs = boundFunction.getArguments().stream().map(arg -> process(arg, context))
                .collect(Collectors.toList());
        if (ExpressionUtils.isAllLiteral(newArgs)) {
            return ExpressionEvaluator.INSTANCE.eval(boundFunction.withChildren(newArgs));
        }
        return boundFunction.withChildren(newArgs);
    }

    @Override
    public Expression visitAlias(Alias alias, ExpressionRewriteContext context) {
        Expression newChild = alias.child().accept(this, context);
        if (alias.child() == newChild) {
            return alias;
        }
        return alias.withChildren(ImmutableList.of(newChild));
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
            Expression whenOperand = process(whenClause.getOperand(), context);

            if (!(whenOperand.isLiteral())) {
                whenClauses.add(new WhenClause(whenOperand, process(whenClause.getResult(), context)));
            } else if (BooleanLiteral.TRUE.equals(whenOperand)) {
                foundNewDefault = true;
                newDefault = process(whenClause.getResult(), context);
                break;
            }
        }

        Expression defaultResult;
        if (foundNewDefault) {
            defaultResult = newDefault;
        } else {
            defaultResult = process(caseWhen.getDefaultValue().orElse(Literal.of(null)), context);
        }

        if (whenClauses.isEmpty()) {
            return defaultResult;
        }
        return new CaseWhen(whenClauses, defaultResult);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        Expression value = process(inPredicate.child(0), context);
        List<Expression> children = Lists.newArrayList();
        children.add(value);
        if (value.isNullLiteral()) {
            return Literal.of(null);
        }
        boolean hasNull = false;
        boolean hasUnresolvedValue = !value.isLiteral();
        for (int i = 1; i < inPredicate.children().size(); i++) {
            Expression inValue = process(inPredicate.child(i), context);
            children.add(inValue);
            if (!inValue.isLiteral()) {
                hasUnresolvedValue = true;
            }
            if (inValue.isNullLiteral()) {
                hasNull = true;
            }
            if (inValue.isLiteral() && value.isLiteral() && ((Literal) value).compareTo((Literal) inValue) == 0) {
                return Literal.of(true);
            }
        }
        if (hasUnresolvedValue) {
            return inPredicate.withChildren(children);
        }
        return hasNull ? Literal.of(null) : Literal.of(false);
    }

    @Override
    public Expression visitIsNull(IsNull isNull, ExpressionRewriteContext context) {
        Expression child = process(isNull.child(), context);
        if (child.isNullLiteral()) {
            return Literal.of(true);
        } else if (!child.nullable()) {
            return Literal.of(false);
        }
        return isNull.withChildren(child);
    }

    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        return ExpressionEvaluator.INSTANCE.eval(arithmetic);
    }

    @Override
    public Expression visitCount(Count count, ExpressionRewriteContext context) {
        // don't break the aggregate information
        if (count.isDistinct()) {
            return count;
        }
        // distinct
        List<Expression> newArguments = count.children()
                .stream()
                .map(arg -> arg.accept(this, context))
                .distinct()
                .collect(Collectors.toList());

        count = count.withDistinctAndChildren(count.isDistinct(), newArguments);

        // TODO: add a switch to optimize aggregate function to result
        /*boolean containsNullLiteral = count.getArguments()
                .stream()
                .anyMatch(arg -> arg instanceof NullLiteral);
        if (containsNullLiteral) {
            return new BigIntLiteral(0);
        }*/

        List<Expression> literals = count.getArguments()
                .stream()
                .filter(arg -> arg instanceof Literal)
                .collect(Collectors.toList());

        // TODO: add a switch to optimize aggregate function to result
        /*if (count.isDistinct() && !literals.isEmpty() && count.arity() == literals.size()) {
            return new BigIntLiteral(1);
        }*/

        // remove literals if contains non-literal
        if (count.isDistinct() && !literals.isEmpty() && count.arity() > literals.size()) {
            List<Expression> nonLiteralArgs = count.getArguments()
                    .stream()
                    .filter(arg -> !(arg instanceof Literal))
                    .collect(Collectors.toList());
            return count.withDistinctAndChildren(count.isDistinct(), nonLiteralArgs);
        }
        return count;
    }

    @Override
    public Expression visitIf(If function, ExpressionRewriteContext context) {
        List<Expression> arguments = function.children()
                .stream()
                .map(arg -> arg.accept(this, context))
                .collect(ImmutableList.toImmutableList());
        function = function.withChildren(arguments);

        // TODO: this process will get rid of some slots in the count(distinct some slots) and break the semantics.
        //       we should find a better way to do the simplify
        /*
        if (arguments.get(0).equals(BooleanLiteral.TRUE)) {
            return arguments.get(1);
        }
        if (arguments.get(0).equals(BooleanLiteral.FALSE) || arguments.get(0) instanceof NullLiteral) {
            return arguments.get(2);
        }
         */
        return function;
    }
}

