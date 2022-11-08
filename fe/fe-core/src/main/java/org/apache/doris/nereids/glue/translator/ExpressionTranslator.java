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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.AssertNumRowsElement.Assertion;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CaseWhenClause;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Regexp;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Used to translate expression of new optimizer to stale expr.
 */
@SuppressWarnings("rawtypes")
public class ExpressionTranslator extends DefaultExpressionVisitor<Expr, PlanTranslatorContext> {

    public static ExpressionTranslator INSTANCE = new ExpressionTranslator();

    /**
     * The entry function of ExpressionTranslator, call {@link Expr#finalizeForNereids()} to generate
     * some attributes using in BE.
     *
     * @param expression nereids expression
     * @param context translator context
     * @return stale planner's expr
     */
    public static Expr translate(Expression expression, PlanTranslatorContext context) {
        Expr staleExpr = expression.accept(INSTANCE, context);
        try {
            staleExpr.finalizeForNereids();
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(
                    "Translate Nereids expression to stale expression failed. " + e.getMessage(), e);
        }
        return staleExpr;
    }

    @Override
    public Expr visitAlias(Alias alias, PlanTranslatorContext context) {
        return alias.child().accept(this, context);
    }

    @Override
    public Expr visitEqualTo(EqualTo equalTo, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.EQ,
                equalTo.child(0).accept(this, context),
                equalTo.child(1).accept(this, context));
    }

    @Override
    public Expr visitGreaterThan(GreaterThan greaterThan, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.GT,
                greaterThan.child(0).accept(this, context),
                greaterThan.child(1).accept(this, context));
    }

    @Override
    public Expr visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.GE,
                greaterThanEqual.child(0).accept(this, context),
                greaterThanEqual.child(1).accept(this, context));
    }

    @Override
    public Expr visitLessThan(LessThan lessThan, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.LT,
                lessThan.child(0).accept(this, context),
                lessThan.child(1).accept(this, context));
    }

    @Override
    public Expr visitLessThanEqual(LessThanEqual lessThanEqual, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.LE,
                lessThanEqual.child(0).accept(this, context),
                lessThanEqual.child(1).accept(this, context));
    }

    @Override
    public Expr visitNullSafeEqual(NullSafeEqual nullSafeEqual, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.EQ_FOR_NULL,
                nullSafeEqual.child(0).accept(this, context),
                nullSafeEqual.child(1).accept(this, context));
    }

    @Override
    public Expr visitNot(Not not, PlanTranslatorContext context) {
        if (not.child() instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) not.child();
            List<Expr> inList = inPredicate.getOptions().stream()
                    .map(e -> translate(e, context))
                    .collect(Collectors.toList());
            return new org.apache.doris.analysis.InPredicate(
                    inPredicate.getCompareExpr().accept(this, context),
                    inList,
                    true);
        } else if (not.child() instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) not.child();
            return new BinaryPredicate(Operator.NE,
                    equalTo.child(0).accept(this, context),
                    equalTo.child(1).accept(this, context));
        } else if (not.child() instanceof InSubquery || not.child() instanceof Exists) {
            return new BoolLiteral(true);
        } else {
            return new CompoundPredicate(CompoundPredicate.Operator.NOT,
                    not.child(0).accept(this, context), null);
        }
    }

    @Override
    public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
        return context.findSlotRef(slotReference.getExprId());
    }

    @Override
    public Expr visitLiteral(Literal literal, PlanTranslatorContext context) {
        return literal.toLegacyLiteral();
    }

    @Override
    public Expr visitBetween(Between between, PlanTranslatorContext context) {
        throw new RuntimeException("Unexpected invocation");
    }

    @Override
    public Expr visitAnd(And and, PlanTranslatorContext context) {
        return new org.apache.doris.analysis.CompoundPredicate(
                org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                and.child(0).accept(this, context),
                and.child(1).accept(this, context));
    }

    @Override
    public Expr visitOr(Or or, PlanTranslatorContext context) {
        return new org.apache.doris.analysis.CompoundPredicate(
                org.apache.doris.analysis.CompoundPredicate.Operator.OR,
                or.child(0).accept(this, context),
                or.child(1).accept(this, context));
    }

    @Override
    public Expr visitLike(Like like, PlanTranslatorContext context) {
        return new org.apache.doris.analysis.LikePredicate(
                LikePredicate.Operator.LIKE,
                like.left().accept(this, context),
                like.right().accept(this, context));
    }

    @Override
    public Expr visitRegexp(Regexp regexp, PlanTranslatorContext context) {
        return new org.apache.doris.analysis.LikePredicate(
                LikePredicate.Operator.REGEXP,
                regexp.left().accept(this, context),
                regexp.right().accept(this, context));
    }

    @Override
    public Expr visitCaseWhen(CaseWhen caseWhen, PlanTranslatorContext context) {
        List<CaseWhenClause> caseWhenClauses = new ArrayList<>();
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            caseWhenClauses.add(new CaseWhenClause(
                    whenClause.left().accept(this, context),
                    whenClause.right().accept(this, context)
            ));
        }
        Expr elseExpr = null;
        Optional<Expression> defaultValue = caseWhen.getDefaultValue();
        if (defaultValue.isPresent()) {
            elseExpr = defaultValue.get().accept(this, context);
        }
        return new CaseExpr(null, caseWhenClauses, elseExpr);
    }

    @Override
    public Expr visitCast(Cast cast, PlanTranslatorContext context) {
        // left child of cast is expression, right child of cast is target type
        return new CastExpr(cast.getDataType().toCatalogDataType(),
                cast.child().accept(this, context), null);
    }

    @Override
    public Expr visitInPredicate(InPredicate inPredicate, PlanTranslatorContext context) {
        List<Expr> inList = inPredicate.getOptions().stream()
                .map(e -> translate(e, context))
                .collect(Collectors.toList());
        return new org.apache.doris.analysis.InPredicate(inPredicate.getCompareExpr().accept(this, context),
                inList,
                false);
    }

    // TODO: Supports for `distinct`
    @Override
    @Developing("Generate FunctionCallExpr and Function without analyze/finalize")
    public Expr visitAggregateFunction(AggregateFunction function, PlanTranslatorContext context) {
        // inputTypesBeforeDissemble is used to find the origin function's input type before disassemble aggregate.
        //
        // For example, 'double avg(int)' will be disassembled to 'varchar avg(int)' and 'double avg(varchar)'
        // which the varchar contains sum(double) and count(int).
        //
        // We save the origin input type 'int' for the global aggregate 'varchar avg(int)', and get it in the
        // 'inputTypesBeforeDissemble' variable, so we can find the catalog function 'avg(int)' in **frontend**.
        //
        // Vectorized engine in backend will find the 'avg(int)' function, and forwarding to the correct global
        // aggregate function 'double avg(varchar)' by FunctionCallExpr.isMergeAggFn.
        Optional<List<Type>> inputTypesBeforeDissemble = function.inputTypesBeforeDissemble()
                .map(types -> types.stream()
                        .map(DataType::toCatalogDataType)
                        .collect(Collectors.toList())
                );

        // We should change the global aggregate function's temporary input type(varchar) to the origin input type.
        //
        // For example: the global aggregate function expression 'avg(slotRef(type=varchar))' of the origin function
        // 'avg(int)' should change to 'avg(slotRef(type=int))', because FunctionCallExpr will be converted to thrift
        // format, and compute signature string by the children's type, we should pass the signature 'avg(int)' to
        // **backend**. If we pass 'avg(varchar)' to backend, it will throw an exception: 'Agg Function avg is not
        // implemented'.
        List<Expr> catalogParams = new ArrayList<>();
        for (int i = 0; i < function.arity(); i++) {
            Expr catalogExpr = function.child(i).accept(this, context);
            if (catalogExpr instanceof SlotRef && inputTypesBeforeDissemble.isPresent()
                    // count(*) in local aggregate contains empty children
                    // but contains one child in global aggregate: 'count(count(*))'.
                    // so the size of inputTypesBeforeDissemble maybe less than global aggregate param.
                    && inputTypesBeforeDissemble.get().size() > i) {
                SlotRef intermediateSlot = (SlotRef) catalogExpr.clone();
                // change the slot type to origin input type
                intermediateSlot.setType(inputTypesBeforeDissemble.get().get(i));
                catalogExpr = intermediateSlot;
            }
            catalogParams.add(catalogExpr);
        }

        boolean distinct = function.isDistinct();
        FunctionParams aggFnParams = new FunctionParams(distinct, catalogParams);

        if (function instanceof Count) {
            Count count = (Count) function;
            if (count.isStar()) {
                return new FunctionCallExpr(function.getName(), FunctionParams.createStarParam(),
                        aggFnParams, inputTypesBeforeDissemble);
            } else if (count.isDistinct()) {
                return new FunctionCallExpr(function.getName(), new FunctionParams(distinct, catalogParams),
                        aggFnParams, inputTypesBeforeDissemble);
            }
        }
        return new FunctionCallExpr(function.getName(), new FunctionParams(distinct, catalogParams),
                aggFnParams, inputTypesBeforeDissemble);
    }

    @Override
    public Expr visitScalarFunction(ScalarFunction function, PlanTranslatorContext context) {
        List<Expr> arguments = function.getArguments()
                .stream().map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());
        List<Type> argTypes = function.expectedInputTypes().stream()
                .map(AbstractDataType::toCatalogDataType)
                .collect(Collectors.toList());

        NullableMode nullableMode = function.nullable()
                ? NullableMode.ALWAYS_NULLABLE
                : NullableMode.ALWAYS_NOT_NULLABLE;

        Function catalogFunction = new Function(new FunctionName(function.getName()), argTypes,
                function.getDataType().toCatalogDataType(), function.hasVarArguments(), true, nullableMode);

        // create catalog FunctionCallExpr without analyze again
        return new FunctionCallExpr(catalogFunction.getFunctionName(), catalogFunction,
                new FunctionParams(false, arguments));
    }

    @Override
    public Expr visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, PlanTranslatorContext context) {
        return new ArithmeticExpr(binaryArithmetic.getLegacyOperator(),
                binaryArithmetic.child(0).accept(this, context),
                binaryArithmetic.child(1).accept(this, context));
    }

    @Override
    public Expr visitTimestampArithmetic(TimestampArithmetic arithmetic, PlanTranslatorContext context) {
        if (arithmetic.getFuncName() == null) {
            return new TimestampArithmeticExpr(arithmetic.getOp(), arithmetic.left().accept(this, context),
                    arithmetic.right().accept(this, context), arithmetic.getTimeUnit().toString(),
                    arithmetic.isIntervalFirst(), arithmetic.getDataType().toCatalogDataType());
        } else {
            return new TimestampArithmeticExpr(arithmetic.getFuncName(), arithmetic.left().accept(this, context),
                    arithmetic.right().accept(this, context), arithmetic.getTimeUnit().toString(),
                    arithmetic.getDataType().toCatalogDataType());
        }
    }

    public static org.apache.doris.analysis.AssertNumRowsElement translateAssert(
            AssertNumRowsElement assertNumRowsElement) {
        return new org.apache.doris.analysis.AssertNumRowsElement(assertNumRowsElement.getDesiredNumOfRows(),
                assertNumRowsElement.getSubqueryString(), translateAssertion(assertNumRowsElement.getAssertion()));
    }

    private static org.apache.doris.analysis.AssertNumRowsElement.Assertion translateAssertion(
            AssertNumRowsElement.Assertion assertion) {
        switch (assertion) {
            case EQ:
                return Assertion.EQ;
            case NE:
                return Assertion.NE;
            case LT:
                return Assertion.LT;
            case LE:
                return Assertion.LE;
            case GT:
                return Assertion.GT;
            case GE:
                return Assertion.GE;
            default:
                throw new AnalysisException("UnSupported type: " + assertion);
        }
    }
}
