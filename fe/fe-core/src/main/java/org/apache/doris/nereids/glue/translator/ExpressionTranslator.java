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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Arithmetic;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionType;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;

import java.util.ArrayList;
import java.util.List;

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
        Expr staleExpr =  expression.accept(INSTANCE, context);
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
        return new org.apache.doris.analysis.CompoundPredicate(
                org.apache.doris.analysis.CompoundPredicate.Operator.NOT,
                not.child(0).accept(this, context),
                null);
    }

    @Override
    public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
        return context.findSlotRef(slotReference.getExprId());
    }

    /**
     * translate to stale literal.
     */
    @Override
    public Expr visitLiteral(Literal literal, PlanTranslatorContext context) {
        DataType dataType = literal.getDataType();
        if (dataType instanceof BooleanType) {
            return new BoolLiteral((Boolean) literal.getValue());
        } else if (dataType instanceof DoubleType) {
            return new FloatLiteral((Double) literal.getValue(), Type.DOUBLE);
        } else if (dataType instanceof IntegerType) {
            return new IntLiteral((Integer) literal.getValue());
        } else if (dataType instanceof NullType) {
            return new NullLiteral();
        } else if (dataType instanceof StringType) {
            return new StringLiteral((String) literal.getValue());
        }
        throw new RuntimeException(String.format("Unsupported data type: %s", dataType.toString()));
    }

    @Override
    public Expr visitBetween(Between between, PlanTranslatorContext context) {
        throw new RuntimeException("Unexpected invocation");
    }

    @Override
    public Expr visitCompoundPredicate(CompoundPredicate compoundPredicate, PlanTranslatorContext context) {
        ExpressionType nodeType = compoundPredicate.getType();
        org.apache.doris.analysis.CompoundPredicate.Operator staleOp;
        switch (nodeType) {
            case OR:
                staleOp = org.apache.doris.analysis.CompoundPredicate.Operator.OR;
                break;
            case AND:
                staleOp = org.apache.doris.analysis.CompoundPredicate.Operator.AND;
                break;
            case NOT:
                staleOp = org.apache.doris.analysis.CompoundPredicate.Operator.NOT;
                break;
            default:
                throw new AnalysisException(String.format("Unknown node type: %s", nodeType.name()));
        }
        return new org.apache.doris.analysis.CompoundPredicate(staleOp,
                compoundPredicate.child(0).accept(this, context),
                compoundPredicate.child(1).accept(this, context));
    }

    // TODO: Supports for `distinct`
    @Override
    public Expr visitBoundFunction(BoundFunction function, PlanTranslatorContext context) {
        List<Expr> paramList = new ArrayList<>();
        for (Expression expr : function.getArguments()) {
            paramList.add(expr.accept(this, context));
        }
        return new FunctionCallExpr(function.getName(), paramList);
    }

    @Override
    public Expr visitArithmetic(Arithmetic arithmetic, PlanTranslatorContext context) {
        Arithmetic.ArithmeticOperator arithmeticOperator = arithmetic.getArithmeticOperator();
        return new ArithmeticExpr(arithmeticOperator.getStaleOp(),
                arithmetic.child(0).accept(this, context),
                arithmeticOperator.isBinary() ? arithmetic.child(1).accept(this, context) : null);
    }
}
