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

package org.apache.doris.nereids.trees.expressions;

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
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.plans.PlanTranslatorContext;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to convert expression of new optimizer to stale expr.
 */
@SuppressWarnings("rawtypes")
public class ExpressionConverter extends ExpressionVisitor<Expr, PlanTranslatorContext> {

    public static ExpressionConverter converter = new ExpressionConverter();

    public static Expr convert(Expression expression, PlanTranslatorContext planContext) {
        return converter.visit(expression, planContext);
    }

    @Override
    public Expr visit(Expression expr, PlanTranslatorContext context) {
        return expr.accept(this, context);
    }

    @Override
    public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
        return context.findExpr(slotReference);
    }

    @Override
    public Expr visitEqualTo(EqualTo equalTo, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.EQ,
                visit(equalTo.child(0), context),
                visit(equalTo.child(1), context));
    }

    @Override
    public Expr visitGreaterThan(GreaterThan greaterThan, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.GT,
                visit(greaterThan.child(0), context),
                visit(greaterThan.child(1), context));
    }

    @Override
    public Expr visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.GE,
                visit(greaterThanEqual.child(0), context),
                visit(greaterThanEqual.child(1), context));
    }

    @Override
    public Expr visitLessThan(LessThan lessThan, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.LT,
                visit(lessThan.child(0), context),
                visit(lessThan.child(1), context));
    }

    @Override
    public Expr visitLessThanEqual(LessThanEqual lessThanEqual, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.LE,
                visit(lessThanEqual.child(0), context),
                visit(lessThanEqual.child(1), context));
    }

    @Override
    public Expr visitNot(Not not, PlanTranslatorContext context) {
        return new org.apache.doris.analysis.CompoundPredicate(
                org.apache.doris.analysis.CompoundPredicate.Operator.NOT,
                visit(not.child(0), context),
                null);
    }

    @Override
    public Expr visitNullSafeEqual(NullSafeEqual nullSafeEqual, PlanTranslatorContext context) {
        return new BinaryPredicate(Operator.EQ_FOR_NULL,
                visit(nullSafeEqual.child(0), context),
                visit(nullSafeEqual.child(1), context));
    }

    /**
     * Convert to stale literal.
     */
    @Override
    public Expr visitLiteral(Literal literal, PlanTranslatorContext context) {
        DataType dataType = literal.getDataType();
        if (dataType instanceof BooleanType) {
            return new BoolLiteral((Boolean) literal.getValue());
        } else if (dataType instanceof DoubleType) {
            return new FloatLiteral((Double) literal.getValue(), Type.DOUBLE);
        } else if (dataType instanceof IntegerType) {
            return new IntLiteral((Long) literal.getValue());
        } else if (dataType instanceof NullType) {
            return new NullLiteral();
        } else if (dataType instanceof StringType) {
            return new StringLiteral((String) literal.getValue());
        }
        throw new RuntimeException(String.format("Unsupported data type: %s", dataType.toString()));
    }

    // TODO: Supports for `distinct`
    @Override
    public Expr visitFunctionCall(FunctionCall function, PlanTranslatorContext context) {
        List<Expr> paramList = new ArrayList<>();
        for (Expression expr : function.getFnParams().getExpressionList()) {
            paramList.add(visit(expr, context));
        }
        return new FunctionCallExpr(function.getFnName().toString(), paramList);
    }

    @Override
    public Expr visitBetweenPredicate(BetweenPredicate betweenPredicate, PlanTranslatorContext context) {
        throw new RuntimeException("Unexpected invocation");
    }

    @Override
    public Expr visitCompoundPredicate(CompoundPredicate compoundPredicate, PlanTranslatorContext context) {
        NodeType nodeType = compoundPredicate.getType();
        org.apache.doris.analysis.CompoundPredicate.Operator staleOp = null;
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
                throw new RuntimeException(String.format("Unknown node type: %s", nodeType.name()));
        }
        return new org.apache.doris.analysis.CompoundPredicate(staleOp,
                visit(compoundPredicate.child(0), context),
                visit(compoundPredicate.child(1), context));
    }

    @Override
    public Expr visitArithmetic(Arithmetic arithmetic, PlanTranslatorContext context) {
        Arithmetic.ArithmeticOperator arithmeticOperator = arithmetic.getArithOperator();
        return new ArithmeticExpr(arithmeticOperator.getStaleOp(),
                visit(arithmetic.child(0), context),
                arithmeticOperator.isBinary() ? visit(arithmetic.child(1), context) : null);
    }

}
