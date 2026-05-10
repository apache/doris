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

package org.apache.doris.analysis;

/**
 * Abstract visitor base class for {@link Expr} and all 39 concrete subclasses.
 * Follows the same {@code <R, C>} pattern as
 * {@code org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor}.
 *
 * <p>Each typed {@code visitXxx} method defaults to delegating upward to
 * {@link #visit(Expr, Object)}, so concrete visitors only need to override
 * the methods they care about.
 *
 * @param <R> return type of each visit method
 * @param <C> context type passed through each visit call
 */
public abstract class ExprVisitor<R, C> {

    /**
     * Catch-all method. All typed {@code visitXxx} methods ultimately delegate here
     * unless overridden by a concrete visitor.
     */
    public abstract R visit(Expr expr, C context);

    // -----------------------------------------------------------------------
    // Literals
    // -----------------------------------------------------------------------

    public R visitBoolLiteral(BoolLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitStringLiteral(StringLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitIntLiteral(IntLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitLargeIntLiteral(LargeIntLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitFloatLiteral(FloatLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitDecimalLiteral(DecimalLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitDateLiteral(DateLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitTimeV2Literal(TimeV2Literal expr, C context) {
        return visit(expr, context);
    }

    public R visitNullLiteral(NullLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitMaxLiteral(MaxLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitJsonLiteral(JsonLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitIPv4Literal(IPv4Literal expr, C context) {
        return visit(expr, context);
    }

    public R visitIPv6Literal(IPv6Literal expr, C context) {
        return visit(expr, context);
    }

    public R visitVarBinaryLiteral(VarBinaryLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitArrayLiteral(ArrayLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitMapLiteral(MapLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitStructLiteral(StructLiteral expr, C context) {
        return visit(expr, context);
    }

    public R visitPlaceHolderExpr(PlaceHolderExpr expr, C context) {
        return visit(expr, context);
    }

    // -----------------------------------------------------------------------
    // Reference / slot expressions
    // -----------------------------------------------------------------------

    public R visitSlotRef(SlotRef expr, C context) {
        return visit(expr, context);
    }

    public R visitColumnRefExpr(ColumnRefExpr expr, C context) {
        return visit(expr, context);
    }

    public R visitInformationFunction(InformationFunction expr, C context) {
        return visit(expr, context);
    }

    public R visitEncryptKeyRef(EncryptKeyRef expr, C context) {
        return visit(expr, context);
    }

    public R visitVariableExpr(VariableExpr expr, C context) {
        return visit(expr, context);
    }

    // -----------------------------------------------------------------------
    // Predicates
    // -----------------------------------------------------------------------

    public R visitBinaryPredicate(BinaryPredicate expr, C context) {
        return visit(expr, context);
    }

    public R visitIsNullPredicate(IsNullPredicate expr, C context) {
        return visit(expr, context);
    }

    public R visitCompoundPredicate(CompoundPredicate expr, C context) {
        return visit(expr, context);
    }

    public R visitInPredicate(InPredicate expr, C context) {
        return visit(expr, context);
    }

    public R visitLikePredicate(LikePredicate expr, C context) {
        return visit(expr, context);
    }

    public R visitMatchPredicate(MatchPredicate expr, C context) {
        return visit(expr, context);
    }

    public R visitBetweenPredicate(BetweenPredicate expr, C context) {
        return visit(expr, context);
    }

    public R visitSearchPredicate(SearchPredicate expr, C context) {
        return visit(expr, context);
    }

    // -----------------------------------------------------------------------
    // Arithmetic / cast
    // -----------------------------------------------------------------------

    public R visitArithmeticExpr(ArithmeticExpr expr, C context) {
        return visit(expr, context);
    }

    public R visitCastExpr(CastExpr expr, C context) {
        return visit(expr, context);
    }

    public R visitTryCastExpr(TryCastExpr expr, C context) {
        return visit(expr, context);
    }

    public R visitTimestampArithmeticExpr(TimestampArithmeticExpr expr, C context) {
        return visit(expr, context);
    }

    // -----------------------------------------------------------------------
    // Functions / lambda / case
    // -----------------------------------------------------------------------

    public R visitFunctionCallExpr(FunctionCallExpr expr, C context) {
        return visit(expr, context);
    }

    public R visitLambdaFunctionCallExpr(LambdaFunctionCallExpr expr, C context) {
        return visit(expr, context);
    }

    public R visitLambdaFunctionExpr(LambdaFunctionExpr expr, C context) {
        return visit(expr, context);
    }

    public R visitCaseExpr(CaseExpr expr, C context) {
        return visit(expr, context);
    }
}
