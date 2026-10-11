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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'percentile_reservoir'
 */
public class PercentileReservoir extends NullableAggregateFunction
        implements BinaryExpression, ExplicitlyCastableSignature, NullIgnoringAggregateFunction,
        RewriteWhenAnalyze {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, DoubleType.INSTANCE)

    );

    /**
     * constructor with 2 arguments.
     */
    public PercentileReservoir(Expression arg0, Expression arg1) {
        this(false, arg0, arg1);
    }

    /**
     * constructor with 2 arguments.
     */
    public PercentileReservoir(boolean distinct, Expression arg0, Expression arg1) {
        this(distinct, false, arg0, arg1);
    }

    public PercentileReservoir(boolean distinct, boolean alwaysNullable, Expression arg0, Expression arg1) {
        super("percentile_reservoir", distinct, alwaysNullable, arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private PercentileReservoir(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkLevel(getArgument(1));
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLevel(getArgument(1));
    }

    /**
     * Execute the level literal that checkLevel() validated. BE would otherwise evaluate the constant
     * expression itself wherever it is not folded (load planning, DISTINCT, debug_skip_fold_constant),
     * and a cast such as FLOAT to DOUBLE can compute a different value there than the FE folding.
     * The level nullability is part of the agg_state layout of the _state / _combine combinators, which
     * is derived from the children again whenever the analyzer rebuilds them, so a nullable level such
     * as CAST('0.25' AS DOUBLE) keeps its nullability through a Nullable wrapper over the literal.
     */
    @Override
    public Expression rewriteWhenAnalyze() {
        Expression levelArgument = getArgument(1);
        Literal level = checkLevel(levelArgument);
        return withChildren(ImmutableList.of(getArgument(0),
                levelArgument.nullable() && !level.nullable() ? new Nullable(level) : level));
    }

    /**
     * The level must be a constant that folds to a literal in [0, 1]. It is folded here instead of
     * waiting for the rewrite phase because a constant expression such as 0.25 + 0.25 is only a
     * literal after folding, some plans (INSERT ... VALUES, load column mappings) never run the
     * rewrite phase, and constant folding can be turned off by debug_skip_fold_constant.
     * The level is brought to DOUBLE with the same implicit cast that signature coercion applies,
     * so a level that is not a valid DOUBLE behaves like the coerced expression: NULL under the
     * default non-strict cast and an error under strict cast, for '' as well as cast('' as double).
     *
     * @return the folded level literal
     */
    private Literal checkLevel(Expression levelArgument) {
        // The analyzed level and an explicit cast to another agg_state layout (ConvertAggStateCast) wrap the
        // validated level in Nullable / NonNullable, also nested and under a Cast when such casts are chained,
        // to keep the state layout. FE does not fold them, so check the value beneath them: Nullable never
        // changes it, and NonNullable only when it is not NULL, as BE rejects a NULL value there.
        Expression unwrappedLevel = levelArgument.rewriteUp(expression -> {
            if (expression instanceof Nullable) {
                return expression.child(0);
            }
            if (expression instanceof NonNullable) {
                Expression value = FoldConstantRuleOnFE.evaluateWithoutContext(expression.child(0));
                return value instanceof Literal && !(value instanceof NullLiteral) ? value : expression;
            }
            return expression;
        });
        Expression level = unwrappedLevel.isConstant()
                ? FoldConstantRuleOnFE.evaluateWithoutContext(
                        TypeCoercionUtils.castIfNotSameType(unwrappedLevel, DoubleType.INSTANCE))
                : unwrappedLevel;
        if (!(level instanceof Literal)) {
            throw new AnalysisException(
                    "percentile_reservoir requires second parameter must be a constant : " + this.toSql());
        }
        // a NULL level is skipped by the null-ignoring BE implementation and yields a NULL result
        if (level instanceof NullLiteral) {
            return (Literal) level;
        }
        double value = ((Literal) level).getDouble();
        // Negate the valid range to reject NaN, which makes both < 0 and > 1 false.
        if (!(value >= 0 && value <= 1)) {
            throw new AnalysisException(
                    "percentile_reservoir level must be in [0, 1], but got " + value + ": " + this.toSql());
        }
        return (Literal) level;
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public PercentileReservoir withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PercentileReservoir(getFunctionParams(distinct, children));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new PercentileReservoir(getAlwaysNullableFunctionParams(alwaysNullable));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPercentileReservoir(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public List<Expression> getDistinctArguments() {
        return distinct ? ImmutableList.of(getArgument(0)) : ImmutableList.of();
    }
}
