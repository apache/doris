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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** MultiDistinctGroupConcat */
public class MultiDistinctGroupConcat extends NullableAggregateFunction
        implements ExplicitlyCastableSignature, MultiDistinction {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).varArgs(VarcharType.SYSTEM_DEFAULT,
                    AnyDataType.INSTANCE_WITHOUT_INDEX),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).varArgs(VarcharType.SYSTEM_DEFAULT,
                    VarcharType.SYSTEM_DEFAULT, AnyDataType.INSTANCE_WITHOUT_INDEX),

            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE,
                    AnyDataType.INSTANCE_WITHOUT_INDEX),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE,
                    StringType.INSTANCE, AnyDataType.INSTANCE_WITHOUT_INDEX),

            FunctionSignature.ret(CharType.SYSTEM_DEFAULT).args(CharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(CharType.SYSTEM_DEFAULT).varArgs(CharType.SYSTEM_DEFAULT,
                    AnyDataType.INSTANCE_WITHOUT_INDEX),
            FunctionSignature.ret(CharType.SYSTEM_DEFAULT).varArgs(CharType.SYSTEM_DEFAULT,
                    CharType.SYSTEM_DEFAULT, AnyDataType.INSTANCE_WITHOUT_INDEX));

    private final boolean mustUseMultiDistinctAgg;

    /**
     * constructor with 1 argument with other arguments.
     */
    public MultiDistinctGroupConcat(Expression arg, Expression... others) {
        this(false, arg, others);
    }

    /**
     * constructor with argument list.
     */
    public MultiDistinctGroupConcat(boolean alwaysNullable, List<Expression> args) {
        this(false, alwaysNullable, args);
    }

    private MultiDistinctGroupConcat(boolean alwaysNullable, Expression arg,
            Expression... others) {
        this(alwaysNullable, ExpressionUtils.mergeArguments(arg, others));
    }

    private MultiDistinctGroupConcat(boolean mustUseMultiDistinctAgg, boolean alwaysNullable, List<Expression> args) {
        super("multi_distinct_group_concat", false, alwaysNullable, args);
        checkArguments(children);
        this.mustUseMultiDistinctAgg = mustUseMultiDistinctAgg;
    }

    @Override
    public boolean nullable() {
        return alwaysNullable || children().stream()
                .anyMatch(expression -> !(expression instanceof OrderExpression) && expression.nullable());
    }

    @Override
    public MultiDistinctGroupConcat withAlwaysNullable(boolean alwaysNullable) {
        return new MultiDistinctGroupConcat(mustUseMultiDistinctAgg, alwaysNullable, children);
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public MultiDistinctGroupConcat withDistinctAndChildren(boolean distinct, List<Expression> children) {
        checkArguments(children);
        return new MultiDistinctGroupConcat(mustUseMultiDistinctAgg, alwaysNullable, children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMultiDistinctGroupConcat(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    private void checkArguments(List<Expression> children) {
        Preconditions.checkArgument(children().size() >= 1, "children's size should >= 1");
        boolean foundOrderExpr = false;
        int firstOrderExrIndex = 0;
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            if (child instanceof OrderExpression) {
                foundOrderExpr = true;
            } else if (!foundOrderExpr) {
                firstOrderExrIndex++;
            } else {
                throw new AnalysisException(
                        "invalid multi_distinct_group_concat parameters: " + children);
            }
        }

        if (firstOrderExrIndex > 2) {
            throw new AnalysisException(
                    "multi_distinct_group_concat requires one or two parameters: " + children);
        }
    }

    @Override
    public boolean mustUseMultiDistinctAgg() {
        return mustUseMultiDistinctAgg || children.stream().anyMatch(OrderExpression.class::isInstance);
    }

    @Override
    public Expression withMustUseMultiDistinctAgg(boolean mustUseMultiDistinctAgg) {
        return new MultiDistinctGroupConcat(mustUseMultiDistinctAgg, alwaysNullable, children);
    }
}
