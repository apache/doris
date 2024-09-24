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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.window.SupportWindowAnalytic;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'regr_count'.
 */
public class RegrCount extends AggregateFunction
        implements BinaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable, SupportWindowAnalytic {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE)
                    .args(AnyDataType.INSTANCE_WITHOUT_INDEX, AnyDataType.INSTANCE_WITHOUT_INDEX)
    );

    /**
     * constructor with 2 argument.
     */
    public RegrCount(Expression arg1, Expression arg2) {
        this(false, arg1, arg2);
    }

    /**
     * constructor with 3 argument.
     */
    public RegrCount(boolean distinct, Expression arg1, Expression arg2) {
        super("regr_count", distinct, arg1, arg2);
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new RegrCount(distinct, left(), right());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRegrCount(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
