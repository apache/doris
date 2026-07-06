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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/** MultiDistinctArrayAgg */
public class MultiDistinctArrayAgg extends NotNullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, MultiDistinction {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(new FollowToAnyDataType(0))).args(new AnyDataType(0))
    );

    public MultiDistinctArrayAgg(Expression arg0) {
        super("multi_distinct_array_agg", arg0);
    }

    /** constructor for withChildren and reuse signature */
    private MultiDistinctArrayAgg(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MultiDistinctArrayAgg withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MultiDistinctArrayAgg(getFunctionParams(false, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMultiDistinctArrayAgg(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public Expression resultForEmptyInput() {
        return new ArrayLiteral(new ArrayList<>(), this.getDataType());
    }
}
