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

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'linear_histogram'.
 */
public class LinearHistogram extends AggregateFunction implements ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                .args(AnyDataType.INSTANCE_WITHOUT_INDEX, DoubleType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                .args(AnyDataType.INSTANCE_WITHOUT_INDEX, DoubleType.INSTANCE, DoubleType.INSTANCE)
    );

    public LinearHistogram(Expression arg0, Expression arg1) {
        super(FunctionSet.LINEAR_HISTOGRAM, arg0, arg1);
    }

    public LinearHistogram(Expression arg0, Expression arg1, Expression arg2) {
        super(FunctionSet.LINEAR_HISTOGRAM, arg0, arg1, arg2);
    }

    private LinearHistogram(boolean distinct, List<Expression> args) {
        super(FunctionSet.LINEAR_HISTOGRAM, distinct, args);
    }

    public LinearHistogram(boolean distinct, Expression arg0, Expression arg1) {
        super(FunctionSet.LINEAR_HISTOGRAM, distinct, arg0, arg1);
    }

    public LinearHistogram(boolean distinct, Expression arg0, Expression arg1, Expression arg2) {
        super(FunctionSet.LINEAR_HISTOGRAM, distinct, arg0, arg1, arg2);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!(child(0).getDataType() instanceof PrimitiveType)) {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
        }
    }

    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        return new LinearHistogram(distinct, children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLinearHistogram(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
