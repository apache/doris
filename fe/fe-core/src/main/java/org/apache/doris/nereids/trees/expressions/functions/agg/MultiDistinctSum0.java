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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecisionForSum;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.LargeIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** MultiDistinctSum0 */
public class MultiDistinctSum0 extends AggregateFunction implements UnaryExpression,
        ExplicitlyCastableSignature, ComputePrecisionForSum, MultiDistinction, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).varArgs(BigIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).varArgs(DoubleType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).varArgs(LargeIntType.INSTANCE)
    );

    public MultiDistinctSum0(Expression arg0) {
        super("multi_distinct_sum0", true, arg0);
    }

    public MultiDistinctSum0(boolean distinct, Expression arg0) {
        super("multi_distinct_sum0", true, arg0);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (child().getDataType().isDateLikeType()) {
            throw new AnalysisException("Sum0 in multi distinct functions do not support Date/Datetime type");
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return new Sum0(getArgument(0)).getSignatures();
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return new Sum0(getArgument(0)).searchSignature(signatures);
    }

    @Override
    public MultiDistinctSum0 withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MultiDistinctSum0(distinct, children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMultiDistinctSum0(this, context);
    }
}
