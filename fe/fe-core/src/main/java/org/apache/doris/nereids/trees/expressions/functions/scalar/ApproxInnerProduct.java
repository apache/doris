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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisIllegalParamException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ScalarFunction 'approx_inner_product'
 */
public class ApproxInnerProduct extends ApproxVectorDistanceFunc {
    private static final String NAME = "approx_inner_product";

    private static final List<FunctionSignature> APPROX_IP_SIGNATURES = Stream.concat(SIGNATURES.stream(),
            Stream.of(
                FunctionSignature.ret(DoubleType.INSTANCE).args(MapType.of(TinyIntType.INSTANCE, FloatType.INSTANCE),
                    MapType.of(TinyIntType.INSTANCE, FloatType.INSTANCE)),
                FunctionSignature.ret(DoubleType.INSTANCE).args(MapType.of(SmallIntType.INSTANCE, FloatType.INSTANCE),
                    MapType.of(SmallIntType.INSTANCE, FloatType.INSTANCE)),
                FunctionSignature.ret(DoubleType.INSTANCE).args(MapType.of(IntegerType.INSTANCE, FloatType.INSTANCE),
                    MapType.of(IntegerType.INSTANCE, FloatType.INSTANCE))
                )
            ).collect(Collectors.toList());

    public ApproxInnerProduct(Expression arg0, Expression arg1) {
        super(NAME, arg0, arg1);
    }

    public static String name() {
        return NAME;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return APPROX_IP_SIGNATURES;
    }

    /**
     * withChildren.
     */
    @Override
    public ApproxInnerProduct withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ApproxInnerProduct(children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitApproxInnerProduct(this, context);
    }

    @Override
    public void checkVectorRange(Expression expr) throws AnalysisIllegalParamException {
        // do nothing
    }
}
