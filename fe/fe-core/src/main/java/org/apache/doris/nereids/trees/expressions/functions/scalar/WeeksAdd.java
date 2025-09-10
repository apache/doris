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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignatureForDateArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.DateAddSubMonotonic;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullableOnDateOrTimeLikeV2Args;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'weeks_add'.
 */
public class WeeksAdd extends ScalarFunction implements BinaryExpression, ExplicitlyCastableSignature,
        ComputeSignatureForDateArithmetic, PropagateNullableOnDateOrTimeLikeV2Args, DateAddSubMonotonic {

    //ATTN: must place Datetime before Date, because for castring from string like literal, date and datetime has
    // the same precedence, but we prefer datetime to avoid data loss. for string literal which could cast to Date
    // without loss, we handle it by ComputeSignatureForDateArithmetic.
    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(DateTimeV2Type.SYSTEM_DEFAULT,
                    IntegerType.INSTANCE),
            FunctionSignature.ret(DateV2Type.INSTANCE).args(DateV2Type.INSTANCE, IntegerType.INSTANCE));

    public WeeksAdd(Expression arg0, Expression arg1) {
        super("weeks_add", arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private WeeksAdd(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public WeeksAdd withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new WeeksAdd(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWeeksAdd(this, context);
    }

    @Override
    public Expression withConstantArgs(Expression literal) {
        return new WeeksAdd(literal, child(1));
    }
}
