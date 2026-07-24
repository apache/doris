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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Internal post-merge policy. NULL never expires; expiration equal to query time is invisible. */
public class RowTtlIsVisible extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE).args(BigIntType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(BooleanType.INSTANCE).args(DateTimeV2Type.WILDCARD, BigIntType.INSTANCE),
            FunctionSignature.ret(BooleanType.INSTANCE).args(TimeStampTzType.WILDCARD, BigIntType.INSTANCE),
            FunctionSignature.ret(BooleanType.INSTANCE).args(DateV2Type.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(BooleanType.INSTANCE).args(DateTimeType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(BooleanType.INSTANCE).args(DateType.INSTANCE, BigIntType.INSTANCE));

    public RowTtlIsVisible(Expression ttlValue, Expression durationMicros) {
        super("row_ttl_is_visible", ttlValue, durationMicros);
    }

    private RowTtlIsVisible(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public RowTtlIsVisible withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new RowTtlIsVisible(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }
}
