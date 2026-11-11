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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Convert a direct TTL input to epoch microseconds in the writing request's time zone. */
public class RowTtlExpiration extends ScalarFunction implements ExplicitlyCastableSignature, PropagateNullable {
    public static final String FUNCTION_NAME = "row_ttl_expiration";

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateTimeV2Type.WILDCARD),
            FunctionSignature.ret(BigIntType.INSTANCE).args(TimeStampTzType.WILDCARD),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateV2Type.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateTimeType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateType.INSTANCE));

    public RowTtlExpiration(Expression expiration) {
        super(FUNCTION_NAME, expiration);
    }

    private RowTtlExpiration(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /** Leave already encoded epochs alone; temporal inputs must be converted before the sink cast. */
    public static Expression convertInput(Column column, Expression expression) {
        if (column.isTtlColumn() && column.getType().isBigIntType() && expression.getDataType().isDateLikeType()) {
            RowTtlExpiration expiration = new RowTtlExpiration(expression);
            expiration.checkLegalityBeforeTypeCoercion();
            return expiration;
        }
        return expression;
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (getArgument(0).getDataType().isTimeStampNsType()) {
            throw new AnalysisException("row ttl expiration does not support TIMESTAMP_NS");
        }
    }

    @Override
    public RowTtlExpiration withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new RowTtlExpiration(getFunctionParams(children));
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
