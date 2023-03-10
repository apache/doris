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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * WidthBucket.
 */
public class WidthBucket extends ScalarFunction implements ExplicitlyCastableSignature, PropagateNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(TinyIntType.INSTANCE,
                    TinyIntType.INSTANCE, TinyIntType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(SmallIntType.INSTANCE,
                    SmallIntType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(IntegerType.INSTANCE,
                    IntegerType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE,
                    BigIntType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(FloatType.INSTANCE,
                    FloatType.INSTANCE, FloatType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DoubleType.INSTANCE,
                    DoubleType.INSTANCE, DoubleType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DecimalV2Type.SYSTEM_DEFAULT,
                    DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DecimalV3Type.WILDCARD,
                    DecimalV3Type.WILDCARD, DecimalV3Type.WILDCARD, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateType.INSTANCE,
                    DateType.INSTANCE, DateType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateV2Type.INSTANCE,
                    DateV2Type.INSTANCE, DateV2Type.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateTimeType.INSTANCE,
                    DateTimeType.INSTANCE, DateTimeType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(DateTimeV2Type.SYSTEM_DEFAULT,
                    DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE)
            );

    /**
     * constructor with 4 argument.
     */
    public WidthBucket(Expression arg0, Expression arg1, Expression arg2, Expression arg3) {
        super("width_bucket", arg0, arg1, arg2, arg3);
    }

    /**
     * withChildren.
     */
    @Override
    public WidthBucket withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 4);
        return new WidthBucket(children.get(0), children.get(1), children.get(2), children.get(3));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWidthBucket(this, context);
    }
}
