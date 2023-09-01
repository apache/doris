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
import org.apache.doris.nereids.trees.expressions.functions.IdenticalSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * change nullable input col to non_nullable col
 */
public class NonNullable extends ScalarFunction implements UnaryExpression, IdenticalSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE).args(BooleanType.INSTANCE),
            FunctionSignature.ret(TinyIntType.INSTANCE).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(SmallIntType.INSTANCE).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).args(IntegerType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(LargeIntType.INSTANCE).args(LargeIntType.INSTANCE),
            FunctionSignature.ret(FloatType.INSTANCE).args(FloatType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE),
            FunctionSignature.ret(DateType.INSTANCE).args(DateType.INSTANCE),
            FunctionSignature.ret(DateV2Type.INSTANCE).args(DateV2Type.INSTANCE),
            FunctionSignature.ret(DateTimeType.INSTANCE).args(DateTimeType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(DateTimeV2Type.SYSTEM_DEFAULT),
            FunctionSignature.ret(DateType.INSTANCE).args(DateType.INSTANCE),
            FunctionSignature.ret(DecimalV2Type.INSTANCE).args(DecimalV2Type.INSTANCE),
            FunctionSignature.ret(DecimalV3Type.INSTANCE).args(DecimalV3Type.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE),
            FunctionSignature.ret(BitmapType.INSTANCE).args(BitmapType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(JsonType.INSTANCE),
            FunctionSignature.ret(ArrayType.SYSTEM_DEFAULT).args(ArrayType.SYSTEM_DEFAULT)
            );

    public NonNullable(Expression expr) {
        super("non_nullable", expr);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

}
