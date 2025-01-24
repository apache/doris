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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class DictGet {
    public static List<FunctionSignature> creatSignatures(PrimitiveType returnType) {
        return ImmutableList.of(
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        DateTimeV2Type.SYSTEM_DEFAULT),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        DateV2Type.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        BooleanType.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        TinyIntType.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        SmallIntType.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        IntegerType.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        BigIntType.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        LargeIntType.INSTANCE),

                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        FloatType.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        DoubleType.INSTANCE),

                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        DecimalV3Type.WILDCARD),

                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        IPv4Type.INSTANCE),
                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        IPv6Type.INSTANCE),

                FunctionSignature.ret(returnType).args(StringType.INSTANCE, StringType.INSTANCE,
                        StringType.INSTANCE));
    }

}
