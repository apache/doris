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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.FunctionTrait;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** OrthogonalBitmapFunction */
public interface OrthogonalBitmapFunction extends FunctionTrait, ExplicitlyCastableSignature {

    List<DataType> SUPPORTED_TYPES = ImmutableList.of(
            SmallIntType.INSTANCE, TinyIntType.INSTANCE, IntegerType.INSTANCE, BigIntType.INSTANCE,
            FloatType.INSTANCE, DoubleType.INSTANCE, CharType.SYSTEM_DEFAULT, StringType.INSTANCE
    );

    List<FunctionSignature> SIGNATURES = SUPPORTED_TYPES.stream()
            .map(type -> FunctionSignature.ret(BigIntType.INSTANCE).varArgs(BitmapType.INSTANCE, type, type))
            .collect(ImmutableList.toImmutableList());

    @Override
    default List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
