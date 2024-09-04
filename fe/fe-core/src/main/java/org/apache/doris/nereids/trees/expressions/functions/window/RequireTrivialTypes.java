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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

/**
 * types used for specific window functions.
 * This list is equal with Type.trivialTypes used in legacy planner
 */
public interface RequireTrivialTypes {

    // todo: add JsonBType
    ImmutableList<DataType> trivialTypes = ImmutableList.of(
            BooleanType.INSTANCE,
            TinyIntType.INSTANCE,
            SmallIntType.INSTANCE,
            IntegerType.INSTANCE,
            BigIntType.INSTANCE,
            LargeIntType.INSTANCE,
            FloatType.INSTANCE,
            DoubleType.INSTANCE,
            DecimalV2Type.SYSTEM_DEFAULT,
            DecimalV3Type.WILDCARD,
            DateType.INSTANCE,
            DateTimeType.INSTANCE,
            DateV2Type.INSTANCE,
            DateTimeV2Type.SYSTEM_DEFAULT,
            TimeType.INSTANCE,
            TimeV2Type.INSTANCE,
            VarcharType.SYSTEM_DEFAULT,
            StringType.INSTANCE
    );
}
