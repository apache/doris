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

#pragma once

#include "udf.h"

namespace doris_udf {

/// This is a function of the iot data processing algorithm.
    class IotFunctions {
    public:

        static void init_iot_functions(FunctionContext *context, StringVal *dst);

        static void
        iot_first_update(FunctionContext *context, const BigIntVal &ts, const DoubleVal &value, StringVal *dst);

        static void
        iot_last_update(FunctionContext *context, const BigIntVal &ts, const DoubleVal &value, StringVal *dst);

        static void iot_first_merge(FunctionContext *context, const StringVal &src, StringVal *dst);

        static void iot_last_merge(FunctionContext *context, const StringVal &src, StringVal *dst);

        static StringVal serialize(FunctionContext *ctx, const StringVal &src);

        static DoubleVal finalize(FunctionContext *context, const StringVal &val);
    };
}
