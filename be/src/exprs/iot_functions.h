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

#include "udf/udf.h"

namespace doris {

class IoTFunctions {
public:
    static void init();

    static void iot_first_update(FunctionContext*, const BigIntVal& ts, const DoubleVal& val, StringVal* dst);
    static void iot_first_merge(FunctionContext*,const StringVal& src, StringVal* dst);

    static void iot_last_update(FunctionContext*, const BigIntVal& ts, const DoubleVal& val, StringVal* dst);
    static void iot_last_merge(FunctionContext*,const StringVal& src, StringVal* dst);

    // serialize for iot_first() iot_last()
    static StringVal iot_first_last_serialize(FunctionContext* ctx, const StringVal& src);
    // finalize for iot_first() iot_last()
    static DoubleVal iot_first_last_finalize(FunctionContext*, const StringVal& src);

private:
    static void _init_iot_first_last_state(FunctionContext* context,
            const BigIntVal& ts, const DoubleVal& val,
            StringVal* dst);
};

}

