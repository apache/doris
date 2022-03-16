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
class QuantileStateFunctions {
public:
    static void init();
    static void quantile_state_init(FunctionContext* ctx, StringVal* dst);
    static void to_quantile_state_prepare(FunctionContext* ctx,
                                          FunctionContext::FunctionStateScope scope);
    static void quantile_percent_prepare(FunctionContext* ctx,
                                         FunctionContext::FunctionStateScope scope);
    static StringVal to_quantile_state(FunctionContext* ctx, const StringVal& src);
    static void quantile_union(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    static DoubleVal quantile_percent(FunctionContext* ctx, StringVal& src);
    static StringVal quantile_state_serialize(FunctionContext* ctx, const StringVal& src);
};

} // namespace doris
