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

#include "exprs/utility_functions.h"

#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/monotime.h"

namespace doris {

void UtilityFunctions::init() {}

StringVal UtilityFunctions::version(FunctionContext* ctx) {
    return AnyValUtil::from_string_temp(ctx, "5.1.0");
}

BooleanVal UtilityFunctions::sleep(FunctionContext* ctx, const IntVal& seconds) {
    if (seconds.is_null) {
        return BooleanVal::null();
    }
    SleepFor(MonoDelta::FromSeconds(seconds.val));
    return BooleanVal(true);
}

} // namespace doris
