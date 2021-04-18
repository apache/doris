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

#include "exprs/hash_functions.h"

#include "udf/udf.h"
#include "util/hash_util.hpp"

namespace doris {

using doris_udf::FunctionContext;
using doris_udf::IntVal;
using doris_udf::StringVal;

void HashFunctions::init() {}

IntVal HashFunctions::murmur_hash3_32(FunctionContext* ctx, int num_children,
                                      const StringVal* inputs) {
    uint32_t seed = HashUtil::MURMUR3_32_SEED;
    for (int i = 0; i < num_children; ++i) {
        if (inputs[i].is_null) {
            return IntVal::null();
        }
        seed = HashUtil::murmur_hash3_32(inputs[i].ptr, inputs[i].len, seed);
    }
    return seed;
}

} // namespace doris
