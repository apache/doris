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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_HLL_FUNCTION_H
#define DORIS_BE_SRC_QUERY_EXPRS_HLL_FUNCTION_H

#include <string>
#include "udf/udf.h"

namespace doris {

class HllFunctions {
public:
    static void init();
    static StringVal hll_hash(FunctionContext* ctx, const StringVal& dest_base);
    static std::string hll_hash(const StringVal& dest_base);

    static StringVal hll_empty(FunctionContext* ctx);
    static void hll_init(FunctionContext*, StringVal* dst);

    template <typename T>
    static void hll_update(FunctionContext*, const T& src, StringVal* dst);

    static void hll_merge(FunctionContext*, const StringVal& src, StringVal* dst);

    static BigIntVal hll_finalize(FunctionContext*, const StringVal& src);

    // Get the hll cardinality, the difference from hll_finalize method is
    // hll_get_value method doesn't free memory,  this function is used in analytic get_value function
    static BigIntVal hll_get_value(FunctionContext*, const StringVal& src);

    static StringVal hll_serialize(FunctionContext* ctx, const StringVal& src);

    static BigIntVal hll_cardinality(FunctionContext* ctx, const StringVal& src);
};
} // namespace doris

#endif
