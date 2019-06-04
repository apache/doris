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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_HLL_HASH_FUNCTION_H
#define DORIS_BE_SRC_QUERY_EXPRS_HLL_HASH_FUNCTION_H

#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "util/hash_util.hpp"
#include "exprs/anyval_util.h"
#include "runtime/datetime_value.h"

namespace doris {

class Expr;
class OpcodeRegistry;
class TupleRow;

class HllHashFunctions {
public:
    static void init();
    static StringVal hll_hash(doris_udf::FunctionContext* ctx, 
                              const doris_udf::StringVal& dest_base);
    static BigIntVal hll_cardinality(doris_udf::FunctionContext* ctx,
                                     const doris_udf::HllVal& dest_base);
    //for backward compatibility, we could remove this method after doris 0.11 version
    static StringVal hll_cardinality(doris_udf::FunctionContext* ctx,
                                     const doris_udf::StringVal& dest_base);
    static StringVal create_string_result(doris_udf::FunctionContext* ctx, 
                                          const StringVal& str, const bool is_null);

    static const int HLL_INIT_EXPLICT_SET_SIZE;
    static const int HLL_EMPTY_SET_SIZE;
};
}

#endif
