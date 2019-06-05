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

#include "exprs/hll_hash_function.h"

#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "runtime/datetime_value.h"
#include "util/path_builder.h"
#include "runtime/string_value.hpp"
#include "exprs/aggregate_functions.h"
#include "exprs/cast_functions.h"
#include "olap/olap_common.h"
#include "olap/utils.h"

namespace doris {

using doris_udf::BigIntVal;
using doris_udf::StringVal;

const int HllHashFunctions::HLL_INIT_EXPLICT_SET_SIZE = 10;
const int HllHashFunctions::HLL_EMPTY_SET_SIZE = 1;

void HllHashFunctions::init() {
}

StringVal HllHashFunctions::create_string_result(doris_udf::FunctionContext* ctx, 
                                                     const StringVal& val, const bool is_null) {
    StringVal result;
    if (is_null) {
        // HLL_DATA_EMPTY
        char buf[HLL_EMPTY_SET_SIZE];
        buf[0] = HLL_DATA_EMPTY;
        result = AnyValUtil::from_buffer_temp(ctx, buf, sizeof(buf));
    } else {
        // HLL_DATA_EXPLHLL_DATA_EXPLICIT
        uint64_t hash = HashUtil::murmur_hash64A(val.ptr, val.len, HashUtil::MURMUR_SEED);
        char buf[HLL_INIT_EXPLICT_SET_SIZE];
        buf[0] = HLL_DATA_EXPLICIT;
        buf[1] = 1;
        *((uint64_t*)(buf + 2)) = hash;
        result = AnyValUtil::from_buffer_temp(ctx, buf, sizeof(buf));
    }   
    return result;
}

StringVal HllHashFunctions::hll_hash(doris_udf::FunctionContext* ctx, 
                                     const StringVal& input) {
    return create_string_result(ctx, input, input.is_null);
}

StringVal HllHashFunctions::hll_cardinality(doris_udf::FunctionContext* ctx,
                                 const doris_udf::StringVal& dest_base) {
    BigIntVal intVal = hll_cardinality(ctx, static_cast<const HllVal&> (dest_base));
    return AnyValUtil::from_string_temp(ctx, std::to_string(intVal.val));
}
    
BigIntVal HllHashFunctions::hll_cardinality(doris_udf::FunctionContext* ctx,
                                            const HllVal& input) {
    if (input.is_null) {
        return BigIntVal::null();
    }
    HllVal dst;
    AggregateFunctions::hll_union_agg_init(ctx, &dst);
    AggregateFunctions::hll_union_agg_update(ctx, input, &dst);
    return AggregateFunctions::hll_union_agg_finalize(ctx, dst);
}
}
