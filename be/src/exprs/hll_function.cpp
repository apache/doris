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

#include "exprs/hll_function.h"

#include "exprs/anyval_util.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace doris {

using doris_udf::BigIntVal;
using doris_udf::StringVal;

void HllFunctions::init() {}

StringVal HllFunctions::hll_hash(FunctionContext* ctx, const StringVal& input) {
    return AnyValUtil::from_string_temp(ctx, hll_hash(input));
}

std::string HllFunctions::hll_hash(const StringVal& input) {
    HyperLogLog hll;
    if (!input.is_null) {
        uint64_t hash_value = HashUtil::murmur_hash64A(input.ptr, input.len, HashUtil::MURMUR_SEED);
        hll.update(hash_value);
    }
    std::string buf;
    buf.resize(hll.max_serialized_size());
    buf.resize(hll.serialize((uint8_t*)buf.c_str()));

    return buf;
}

void HllFunctions::hll_init(FunctionContext*, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(HyperLogLog);
    dst->ptr = (uint8_t*)new HyperLogLog();
}

StringVal HllFunctions::hll_empty(FunctionContext* ctx) {
    return AnyValUtil::from_string_temp(ctx, HyperLogLog::empty());
}

template <typename T>
void HllFunctions::hll_update(FunctionContext*, const T& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    uint64_t hash_value = AnyValUtil::hash64_murmur(src, HashUtil::MURMUR_SEED);
    if (hash_value != 0) {
        auto* dst_hll = reinterpret_cast<HyperLogLog*>(dst->ptr);
        dst_hll->update(hash_value);
    }
}

void HllFunctions::hll_merge(FunctionContext*, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto* dst_hll = reinterpret_cast<HyperLogLog*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        dst_hll->merge(*reinterpret_cast<HyperLogLog*>(src.ptr));
    } else {
        dst_hll->merge(HyperLogLog(Slice(src.ptr, src.len)));
    }
}

BigIntVal HllFunctions::hll_finalize(FunctionContext*, const StringVal& src) {
    auto* src_hll = reinterpret_cast<HyperLogLog*>(src.ptr);
    BigIntVal result(src_hll->estimate_cardinality());
    delete src_hll;
    return result;
}

BigIntVal HllFunctions::hll_get_value(FunctionContext*, const StringVal& src) {
    if (src.is_null) {
        return BigIntVal::null();
    }
    auto* src_hll = reinterpret_cast<HyperLogLog*>(src.ptr);
    BigIntVal result(src_hll->estimate_cardinality());
    return result;
}

BigIntVal HllFunctions::hll_cardinality(FunctionContext* ctx, const StringVal& input) {
    if (input.is_null) {
        return BigIntVal();
    }
    StringVal dst;
    hll_init(ctx, &dst);
    hll_merge(ctx, input, &dst);
    return hll_finalize(ctx, dst);
}

StringVal HllFunctions::hll_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_hll = reinterpret_cast<HyperLogLog*>(src.ptr);
    StringVal result(ctx, src_hll->max_serialized_size());
    int size = src_hll->serialize((uint8_t*)result.ptr);
    result.resize(ctx, size);
    delete src_hll;
    return result;
}

template void HllFunctions::hll_update(FunctionContext*, const BooleanVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const TinyIntVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const SmallIntVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const IntVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const BigIntVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const FloatVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const DoubleVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const StringVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const DateTimeVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const LargeIntVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const DecimalV2Val&, StringVal*);
} // namespace doris
