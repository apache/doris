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

namespace doris {

using doris_udf::BigIntVal;
using doris_udf::StringVal;

void HllFunctions::init() {
}

StringVal HllFunctions::hll_hash(FunctionContext* ctx, const StringVal& input) {
    const int HLL_SINGLE_VALUE_SIZE = 10;
    const int HLL_EMPTY_SIZE = 1;
    std::string buf;
    std::unique_ptr<HyperLogLog> hll;
    if (!input.is_null) {
        uint64_t hash_value = HashUtil::murmur_hash64A(input.ptr, input.len, HashUtil::MURMUR_SEED);
        hll.reset(new HyperLogLog(hash_value));
        buf.resize(HLL_SINGLE_VALUE_SIZE);
    } else {
        hll.reset(new HyperLogLog());
        buf.resize(HLL_EMPTY_SIZE);
    }
    hll->serialize((char*)buf.c_str());
    return AnyValUtil::from_string_temp(ctx, buf);
}

void HllFunctions::hll_init(FunctionContext *, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(HyperLogLog);
    dst->ptr = (uint8_t*)new HyperLogLog();
}

template <typename T>
void HllFunctions::hll_update(FunctionContext *, const T &src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    uint64_t hash_value = AnyValUtil::hash64_murmur(src, HashUtil::MURMUR_SEED);
    if (hash_value != 0) {
        auto* dst_hll = reinterpret_cast<HyperLogLog*>(dst->ptr);
        dst_hll->update(hash_value);
    }
}
void HllFunctions::hll_merge(FunctionContext*, const StringVal &src, StringVal* dst) {
    HyperLogLog src_hll = HyperLogLog((char*)src.ptr);
    auto* dst_hll = reinterpret_cast<HyperLogLog*>(dst->ptr);
    dst_hll->merge(src_hll);
}

BigIntVal HllFunctions::hll_finalize(FunctionContext*, const StringVal &src) {
    auto* src_hll = reinterpret_cast<HyperLogLog*>(src.ptr);
    BigIntVal result(src_hll->estimate_cardinality());
    delete src_hll;
    return result;
}

BigIntVal HllFunctions::hll_cardinality(FunctionContext* ctx, const StringVal& input) {
    if (input.is_null) {
        return BigIntVal::null();
    }
    StringVal dst;
    hll_init(ctx, &dst);
    hll_merge(ctx, input, &dst);
    return hll_finalize(ctx, dst);
}

StringVal HllFunctions::hll_serialize(FunctionContext *ctx, const StringVal &src) {
    auto* src_hll = reinterpret_cast<HyperLogLog*>(src.ptr);
    StringVal result(ctx, HLL_COLUMN_DEFAULT_LEN);
    int size = src_hll->serialize((char*)result.ptr);
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
template void HllFunctions::hll_update(FunctionContext*, const DecimalVal&, StringVal*);
template void HllFunctions::hll_update(FunctionContext*, const DecimalV2Val&, StringVal*);
}
