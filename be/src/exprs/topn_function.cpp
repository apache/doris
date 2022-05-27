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

#include "exprs/topn_function.h"

#include "util/slice.h"
#include "util/topn_counter.h"

namespace doris {

using doris_udf::AnyVal;

void TopNFunctions::init() {}

void TopNFunctions::topn_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(TopNCounter);
    const AnyVal* space_expand_rate_val = ctx->get_constant_arg(2);
    if (space_expand_rate_val != nullptr) {
        int32_t space_expand_rate = reinterpret_cast<const IntVal*>(space_expand_rate_val)->val;
        dst->ptr = (uint8_t*)new TopNCounter(space_expand_rate);
        return;
    }
    dst->ptr = (uint8_t*)new TopNCounter();
}

template <typename T>
void TopNFunctions::topn_update(FunctionContext*, const T& src, const IntVal& topn,
                                StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto* dst_topn = reinterpret_cast<TopNCounter*>(dst->ptr);
    dst_topn->set_top_num(topn.val);
    dst_topn->add_item(src);
}

template <typename T>
void TopNFunctions::topn_update(FunctionContext*, const T& src, const IntVal& topn,
                                const IntVal& space_expand_rate, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto* dst_topn = reinterpret_cast<TopNCounter*>(dst->ptr);
    dst_topn->set_top_num(topn.val);
    dst_topn->add_item(src);
}

void TopNFunctions::topn_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto* dst_topn = reinterpret_cast<TopNCounter*>(dst->ptr);
    dst_topn->merge(TopNCounter(Slice(src.ptr, src.len)));
}

StringVal TopNFunctions::topn_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_topn = reinterpret_cast<TopNCounter*>(src.ptr);

    std::string buffer;
    src_topn->serialize(&buffer);
    StringVal result(ctx, buffer.size());
    memcpy(result.ptr, buffer.data(), buffer.size());
    delete src_topn;
    return result;
}

StringVal TopNFunctions::topn_finalize(FunctionContext* ctx, const StringVal& src) {
    auto* src_topn = reinterpret_cast<TopNCounter*>(src.ptr);
    std::string result_str;
    src_topn->finalize(result_str);

    StringVal result(ctx, result_str.size());
    memcpy(result.ptr, result_str.data(), result_str.size());

    delete src_topn;
    return result;
}

template void TopNFunctions::topn_update(FunctionContext*, const StringVal&, const IntVal&,
                                         StringVal*);

template void TopNFunctions::topn_update(FunctionContext*, const StringVal&, const IntVal&,
                                         const IntVal&, StringVal*);

} // namespace doris