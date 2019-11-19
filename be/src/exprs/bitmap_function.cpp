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

#include "exprs/bitmap_function.h"

#include "exprs/anyval_util.h"
#include "util/bitmap.h"
#include "util/string_parser.hpp"

namespace doris {
void BitmapFunctions::init() {
}

void BitmapFunctions::bitmap_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(RoaringBitmap);
    dst->ptr = (uint8_t*)new RoaringBitmap();
}

StringVal BitmapFunctions::bitmap_empty(FunctionContext* ctx) {
    RoaringBitmap bitmap;
    std::string buf;
    buf.resize(bitmap.size());
    bitmap.serialize((char*)buf.c_str());
    return AnyValUtil::from_string_temp(ctx, buf);
}

template <typename T>
void BitmapFunctions::bitmap_update_int(FunctionContext* ctx, const T& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    auto* dst_bitmap = reinterpret_cast<RoaringBitmap*>(dst->ptr);
    dst_bitmap->update(src.val);
}

BigIntVal BitmapFunctions::bitmap_finalize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<RoaringBitmap*>(src.ptr);
    BigIntVal result(src_bitmap->cardinality());
    delete src_bitmap;
    return result;
}

void BitmapFunctions::bitmap_union(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    auto* dst_bitmap = reinterpret_cast<RoaringBitmap*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        dst_bitmap->merge(*reinterpret_cast<RoaringBitmap*>(src.ptr));
    } else {
        dst_bitmap->merge(RoaringBitmap((char*)src.ptr));
    }
}

BigIntVal BitmapFunctions::bitmap_count(FunctionContext* ctx, const StringVal& src) {
    // zero size means the src input is a agg object
    if (src.len == 0) {
        auto bitmap = reinterpret_cast<RoaringBitmap*>(src.ptr);
        return {bitmap->cardinality()};
    } else {
        RoaringBitmap bitmap ((char*)src.ptr);
        return {bitmap.cardinality()};
    }
}

StringVal BitmapFunctions::to_bitmap(doris_udf::FunctionContext* ctx, const doris_udf::StringVal& src) {
    std::unique_ptr<RoaringBitmap> bitmap {new RoaringBitmap()};
    if (!src.is_null) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        uint32_t int_value = StringParser::string_to_unsigned_int<uint32_t>(reinterpret_cast<char*>(src.ptr), src.len, &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            std::stringstream error_msg;
            error_msg << "The to_bitmap function argument: " << std::string(reinterpret_cast<char*>(src.ptr), src.len)
            << " type isn't integer family or exceed unsigned integer max value 4294967295";
            ctx->set_error(error_msg.str().c_str());
            return StringVal::null();
        }
        bitmap->update(int_value);
    }
    std::string buf;
    buf.resize(bitmap->size());
    bitmap->serialize((char*)buf.c_str());
    return AnyValUtil::from_string_temp(ctx, buf);
}

StringVal BitmapFunctions::bitmap_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<RoaringBitmap*>(src.ptr);
    StringVal result(ctx, src_bitmap->size());
    src_bitmap->serialize((char*)result.ptr);
    delete src_bitmap;
    return result;
}

template void BitmapFunctions::bitmap_update_int<TinyIntVal>(
        FunctionContext* ctx, const TinyIntVal& src, StringVal* dst);
template void BitmapFunctions::bitmap_update_int<SmallIntVal>(
        FunctionContext* ctx, const SmallIntVal& src, StringVal* dst);
template void BitmapFunctions::bitmap_update_int<IntVal>(
        FunctionContext* ctx, const IntVal& src, StringVal* dst);

}
