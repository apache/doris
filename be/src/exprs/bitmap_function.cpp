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

namespace doris {
void BitmapFunctions::init() {
}

void BitmapFunctions::bitmap_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(RoaringBitmap);
    dst->ptr = (uint8_t*)new RoaringBitmap();
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
    RoaringBitmap src_bitmap = RoaringBitmap((char*)src.ptr);
    auto* dst_bitmap = reinterpret_cast<RoaringBitmap*>(dst->ptr);
    dst_bitmap->merge(src_bitmap);
}

BigIntVal BitmapFunctions::bitmap_count(FunctionContext* ctx, const StringVal& src) {
    RoaringBitmap bitmap ((char*)src.ptr);
    BigIntVal result(bitmap.cardinality());
    return result;
}

// we assume the input src is a valid integer string
StringVal BitmapFunctions::to_bitmap(doris_udf::FunctionContext* ctx, const doris_udf::StringVal& src) {
    std::unique_ptr<RoaringBitmap> bitmap {new RoaringBitmap()};
    if (!src.is_null) {
        std::string tmp_str = std::string(reinterpret_cast<char*>(src.ptr), src.len) ;
        bitmap->update(std::stoi(tmp_str));
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
