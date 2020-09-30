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

#include "orthogonal_bitmap_function.h"
#include "bitmap_value.h"
#include "string_value.h"
#include <iostream>

namespace doris_udf {

namespace detail {

const int DATETIME_PACKED_TIME_BYTE_SIZE = 8;
const int DATETIME_TYPE_BYTE_SIZE = 4;

const int DECIMAL_BYTE_SIZE = 16;

// get_val start
template<typename ValType, typename T>
T get_val(const ValType& x) {
    return x.val;
}

template<>
StringValue get_val(const StringVal& x) {
    return StringValue::from_string_val(x);
}
// get_val end

// serialize_size start
template<typename T>
int32_t serialize_size(const T& v) {
    return sizeof(T);
}

template<>
int32_t serialize_size(const StringValue& v) {
    return v.len + 4;
}
// serialize_size end

// write_to start
template<typename T>
char* write_to(const T& v, char* dest) {
    size_t type_size = sizeof(T);
    memcpy(dest, &v, type_size);
    dest += type_size;
    return dest;
}

template<>
char* write_to(const StringValue& v, char* dest) {
    *(int32_t*)dest = v.len;
    dest += 4;
    memcpy(dest, v.ptr, v.len);
    dest += v.len;
    return dest;
}
// write_to end

// read_from start
template<typename T>
void read_from(const char** src, T* result) {
    size_t type_size = sizeof(T);
    memcpy(result, *src, type_size);
    *src += type_size;
}

template<>
void read_from(const char** src, StringValue* result) {
    int32_t length = *(int32_t*)(*src);
    *src += 4;
    *result = StringValue((char *)*src, length);
    *src += length;
}
// read_from end

} // namespace detail

static StringVal serialize(FunctionContext* ctx, BitmapValue* value) {
    StringVal result(ctx, value->getSizeInBytes());
    value->write((char*) result.ptr);
    return result;
}

// Calculate the intersection of two or more bitmaps
template<typename T>
struct BitmapIntersect {
public:
    BitmapIntersect() {}

    explicit BitmapIntersect(const char* src) {
        deserialize(src);
    }

    void add_key(const T key) {
        BitmapValue empty_bitmap;
        _bitmaps[key] = empty_bitmap;
    }

    void update(const T& key, const BitmapValue& bitmap) {
        if (_bitmaps.find(key) != _bitmaps.end()) {
            _bitmaps[key] |= bitmap;
        }
    }

    void merge(const BitmapIntersect& other) {
        for (auto& kv: other._bitmaps) {
            if (_bitmaps.find(kv.first) != _bitmaps.end()) {
                _bitmaps[kv.first] |= kv.second;
            } else {
                _bitmaps[kv.first] = kv.second;
            }
        }
    }

    // calculate the intersection for _bitmaps's bitmap values
    int64_t intersect_count() const {
        if (_bitmaps.empty()) {
            return 0;
        }

        BitmapValue result;
        auto it = _bitmaps.begin();
        result |= it->second;
        it++;
        for (;it != _bitmaps.end(); it++) {
            result &= it->second;
        }

        return result.cardinality();
    }

    // intersection
    BitmapValue intersect() {
        BitmapValue result;
        auto it = _bitmaps.begin();
        result |= it->second;
        it++;
        for (;it != _bitmaps.end(); it++) {
            result &= it->second;
        }
        return result;
    }

    // the serialize size
    size_t size() {
        size_t size = 4;
        for (auto& kv: _bitmaps) {
            size +=  detail::serialize_size(kv.first);;
            size +=  kv.second.getSizeInBytes();
        }
        return size;
    }

    //must call size() first
    void serialize(char* dest) {
        char* writer = dest;
        *(int32_t*)writer = _bitmaps.size();
        writer += 4;
        for (auto& kv: _bitmaps) {
            writer = detail::write_to(kv.first, writer);
            kv.second.write(writer);
            writer += kv.second.getSizeInBytes();
        }
    }

    void deserialize(const char* src) {
        const char* reader = src;
        int32_t bitmaps_size = *(int32_t*)reader;
        reader += 4;
        for (int32_t i = 0; i < bitmaps_size; i++) {
            T key;
            detail::read_from(&reader, &key);
            BitmapValue bitmap(reader);
            reader += bitmap.getSizeInBytes();
            _bitmaps[key] = bitmap;
        }
    }

private:
    std::map<T, BitmapValue> _bitmaps;
};

void OrthogonalBitmapFunctions::init() {
}

void OrthogonalBitmapFunctions::bitmap_union_count_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(BitmapValue);
    dst->ptr = (uint8_t*)new BitmapValue();
}

void OrthogonalBitmapFunctions::bitmap_union(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto dst_bitmap = reinterpret_cast<BitmapValue*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        (*dst_bitmap) |= *reinterpret_cast<BitmapValue*>(src.ptr);
    } else {
        (*dst_bitmap) |= BitmapValue((char*) src.ptr);
    }
}

StringVal OrthogonalBitmapFunctions::bitmap_serialize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return src;
    }

    auto src_bitmap = reinterpret_cast<BitmapValue*>(src.ptr);
    StringVal result = serialize(ctx, src_bitmap);
    delete src_bitmap;
    return result;
}

StringVal OrthogonalBitmapFunctions::bitmap_count_serialize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return src;
    }

    auto src_bitmap = reinterpret_cast<BitmapValue*>(src.ptr);
    int64_t val = src_bitmap->cardinality();
    StringVal result(ctx, sizeof(int64_t));

    *(int64_t*)result.ptr = val;
    delete src_bitmap;
    return result;

}

// This is a init function for bitmap_intersect.
template<typename T, typename ValType>
void OrthogonalBitmapFunctions::bitmap_intersect_init(FunctionContext* ctx, StringVal* dst) {
    // constant args start from index 2
    if (ctx->get_num_constant_args() > 1) {
        dst->is_null = false;
        dst->len = sizeof(BitmapIntersect<T>);
        auto intersect = new BitmapIntersect<T>();

        for (int i = 2; i < ctx->get_num_constant_args(); ++i) {
            ValType* arg = reinterpret_cast<ValType*>(ctx->get_constant_arg(i));
            intersect->add_key(detail::get_val<ValType, T>(*arg));
        }

        dst->ptr = (uint8_t*)intersect;
    } else {
        dst->is_null = false;
        dst->len = sizeof(BitmapValue);
        dst->ptr = (uint8_t*)new BitmapValue();
    }
}

// This is a init function for intersect_count.
template<typename T, typename ValType>
void OrthogonalBitmapFunctions::bitmap_intersect_count_init(FunctionContext* ctx, StringVal* dst) {
    if (ctx->get_num_constant_args() > 1) {
        dst->is_null = false;
        dst->len = sizeof(BitmapIntersect<T>);
        auto intersect = new BitmapIntersect<T>();

        // constant args start from index 2
        for (int i = 2; i < ctx->get_num_constant_args(); ++i) {
            ValType* arg = reinterpret_cast<ValType*>(ctx->get_constant_arg(i));
            intersect->add_key(detail::get_val<ValType, T>(*arg));
        }

        dst->ptr = (uint8_t*)intersect;
    } else {
        dst->is_null = false;
        dst->len = sizeof(int64_t);
        dst->ptr = (uint8_t*)new int64_t;
        *(int64_t *)dst->ptr = 0;
    }
}

template<typename T, typename ValType>
void OrthogonalBitmapFunctions::bitmap_intersect_update(FunctionContext* ctx, const StringVal& src, const ValType& key,
                                              int num_key, const ValType* keys, const StringVal* dst) {
    auto* dst_bitmap = reinterpret_cast<BitmapIntersect<T>*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        dst_bitmap->update(detail::get_val<ValType, T>(key), *reinterpret_cast<BitmapValue*>(src.ptr));
    } else {
        dst_bitmap->update(detail::get_val<ValType, T>(key), BitmapValue((char*)src.ptr));
    }
}

template<typename T>
void OrthogonalBitmapFunctions::bitmap_intersect_merge(FunctionContext* ctx, const StringVal& src, const StringVal* dst) {
    auto* dst_bitmap = reinterpret_cast<BitmapIntersect<T>*>(dst->ptr);
    dst_bitmap->merge(BitmapIntersect<T>((char*)src.ptr));
}

template<typename T>
StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    StringVal result(ctx, src_bitmap->size());
    src_bitmap->serialize((char*)result.ptr);
    delete src_bitmap;
    return result;
}

template<typename T>
BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    BigIntVal result = BigIntVal(src_bitmap->intersect_count());
    delete src_bitmap;
    return result;
}

void OrthogonalBitmapFunctions::bitmap_count_merge(FunctionContext* context, const StringVal& src, StringVal* dst) {
    if (dst->len != sizeof(int64_t)) {
        auto dst_bitmap = reinterpret_cast<BitmapValue*>(dst->ptr);
        delete dst_bitmap;
        dst->is_null = false;
        dst->len = sizeof(int64_t);
        dst->ptr = (uint8_t*)new int64_t;
        *(int64_t *)dst->ptr = 0;
    }
    *(int64_t *)dst->ptr += *(int64_t *)src.ptr;
}

BigIntVal OrthogonalBitmapFunctions::bitmap_count_finalize(FunctionContext* context, const StringVal& src) {
    auto *pval = reinterpret_cast<int64_t *>(src.ptr);
    int64_t result = *pval;
    delete pval;
    return result;
}

template<typename T>
StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    int64_t val = src_bitmap->intersect_count();
    StringVal result(ctx, sizeof(int64_t));
    *(int64_t*)result.ptr = val;
    delete src_bitmap;
    return result;
}

template<typename T>
StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    BitmapValue bitmap_val = src_bitmap->intersect();
    StringVal result = serialize(ctx, &bitmap_val);
    delete src_bitmap;
    return result;
}


template void OrthogonalBitmapFunctions::bitmap_intersect_count_init<int8_t, TinyIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_count_init<int16_t, SmallIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_count_init<int32_t, IntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_count_init<int64_t, BigIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_count_init<float, FloatVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_count_init<double, DoubleVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_count_init<StringValue, StringVal>(
    FunctionContext* ctx, StringVal* dst);

template void OrthogonalBitmapFunctions::bitmap_intersect_init<int8_t, TinyIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_init<int16_t, SmallIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_init<int32_t, IntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_init<int64_t, BigIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_init<float, FloatVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_init<double, DoubleVal>(
    FunctionContext* ctx, StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_init<StringValue, StringVal>(
    FunctionContext* ctx, StringVal* dst);

template void OrthogonalBitmapFunctions::bitmap_intersect_update<int8_t, TinyIntVal>(
    FunctionContext* ctx, const StringVal& src, const TinyIntVal& key,
    int num_key, const TinyIntVal* keys, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_update<int16_t, SmallIntVal>(
    FunctionContext* ctx, const StringVal& src, const SmallIntVal& key,
    int num_key, const SmallIntVal* keys, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_update<int32_t, IntVal>(
    FunctionContext* ctx, const StringVal& src, const IntVal& key,
    int num_key, const IntVal* keys, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_update<int64_t, BigIntVal>(
    FunctionContext* ctx, const StringVal& src, const BigIntVal& key,
    int num_key, const BigIntVal* keys, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_update<float, FloatVal>(
    FunctionContext* ctx, const StringVal& src, const FloatVal& key,
    int num_key, const FloatVal* keys, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_update<double, DoubleVal>(
    FunctionContext* ctx, const StringVal& src, const DoubleVal& key,
    int num_key, const DoubleVal* keys, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_update<StringValue, StringVal>(
    FunctionContext* ctx, const StringVal& src, const StringVal& key,
    int num_key, const StringVal* keys, const StringVal* dst);

template void OrthogonalBitmapFunctions::bitmap_intersect_merge<int8_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_merge<int16_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_merge<int32_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_merge<int64_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_merge<float>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_merge<double>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void OrthogonalBitmapFunctions::bitmap_intersect_merge<StringValue>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);

template StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize<int8_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize<int16_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize<int32_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize<int64_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize<float>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize<double>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_serialize<StringValue>(
    FunctionContext* ctx, const StringVal& src);

template BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize<int8_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize<int16_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize<int32_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize<int64_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize<float>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize<double>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal OrthogonalBitmapFunctions::bitmap_intersect_finalize<StringValue>(
    FunctionContext* ctx, const StringVal& src);

template StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize<int8_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize<int16_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize<int32_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize<int64_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize<float>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize<double>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_count_serialize<StringValue>(
    FunctionContext* ctx, const StringVal& src);

template StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize<int8_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize<int16_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize<int32_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize<int64_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize<float>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize<double>(
    FunctionContext* ctx, const StringVal& src);
template StringVal OrthogonalBitmapFunctions::bitmap_intersect_and_serialize<StringValue>(
    FunctionContext* ctx, const StringVal& src);
}
