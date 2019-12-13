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

namespace detail {

const int DATETIME_PACKED_TIME_BYTE_SIZE = 8;
const int DATETIME_TYPE_BYTE_SIZE = 4;

const int DECIMAL_BYTE_SIZE = 16;

// get_val start
template<typename ValType, typename T>
T get_val(const ValType& x) {
    DCHECK(!x.is_null);
    return x.val;
}

template<>
StringValue get_val(const StringVal& x) {
    DCHECK(!x.is_null);
    return StringValue::from_string_val(x);
}

template<>
DateTimeValue get_val(const DateTimeVal& x) {
    return DateTimeValue::from_datetime_val(x);
}

template<>
DecimalV2Value get_val(const DecimalV2Val& x) {
    return DecimalV2Value::from_decimal_val(x);
}
// get_val end

// serialize_size start
template<typename T>
int32_t serialize_size(const T& v) {
    return sizeof(T);
}

template<>
int32_t serialize_size(const DateTimeValue& v) {
    return DATETIME_PACKED_TIME_BYTE_SIZE + DATETIME_TYPE_BYTE_SIZE;
}

template<>
int32_t serialize_size(const DecimalV2Value& v) {
    return DECIMAL_BYTE_SIZE;
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
char* write_to(const DateTimeValue& v, char* dest) {
    DateTimeVal value;
    v.to_datetime_val(&value);
    *(int64_t*)dest = value.packed_time;
    dest += DATETIME_PACKED_TIME_BYTE_SIZE;
    *(int*)dest = value.type;
    dest += DATETIME_TYPE_BYTE_SIZE;
    return dest;
}

template<>
char* write_to(const DecimalV2Value& v, char* dest) {
    __int128 value = v.value();
    memcpy(dest, &value, DECIMAL_BYTE_SIZE);
    dest += DECIMAL_BYTE_SIZE;
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
void read_from(const char** src, DateTimeValue* result) {
    DateTimeVal value;
    value.is_null = false;
    value.packed_time = *(int64_t*)(*src);
    *src += DATETIME_PACKED_TIME_BYTE_SIZE;
    value.type = *(int*)(*src);
    *src += DATETIME_TYPE_BYTE_SIZE;
    *result = DateTimeValue::from_datetime_val(value);;
}

template<>
void read_from(const char** src, DecimalV2Value* result) {
    __int128 v = 0;
    memcpy(&v, *src, DECIMAL_BYTE_SIZE);
    *src += DECIMAL_BYTE_SIZE;
    *result = DecimalV2Value(v);
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

// Calculate the intersection of two or more bitmaps
// Usage: intersect_count(bitmap_column_to_count, filter_column, filter_values ...)
// Example: intersect_count(user_id, event, 'A', 'B', 'C'), meaning find the intersect count of user_id in all A/B/C 3 bitmaps
// Todo(kks) Use Array type instead of variable arguments
template<typename T>
struct BitmapIntersect {
public:
    BitmapIntersect() {}

    explicit BitmapIntersect(const char* src) {
        deserialize(src);
    }

    void add_key(const T key) {
        RoaringBitmap empty_bitmap;
        _bitmaps[key] = empty_bitmap;
    }

    void update(const T& key, const RoaringBitmap& bitmap) {
        if (_bitmaps.find(key) != _bitmaps.end()) {
            _bitmaps[key].merge(bitmap);
        }
    }

    void merge(const BitmapIntersect& other) {
        for (auto& kv: other._bitmaps) {
            if (_bitmaps.find(kv.first) != _bitmaps.end()) {
                _bitmaps[kv.first].merge(kv.second);
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

        RoaringBitmap result;
        auto it = _bitmaps.begin();
        result.merge(it->second);
        it++;
        for (;it != _bitmaps.end(); it++) {
            result.intersect(it->second);
        }

        return result.cardinality();
    }

    // the serialize size
    size_t size() {
        size_t size = 4;
        for (auto& kv: _bitmaps) {
            size +=  detail::serialize_size(kv.first);;
            size +=  kv.second.size();
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
            kv.second.serialize(writer);
            writer += kv.second.size();
        }
    }

    void deserialize(const char* src) {
        const char* reader = src;
        int32_t bitmaps_size = *(int32_t*)reader;
        reader += 4;
        for (int32_t i = 0; i < bitmaps_size; i++) {
            T key;
            detail::read_from(&reader, &key);
            RoaringBitmap bitmap(reader);
            reader += bitmap.size();
            _bitmaps[key] = bitmap;
        }
    }

private:
    std::map<T, RoaringBitmap> _bitmaps;
};

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

StringVal BitmapFunctions::bitmap_hash(doris_udf::FunctionContext* ctx, const doris_udf::StringVal& src) {
    RoaringBitmap bitmap;
    if (!src.is_null) {
        uint32_t hash_value = HashUtil::murmur_hash3_32(src.ptr, src.len, HashUtil::MURMUR3_32_SEED);
        bitmap.update(hash_value);
    }
    std::string buf;
    buf.resize(bitmap.size());
    bitmap.serialize((char*)buf.c_str());
    return AnyValUtil::from_string_temp(ctx, buf);
}

StringVal BitmapFunctions::bitmap_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<RoaringBitmap*>(src.ptr);
    StringVal result(ctx, src_bitmap->size());
    src_bitmap->serialize((char*)result.ptr);
    delete src_bitmap;
    return result;
}

template<typename T, typename ValType>
void BitmapFunctions::bitmap_intersect_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(BitmapIntersect<T>);
    auto intersect = new BitmapIntersect<T>();

    // constant args start from index 2
    for (int i = 2; i < ctx->get_num_constant_args(); ++i) {
        DCHECK(ctx->is_arg_constant(i));
        ValType* arg = reinterpret_cast<ValType*>(ctx->get_constant_arg(i));
        intersect->add_key(detail::get_val<ValType, T>(*arg));
    }

    dst->ptr = (uint8_t*)intersect;
}

template<typename T, typename ValType>
void BitmapFunctions::bitmap_intersect_update(FunctionContext* ctx, const StringVal& src, const ValType& key,
                                              int num_key, const ValType* keys, const StringVal* dst) {
    auto* dst_bitmap = reinterpret_cast<BitmapIntersect<T>*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        dst_bitmap->update(detail::get_val<ValType, T>(key), *reinterpret_cast<RoaringBitmap*>(src.ptr));
    } else {
        dst_bitmap->update(detail::get_val<ValType, T>(key), RoaringBitmap((char*)src.ptr));
    }
}

template<typename T>
void BitmapFunctions::bitmap_intersect_merge(FunctionContext* ctx, const StringVal& src, const StringVal* dst) {
    auto* dst_bitmap = reinterpret_cast<BitmapIntersect<T>*>(dst->ptr);
    dst_bitmap->merge(BitmapIntersect<T>((char*)src.ptr));
}

template<typename T>
StringVal BitmapFunctions::bitmap_intersect_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    StringVal result(ctx, src_bitmap->size());
    src_bitmap->serialize((char*)result.ptr);
    delete src_bitmap;
    return result;
}

template<typename T>
BigIntVal BitmapFunctions::bitmap_intersect_finalize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    BigIntVal result = BigIntVal(src_bitmap->intersect_count());
    delete src_bitmap;
    return result;
}


template void BitmapFunctions::bitmap_update_int<TinyIntVal>(
        FunctionContext* ctx, const TinyIntVal& src, StringVal* dst);
template void BitmapFunctions::bitmap_update_int<SmallIntVal>(
        FunctionContext* ctx, const SmallIntVal& src, StringVal* dst);
template void BitmapFunctions::bitmap_update_int<IntVal>(
        FunctionContext* ctx, const IntVal& src, StringVal* dst);


template void BitmapFunctions::bitmap_intersect_init<int8_t, TinyIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<int16_t, SmallIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<int32_t, IntVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<int64_t, BigIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<__int128, LargeIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<float, FloatVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<double, DoubleVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<DateTimeValue, DateTimeVal>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<DecimalV2Value, DecimalV2Val>(
    FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<StringValue, StringVal>(
    FunctionContext* ctx, StringVal* dst);


template void BitmapFunctions::bitmap_intersect_update<int8_t, TinyIntVal>(
    FunctionContext* ctx, const StringVal& src, const TinyIntVal& key,
    int num_key, const TinyIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<int16_t, SmallIntVal>(
    FunctionContext* ctx, const StringVal& src, const SmallIntVal& key,
    int num_key, const SmallIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<int32_t, IntVal>(
    FunctionContext* ctx, const StringVal& src, const IntVal& key,
    int num_key, const IntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<int64_t, BigIntVal>(
    FunctionContext* ctx, const StringVal& src, const BigIntVal& key,
    int num_key, const BigIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<__int128, LargeIntVal>(
    FunctionContext* ctx, const StringVal& src, const LargeIntVal& key,
    int num_key, const LargeIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<float, FloatVal>(
    FunctionContext* ctx, const StringVal& src, const FloatVal& key,
    int num_key, const FloatVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<double, DoubleVal>(
    FunctionContext* ctx, const StringVal& src, const DoubleVal& key,
    int num_key, const DoubleVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<DateTimeValue, DateTimeVal>(
    FunctionContext* ctx, const StringVal& src, const DateTimeVal& key,
    int num_key, const DateTimeVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<DecimalV2Value, DecimalV2Val>(
    FunctionContext* ctx, const StringVal& src, const DecimalV2Val& key,
    int num_key, const DecimalV2Val* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<StringValue, StringVal>(
    FunctionContext* ctx, const StringVal& src, const StringVal& key,
    int num_key, const StringVal* keys, const StringVal* dst);


template void BitmapFunctions::bitmap_intersect_merge<int8_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<int16_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<int32_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<int64_t>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<__int128>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<float>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<double>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<DateTimeValue>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<DecimalV2Value>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<StringValue>(
    FunctionContext* ctx, const StringVal& src, const StringVal* dst);

template StringVal BitmapFunctions::bitmap_intersect_serialize<int8_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<int16_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<int32_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<int64_t>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<__int128>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<float>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<double>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<DateTimeValue>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<DecimalV2Value>(
    FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<StringValue>(
    FunctionContext* ctx, const StringVal& src);

template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int8_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int16_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int32_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int64_t>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<__int128>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<float>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<double>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<DateTimeValue>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<DecimalV2Value>(
    FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<StringValue>(
    FunctionContext* ctx, const StringVal& src);

}
