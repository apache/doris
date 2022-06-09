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
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "util/bitmap_value.h"
#include "util/string_parser.hpp"

namespace doris {

namespace detail {

const int DATETIME_PACKED_TIME_BYTE_SIZE = 8;
const int DATETIME_TYPE_BYTE_SIZE = 4;

const int DECIMAL_BYTE_SIZE = 16;

// get_val start
template <typename ValType, typename T>
T get_val(const ValType& x) {
    DCHECK(!x.is_null);
    return x.val;
}

template <>
StringValue get_val(const StringVal& x) {
    DCHECK(!x.is_null);
    return StringValue::from_string_val(x);
}

template <>
DateTimeValue get_val(const DateTimeVal& x) {
    return DateTimeValue::from_datetime_val(x);
}

template <>
DecimalV2Value get_val(const DecimalV2Val& x) {
    return DecimalV2Value::from_decimal_val(x);
}
// get_val end

// serialize_size start
template <typename T>
int32_t serialize_size(const T& v) {
    return sizeof(T);
}

template <>
int32_t serialize_size(const DateTimeValue& v) {
    return DATETIME_PACKED_TIME_BYTE_SIZE + DATETIME_TYPE_BYTE_SIZE;
}

template <>
int32_t serialize_size(const DecimalV2Value& v) {
    return DECIMAL_BYTE_SIZE;
}

template <>
int32_t serialize_size(const StringValue& v) {
    return v.len + 4;
}
// serialize_size end

// write_to start
template <typename T>
char* write_to(const T& v, char* dest) {
    size_t type_size = sizeof(T);
    memcpy(dest, &v, type_size);
    dest += type_size;
    return dest;
}

template <>
char* write_to(const DateTimeValue& v, char* dest) {
    DateTimeVal value;
    v.to_datetime_val(&value);
    *(int64_t*)dest = value.packed_time;
    dest += DATETIME_PACKED_TIME_BYTE_SIZE;
    *(int*)dest = value.type;
    dest += DATETIME_TYPE_BYTE_SIZE;
    return dest;
}

template <>
char* write_to(const DecimalV2Value& v, char* dest) {
    __int128 value = v.value();
    memcpy(dest, &value, DECIMAL_BYTE_SIZE);
    dest += DECIMAL_BYTE_SIZE;
    return dest;
}

template <>
char* write_to(const StringValue& v, char* dest) {
    *(int32_t*)dest = v.len;
    dest += 4;
    memcpy(dest, v.ptr, v.len);
    dest += v.len;
    return dest;
}
// write_to end

// read_from start
template <typename T>
void read_from(const char** src, T* result) {
    size_t type_size = sizeof(T);
    memcpy(result, *src, type_size);
    *src += type_size;
}

template <>
void read_from(const char** src, DateTimeValue* result) {
    DateTimeVal value;
    value.is_null = false;
    value.packed_time = *(int64_t*)(*src);
    *src += DATETIME_PACKED_TIME_BYTE_SIZE;
    value.type = *(int*)(*src);
    *src += DATETIME_TYPE_BYTE_SIZE;
    *result = DateTimeValue::from_datetime_val(value);
    ;
}

template <>
void read_from(const char** src, DecimalV2Value* result) {
    __int128 v = 0;
    memcpy(&v, *src, DECIMAL_BYTE_SIZE);
    *src += DECIMAL_BYTE_SIZE;
    *result = DecimalV2Value(v);
}

template <>
void read_from(const char** src, StringValue* result) {
    int32_t length = *(int32_t*)(*src);
    *src += 4;
    *result = StringValue((char*)*src, length);
    *src += length;
}
// read_from end

} // namespace detail

static StringVal serialize(FunctionContext* ctx, BitmapValue* value) {
    if (!value) {
        BitmapValue empty_bitmap;
        StringVal result(ctx, empty_bitmap.getSizeInBytes());
        empty_bitmap.write((char*)result.ptr);
        return result;
    } else {
        StringVal result(ctx, value->getSizeInBytes());
        value->write((char*)result.ptr);
        return result;
    }
}

// Calculate the intersection of two or more bitmaps
// Usage: intersect_count(bitmap_column_to_count, filter_column, filter_values ...)
// Example: intersect_count(user_id, event, 'A', 'B', 'C'), meaning find the intersect count of user_id in all A/B/C 3 bitmaps
// Todo(kks) Use Array type instead of variable arguments
template <typename T>
struct BitmapIntersect {
public:
    BitmapIntersect() {}

    explicit BitmapIntersect(const char* src) { deserialize(src); }

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
        for (auto& kv : other._bitmaps) {
            if (_bitmaps.find(kv.first) != _bitmaps.end()) {
                _bitmaps[kv.first] |= kv.second;
            } else {
                _bitmaps[kv.first] = kv.second;
            }
        }
    }

    // intersection
    BitmapValue intersect() const {
        BitmapValue result;
        auto it = _bitmaps.begin();
        result |= it->second;
        it++;
        for (; it != _bitmaps.end(); it++) {
            result &= it->second;
        }
        return result;
    }

    // calculate the intersection for _bitmaps's bitmap values
    int64_t intersect_count() const {
        if (_bitmaps.empty()) {
            return 0;
        }
        return intersect().cardinality();
    }

    // the serialize size
    size_t size() {
        size_t size = 4;
        for (auto& kv : _bitmaps) {
            size += detail::serialize_size(kv.first);
            ;
            size += kv.second.getSizeInBytes();
        }
        return size;
    }

    //must call size() first
    void serialize(char* dest) {
        char* writer = dest;
        *(int32_t*)writer = _bitmaps.size();
        writer += 4;
        for (auto& kv : _bitmaps) {
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

void BitmapFunctions::init() {}

void BitmapFunctions::bitmap_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(BitmapValue);
    dst->ptr = (uint8_t*)new BitmapValue();
}

StringVal BitmapFunctions::bitmap_empty(FunctionContext* ctx) {
    BitmapValue bitmap;
    return serialize(ctx, &bitmap);
}

template <typename T>
void BitmapFunctions::bitmap_update_int(FunctionContext* ctx, const T& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto dst_bitmap = reinterpret_cast<BitmapValue*>(dst->ptr);
    dst_bitmap->add(src.val);
}

BigIntVal BitmapFunctions::bitmap_finalize(FunctionContext* ctx, const StringVal& src) {
    auto src_bitmap = reinterpret_cast<BitmapValue*>(src.ptr);
    BigIntVal result(src_bitmap->cardinality());
    delete src_bitmap;
    return result;
}

BigIntVal BitmapFunctions::bitmap_get_value(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return 0;
    }
    auto src_bitmap = reinterpret_cast<BitmapValue*>(src.ptr);
    BigIntVal result(src_bitmap->cardinality());
    return result;
}

void BitmapFunctions::bitmap_union(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    auto dst_bitmap = reinterpret_cast<BitmapValue*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        (*dst_bitmap) |= *reinterpret_cast<BitmapValue*>(src.ptr);
    } else {
        (*dst_bitmap) |= BitmapValue((char*)src.ptr);
    }
}

// the dst value could be null
void BitmapFunctions::nullable_bitmap_init(FunctionContext* ctx, StringVal* dst) {
    dst->ptr = nullptr;
    dst->len = 0;
}

void BitmapFunctions::bitmap_intersect(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    // if dst is null, the src input is the first value
    if (UNLIKELY(dst->ptr == nullptr)) {
        dst->is_null = false;
        dst->len = sizeof(BitmapValue);
        dst->ptr = (uint8_t*)new BitmapValue((char*)src.ptr);
        return;
    }
    auto dst_bitmap = reinterpret_cast<BitmapValue*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        (*dst_bitmap) &= *reinterpret_cast<BitmapValue*>(src.ptr);
    } else {
        (*dst_bitmap) &= BitmapValue((char*)src.ptr);
    }
}

BigIntVal BitmapFunctions::bitmap_count(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return 0;
    }
    // zero size means the src input is a agg object
    if (src.len == 0) {
        auto bitmap = reinterpret_cast<BitmapValue*>(src.ptr);
        return {static_cast<int64_t>(bitmap->cardinality())};
    } else {
        BitmapValue bitmap((char*)src.ptr);
        return {static_cast<int64_t>(bitmap.cardinality())};
    }
}

BigIntVal BitmapFunctions::bitmap_min(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return BigIntVal::null();
    }

    if (src.len == 0) {
        return reinterpret_cast<BitmapValue*>(src.ptr)->minimum();
    } else {
        auto bitmap = BitmapValue((char*)src.ptr);
        return bitmap.minimum();
    }
}

StringVal BitmapFunctions::to_bitmap(doris_udf::FunctionContext* ctx,
                                     const doris_udf::StringVal& src) {
    BitmapValue bitmap;

    if (!src.is_null) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        uint64_t int_value = StringParser::string_to_unsigned_int<uint64_t>(
                reinterpret_cast<char*>(src.ptr), src.len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            bitmap.add(int_value);
        }
    }

    return serialize(ctx, &bitmap);
}

StringVal BitmapFunctions::bitmap_hash(doris_udf::FunctionContext* ctx,
                                       const doris_udf::StringVal& src) {
    BitmapValue bitmap;
    if (!src.is_null) {
        uint32_t hash_value =
                HashUtil::murmur_hash3_32(src.ptr, src.len, HashUtil::MURMUR3_32_SEED);
        bitmap.add(hash_value);
    }
    return serialize(ctx, &bitmap);
}

StringVal BitmapFunctions::bitmap_serialize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        // bitmap functions should never return nullable value
        return serialize(ctx, nullptr);
    }

    auto src_bitmap = reinterpret_cast<BitmapValue*>(src.ptr);
    StringVal result = serialize(ctx, src_bitmap);
    delete src_bitmap;
    return result;
}

// This is a init function for intersect_count not for bitmap_intersect.
template <typename T, typename ValType>
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

template <typename T, typename ValType>
void BitmapFunctions::bitmap_intersect_update(FunctionContext* ctx, const StringVal& src,
                                              const ValType& key, int num_key, const ValType* keys,
                                              const StringVal* dst) {
    auto* dst_bitmap = reinterpret_cast<BitmapIntersect<T>*>(dst->ptr);
    // zero size means the src input is a agg object
    if (src.len == 0) {
        dst_bitmap->update(detail::get_val<ValType, T>(key),
                           *reinterpret_cast<BitmapValue*>(src.ptr));
    } else {
        dst_bitmap->update(detail::get_val<ValType, T>(key), BitmapValue((char*)src.ptr));
    }
}

template <typename T>
void BitmapFunctions::bitmap_intersect_merge(FunctionContext* ctx, const StringVal& src,
                                             const StringVal* dst) {
    auto* dst_bitmap = reinterpret_cast<BitmapIntersect<T>*>(dst->ptr);
    dst_bitmap->merge(BitmapIntersect<T>((char*)src.ptr));
}

template <typename T>
StringVal BitmapFunctions::bitmap_intersect_serialize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    StringVal result(ctx, src_bitmap->size());
    src_bitmap->serialize((char*)result.ptr);
    delete src_bitmap;
    return result;
}

template <typename T>
BigIntVal BitmapFunctions::bitmap_intersect_finalize(FunctionContext* ctx, const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    BigIntVal result = BigIntVal(src_bitmap->intersect_count());
    delete src_bitmap;
    return result;
}

StringVal BitmapFunctions::bitmap_or(FunctionContext* ctx, const StringVal& lhs,
                                     const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return StringVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(rhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)rhs.ptr);
    }
    return serialize(ctx, &bitmap);
}

StringVal BitmapFunctions::bitmap_or(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                     const StringVal* bitmap_strs) {
    DCHECK_GE(num_args, 1);
    if (lhs.is_null || bitmap_strs->is_null) {
        return StringVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }
    for (int i = 0; i < num_args; ++i) {
        if (bitmap_strs[i].is_null) {
            return StringVal::null();
        }
        if (bitmap_strs[i].len == 0) {
            bitmap |= *reinterpret_cast<BitmapValue*>(bitmap_strs[i].ptr);
        } else {
            bitmap |= BitmapValue((char*)bitmap_strs[i].ptr);
        }
    }
    return serialize(ctx, &bitmap);
}

StringVal BitmapFunctions::bitmap_and(FunctionContext* ctx, const StringVal& lhs,
                                      const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return StringVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        bitmap &= *reinterpret_cast<BitmapValue*>(rhs.ptr);
    } else {
        bitmap &= BitmapValue((char*)rhs.ptr);
    }
    return serialize(ctx, &bitmap);
}

StringVal BitmapFunctions::bitmap_and(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                      const StringVal* bitmap_strs) {
    DCHECK_GE(num_args, 1);
    if (lhs.is_null || bitmap_strs->is_null) {
        return StringVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }
    for (int i = 0; i < num_args; ++i) {
        if (bitmap_strs[i].is_null) {
            return StringVal::null();
        }
        if (bitmap_strs[i].len == 0) {
            bitmap &= *reinterpret_cast<BitmapValue*>(bitmap_strs[i].ptr);
        } else {
            bitmap &= BitmapValue((char*)bitmap_strs[i].ptr);
        }
    }
    return serialize(ctx, &bitmap);
}

BigIntVal BitmapFunctions::bitmap_and_count(FunctionContext* ctx, const StringVal& lhs,
                                            const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return BigIntVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        return bitmap.and_cardinality(*reinterpret_cast<BitmapValue*>(rhs.ptr));
    } else {
        return bitmap.and_cardinality(BitmapValue((char*)rhs.ptr));
    }
}

BigIntVal BitmapFunctions::bitmap_and_count(FunctionContext* ctx, const StringVal& lhs,
                                            int num_args, const StringVal* bitmap_strs) {
    DCHECK_GE(num_args, 1);
    if (lhs.is_null || bitmap_strs->is_null) {
        return BigIntVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    for (int i = 0; i < num_args; i++) {
        if (bitmap_strs[i].is_null) {
            return BigIntVal::null();
        }
        if (bitmap_strs[i].len == 0) {
            bitmap &= *reinterpret_cast<BitmapValue*>(bitmap_strs[i].ptr);
        } else {
            bitmap &= BitmapValue((char*)bitmap_strs[i].ptr);
        }
    }
    return {static_cast<int64_t>(bitmap.cardinality())};
}

BigIntVal BitmapFunctions::bitmap_or_count(FunctionContext* ctx, const StringVal& lhs,
                                           const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return BigIntVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        return bitmap.or_cardinality(*reinterpret_cast<BitmapValue*>(rhs.ptr));
    } else {
        return bitmap.or_cardinality(BitmapValue((char*)rhs.ptr));
    }
}

BigIntVal BitmapFunctions::bitmap_or_count(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                           const StringVal* bitmap_strs) {
    DCHECK_GE(num_args, 1);
    if (lhs.is_null || bitmap_strs->is_null) {
        return BigIntVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    for (int i = 0; i < num_args; i++) {
        if (bitmap_strs[i].is_null) {
            return BigIntVal::null();
        }
        if (bitmap_strs[i].len == 0) {
            bitmap |= *reinterpret_cast<BitmapValue*>(bitmap_strs[i].ptr);
        } else {
            bitmap |= BitmapValue((char*)bitmap_strs[i].ptr);
        }
    }
    return {static_cast<int64_t>(bitmap.cardinality())};
}

StringVal BitmapFunctions::bitmap_xor(FunctionContext* ctx, const StringVal& lhs,
                                      const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return StringVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        bitmap ^= *reinterpret_cast<BitmapValue*>(rhs.ptr);
    } else {
        bitmap ^= BitmapValue((char*)rhs.ptr);
    }
    return serialize(ctx, &bitmap);
}

StringVal BitmapFunctions::bitmap_xor(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                      const StringVal* bitmap_strs) {
    DCHECK_GE(num_args, 1);
    if (lhs.is_null || bitmap_strs->is_null) {
        return StringVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }
    for (int i = 0; i < num_args; ++i) {
        if (bitmap_strs[i].is_null) {
            return StringVal::null();
        }
        if (bitmap_strs[i].len == 0) {
            bitmap ^= *reinterpret_cast<BitmapValue*>(bitmap_strs[i].ptr);
        } else {
            bitmap ^= BitmapValue((char*)bitmap_strs[i].ptr);
        }
    }
    return serialize(ctx, &bitmap);
}

BigIntVal BitmapFunctions::bitmap_xor_count(FunctionContext* ctx, const StringVal& lhs,
                                            const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return BigIntVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        return bitmap.xor_cardinality(*reinterpret_cast<BitmapValue*>(rhs.ptr));
    } else {
        return bitmap.xor_cardinality(BitmapValue((char*)rhs.ptr));
    }
}

BigIntVal BitmapFunctions::bitmap_xor_count(FunctionContext* ctx, const StringVal& lhs,
                                            int num_args, const StringVal* bitmap_strs) {
    DCHECK_GE(num_args, 1);
    if (lhs.is_null || bitmap_strs->is_null) {
        return BigIntVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    for (int i = 0; i < num_args; i++) {
        if (bitmap_strs[i].is_null) {
            return BigIntVal::null();
        }
        if (bitmap_strs[i].len == 0) {
            bitmap ^= *reinterpret_cast<BitmapValue*>(bitmap_strs[i].ptr);
        } else {
            bitmap ^= BitmapValue((char*)bitmap_strs[i].ptr);
        }
    }
    return {static_cast<int64_t>(bitmap.cardinality())};
}

StringVal BitmapFunctions::bitmap_not(FunctionContext* ctx, const StringVal& lhs,
                                      const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return StringVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        bitmap -= *reinterpret_cast<BitmapValue*>(rhs.ptr);
    } else {
        bitmap -= BitmapValue((char*)rhs.ptr);
    }
    return serialize(ctx, &bitmap);
}

StringVal BitmapFunctions::bitmap_and_not(FunctionContext* ctx, const StringVal& lhs,
                                          const StringVal& rhs) {
    return bitmap_xor(ctx, lhs, bitmap_and(ctx, lhs, rhs));
}

BigIntVal BitmapFunctions::bitmap_and_not_count(FunctionContext* ctx, const StringVal& lhs,
                                                const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return BigIntVal::null();
    }
    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        return bitmap.andnot_cardinality(*reinterpret_cast<BitmapValue*>(rhs.ptr));
    } else {
        return bitmap.andnot_cardinality(BitmapValue((char*)rhs.ptr));
    }
}

StringVal BitmapFunctions::bitmap_to_string(FunctionContext* ctx, const StringVal& input) {
    if (input.is_null) {
        return StringVal::null();
    }
    std::string str;
    if (input.len == 0) {
        str = reinterpret_cast<BitmapValue*>(input.ptr)->to_string();
    } else {
        BitmapValue bitmap((char*)input.ptr);
        str = bitmap.to_string();
    }
    return AnyValUtil::from_string_temp(ctx, str);
}

StringVal BitmapFunctions::bitmap_from_string(FunctionContext* ctx, const StringVal& input) {
    if (input.is_null) {
        return StringVal::null();
    }

    std::vector<uint64_t> bits;
    // The constructor of `stringPiece` only support int type.
    if ((input.len > INT32_MAX) || !SplitStringAndParse({(const char*)input.ptr, (int)input.len},
                                                        ",", &safe_strtou64, &bits)) {
        return StringVal::null();
    }

    BitmapValue bitmap(bits);
    return serialize(ctx, &bitmap);
}

BooleanVal BitmapFunctions::bitmap_contains(FunctionContext* ctx, const StringVal& src,
                                            const BigIntVal& input) {
    if (src.is_null || input.is_null) {
        return BooleanVal::null();
    }

    if (src.len == 0) {
        auto bitmap = reinterpret_cast<BitmapValue*>(src.ptr);
        return {bitmap->contains(input.val)};
    }

    BitmapValue bitmap((char*)src.ptr);
    return {bitmap.contains(input.val)};
}

BooleanVal BitmapFunctions::bitmap_has_any(FunctionContext* ctx, const StringVal& lhs,
                                           const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return BooleanVal::null();
    }

    BitmapValue bitmap;
    if (lhs.len == 0) {
        bitmap |= *reinterpret_cast<BitmapValue*>(lhs.ptr);
    } else {
        bitmap |= BitmapValue((char*)lhs.ptr);
    }

    if (rhs.len == 0) {
        bitmap &= *reinterpret_cast<BitmapValue*>(rhs.ptr);
    } else {
        bitmap &= BitmapValue((char*)rhs.ptr);
    }

    return {bitmap.cardinality() != 0};
}

BooleanVal BitmapFunctions::bitmap_has_all(FunctionContext* ctx, const StringVal& lhs,
                                           const StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return BooleanVal::null();
    }

    if (lhs.len != 0 && rhs.len != 0) {
        BitmapValue bitmap = BitmapValue(reinterpret_cast<char*>(lhs.ptr));
        int64_t lhs_cardinality = bitmap.cardinality();
        bitmap |= BitmapValue(reinterpret_cast<char*>(rhs.ptr));
        return {bitmap.cardinality() == lhs_cardinality};
    } else if (rhs.len != 0) {
        return {false};
    } else {
        return {true};
    }
}

BigIntVal BitmapFunctions::bitmap_max(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return BigIntVal::null();
    }

    if (src.len == 0) {
        return reinterpret_cast<BitmapValue*>(src.ptr)->maximum();
    } else {
        auto bitmap = BitmapValue((char*)src.ptr);
        return bitmap.maximum();
    }
}

StringVal BitmapFunctions::bitmap_subset_in_range(FunctionContext* ctx, const StringVal& src,
                                                  const BigIntVal& range_start,
                                                  const BigIntVal& range_end) {
    if (src.is_null || range_start.is_null || range_end.is_null) {
        return StringVal::null();
    }
    if (range_start.val >= range_end.val || range_start.val < 0 || range_end.val < 0) {
        return StringVal::null();
    }
    BitmapValue ret_bitmap;
    if (src.len == 0) {
        ret_bitmap = *reinterpret_cast<BitmapValue*>(src.ptr);
    } else {
        BitmapValue bitmap = BitmapValue((char*)src.ptr);
        bitmap.sub_range(range_start.val, range_end.val, &ret_bitmap);
    }

    return serialize(ctx, &ret_bitmap);
}

StringVal BitmapFunctions::sub_bitmap(FunctionContext* ctx, const StringVal& src,
                                      const BigIntVal& offset, const BigIntVal& cardinality_limit) {
    if (src.is_null || offset.is_null || cardinality_limit.is_null || cardinality_limit.val <= 0) {
        return StringVal::null();
    }

    BitmapValue ret_bitmap;
    if (src.len == 0) {
        ret_bitmap = *reinterpret_cast<BitmapValue*>(src.ptr);
    } else {
        BitmapValue bitmap = BitmapValue((char*)src.ptr);
        if (bitmap.offset_limit(offset.val, cardinality_limit.val, &ret_bitmap) == 0) {
            return StringVal::null();
        }
    }

    return serialize(ctx, &ret_bitmap);
}

StringVal BitmapFunctions::bitmap_subset_limit(FunctionContext* ctx, const StringVal& src,
                                               const BigIntVal& range_start,
                                               const BigIntVal& cardinality_limit) {
    if (src.is_null || range_start.is_null || cardinality_limit.is_null) {
        return StringVal::null();
    }
    if (range_start.val < 0 || cardinality_limit.val < 0) {
        return StringVal::null();
    }
    BitmapValue ret_bitmap;
    if (src.len == 0) {
        ret_bitmap = *reinterpret_cast<BitmapValue*>(src.ptr);
    } else {
        BitmapValue bitmap = BitmapValue((char*)src.ptr);
        bitmap.sub_limit(range_start.val, cardinality_limit.val, &ret_bitmap);
    }

    return serialize(ctx, &ret_bitmap);
}

void BitmapFunctions::orthogonal_bitmap_union_count_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(BitmapValue);
    dst->ptr = (uint8_t*)new BitmapValue();
}

StringVal BitmapFunctions::orthogonal_bitmap_count_serialize(FunctionContext* ctx,
                                                             const StringVal& src) {
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
template <typename T, typename ValType>
void BitmapFunctions::orthogonal_bitmap_intersect_init(FunctionContext* ctx, StringVal* dst) {
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
template <typename T, typename ValType>
void BitmapFunctions::orthogonal_bitmap_intersect_count_init(FunctionContext* ctx, StringVal* dst) {
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
        *(int64_t*)dst->ptr = 0;
    }
}

template <typename T>
StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize(FunctionContext* ctx,
                                                                 const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    BitmapValue bitmap_val = src_bitmap->intersect();
    StringVal result = serialize(ctx, &bitmap_val);
    delete src_bitmap;
    return result;
}

template <typename T>
BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize(FunctionContext* ctx,
                                                                const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    BigIntVal result = BigIntVal(src_bitmap->intersect_count());
    delete src_bitmap;
    return result;
}

void BitmapFunctions::orthogonal_bitmap_count_merge(FunctionContext* context, const StringVal& src,
                                                    StringVal* dst) {
    if (dst->len != sizeof(int64_t)) {
        auto dst_bitmap = reinterpret_cast<BitmapValue*>(dst->ptr);
        delete dst_bitmap;
        dst->is_null = false;
        dst->len = sizeof(int64_t);
        dst->ptr = (uint8_t*)new int64_t;
        *(int64_t*)dst->ptr = 0;
    }
    *(int64_t*)dst->ptr += *(int64_t*)src.ptr;
}

BigIntVal BitmapFunctions::orthogonal_bitmap_count_finalize(FunctionContext* context,
                                                            const StringVal& src) {
    auto* pval = reinterpret_cast<int64_t*>(src.ptr);
    int64_t result = *pval;
    delete pval;
    return result;
}

template <typename T>
StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize(FunctionContext* ctx,
                                                                       const StringVal& src) {
    auto* src_bitmap = reinterpret_cast<BitmapIntersect<T>*>(src.ptr);
    int64_t val = src_bitmap->intersect_count();
    StringVal result(ctx, sizeof(int64_t));
    *(int64_t*)result.ptr = val;
    delete src_bitmap;
    return result;
}

template void BitmapFunctions::bitmap_update_int<TinyIntVal>(FunctionContext* ctx,
                                                             const TinyIntVal& src, StringVal* dst);
template void BitmapFunctions::bitmap_update_int<SmallIntVal>(FunctionContext* ctx,
                                                              const SmallIntVal& src,
                                                              StringVal* dst);
template void BitmapFunctions::bitmap_update_int<IntVal>(FunctionContext* ctx, const IntVal& src,
                                                         StringVal* dst);
template void BitmapFunctions::bitmap_update_int<BigIntVal>(FunctionContext* ctx,
                                                            const BigIntVal& src, StringVal* dst);

// this is init function for intersect_count not for bitmap_intersect
template void BitmapFunctions::bitmap_intersect_init<int8_t, TinyIntVal>(FunctionContext* ctx,
                                                                         StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<int16_t, SmallIntVal>(FunctionContext* ctx,
                                                                           StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<int32_t, IntVal>(FunctionContext* ctx,
                                                                      StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<int64_t, BigIntVal>(FunctionContext* ctx,
                                                                         StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<__int128, LargeIntVal>(FunctionContext* ctx,
                                                                            StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<float, FloatVal>(FunctionContext* ctx,
                                                                      StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<double, DoubleVal>(FunctionContext* ctx,
                                                                        StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<DateTimeValue, DateTimeVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<DecimalV2Value, DecimalV2Val>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::bitmap_intersect_init<StringValue, StringVal>(FunctionContext* ctx,
                                                                             StringVal* dst);

template void BitmapFunctions::bitmap_intersect_update<int8_t, TinyIntVal>(
        FunctionContext* ctx, const StringVal& src, const TinyIntVal& key, int num_key,
        const TinyIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<int16_t, SmallIntVal>(
        FunctionContext* ctx, const StringVal& src, const SmallIntVal& key, int num_key,
        const SmallIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<int32_t, IntVal>(
        FunctionContext* ctx, const StringVal& src, const IntVal& key, int num_key,
        const IntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<int64_t, BigIntVal>(
        FunctionContext* ctx, const StringVal& src, const BigIntVal& key, int num_key,
        const BigIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<__int128, LargeIntVal>(
        FunctionContext* ctx, const StringVal& src, const LargeIntVal& key, int num_key,
        const LargeIntVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<float, FloatVal>(
        FunctionContext* ctx, const StringVal& src, const FloatVal& key, int num_key,
        const FloatVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<double, DoubleVal>(
        FunctionContext* ctx, const StringVal& src, const DoubleVal& key, int num_key,
        const DoubleVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<DateTimeValue, DateTimeVal>(
        FunctionContext* ctx, const StringVal& src, const DateTimeVal& key, int num_key,
        const DateTimeVal* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<DecimalV2Value, DecimalV2Val>(
        FunctionContext* ctx, const StringVal& src, const DecimalV2Val& key, int num_key,
        const DecimalV2Val* keys, const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_update<StringValue, StringVal>(
        FunctionContext* ctx, const StringVal& src, const StringVal& key, int num_key,
        const StringVal* keys, const StringVal* dst);

template void BitmapFunctions::bitmap_intersect_merge<int8_t>(FunctionContext* ctx,
                                                              const StringVal& src,
                                                              const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<int16_t>(FunctionContext* ctx,
                                                               const StringVal& src,
                                                               const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<int32_t>(FunctionContext* ctx,
                                                               const StringVal& src,
                                                               const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<int64_t>(FunctionContext* ctx,
                                                               const StringVal& src,
                                                               const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<__int128>(FunctionContext* ctx,
                                                                const StringVal& src,
                                                                const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<float>(FunctionContext* ctx,
                                                             const StringVal& src,
                                                             const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<double>(FunctionContext* ctx,
                                                              const StringVal& src,
                                                              const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<DateTimeValue>(FunctionContext* ctx,
                                                                     const StringVal& src,
                                                                     const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<DecimalV2Value>(FunctionContext* ctx,
                                                                      const StringVal& src,
                                                                      const StringVal* dst);
template void BitmapFunctions::bitmap_intersect_merge<StringValue>(FunctionContext* ctx,
                                                                   const StringVal& src,
                                                                   const StringVal* dst);

template StringVal BitmapFunctions::bitmap_intersect_serialize<int8_t>(FunctionContext* ctx,
                                                                       const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<int16_t>(FunctionContext* ctx,
                                                                        const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<int32_t>(FunctionContext* ctx,
                                                                        const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<int64_t>(FunctionContext* ctx,
                                                                        const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<__int128>(FunctionContext* ctx,
                                                                         const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<float>(FunctionContext* ctx,
                                                                      const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<double>(FunctionContext* ctx,
                                                                       const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<DateTimeValue>(FunctionContext* ctx,
                                                                              const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<DecimalV2Value>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::bitmap_intersect_serialize<StringValue>(FunctionContext* ctx,
                                                                            const StringVal& src);

template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int8_t>(FunctionContext* ctx,
                                                                      const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int16_t>(FunctionContext* ctx,
                                                                       const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int32_t>(FunctionContext* ctx,
                                                                       const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<int64_t>(FunctionContext* ctx,
                                                                       const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<__int128>(FunctionContext* ctx,
                                                                        const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<float>(FunctionContext* ctx,
                                                                     const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<double>(FunctionContext* ctx,
                                                                      const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<DateTimeValue>(FunctionContext* ctx,
                                                                             const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<DecimalV2Value>(FunctionContext* ctx,
                                                                              const StringVal& src);
template BigIntVal BitmapFunctions::bitmap_intersect_finalize<StringValue>(FunctionContext* ctx,
                                                                           const StringVal& src);

template void BitmapFunctions::orthogonal_bitmap_intersect_count_init<int8_t, TinyIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_count_init<int16_t, SmallIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_count_init<int32_t, IntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_count_init<int64_t, BigIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_count_init<float, FloatVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_count_init<double, DoubleVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_count_init<StringValue, StringVal>(
        FunctionContext* ctx, StringVal* dst);

template void BitmapFunctions::orthogonal_bitmap_intersect_init<int8_t, TinyIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_init<int16_t, SmallIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_init<int32_t, IntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_init<int64_t, BigIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_init<float, FloatVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_init<double, DoubleVal>(
        FunctionContext* ctx, StringVal* dst);
template void BitmapFunctions::orthogonal_bitmap_intersect_init<StringValue, StringVal>(
        FunctionContext* ctx, StringVal* dst);

template StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize<int8_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize<int16_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize<int32_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize<int64_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize<float>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize<double>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_serialize<StringValue>(
        FunctionContext* ctx, const StringVal& src);

template BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize<int8_t>(
        FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize<int16_t>(
        FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize<int32_t>(
        FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize<int64_t>(
        FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize<float>(
        FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize<double>(
        FunctionContext* ctx, const StringVal& src);
template BigIntVal BitmapFunctions::orthogonal_bitmap_intersect_finalize<StringValue>(
        FunctionContext* ctx, const StringVal& src);

template StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize<int8_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize<int16_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize<int32_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize<int64_t>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize<float>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize<double>(
        FunctionContext* ctx, const StringVal& src);
template StringVal BitmapFunctions::orthogonal_bitmap_intersect_count_serialize<StringValue>(
        FunctionContext* ctx, const StringVal& src);
} // namespace doris
