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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/anyval-util.h
// and modified by Doris

#pragma once

#include "common/status.h"
#include "exprs/expr.h"
#include "runtime/collection_value.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"
#include "udf/udf.h"
#include "util/hash_util.hpp"
#include "util/types.h"

namespace doris {

class MemPool;

// Utilities for AnyVals
class AnyValUtil {
public:
    static uint32_t hash(const doris_udf::BooleanVal& v, int seed) {
        return HashUtil::hash(&v.val, 1, seed);
    }

    static uint32_t hash(const doris_udf::TinyIntVal& v, int seed) {
        return HashUtil::hash(&v.val, 1, seed);
    }

    static uint32_t hash(const doris_udf::SmallIntVal& v, int seed) {
        return HashUtil::hash(&v.val, 2, seed);
    }

    static uint32_t hash(const doris_udf::IntVal& v, int seed) {
        return HashUtil::hash(&v.val, 4, seed);
    }

    static uint32_t hash(const doris_udf::BigIntVal& v, int seed) {
        return HashUtil::hash(&v.val, 8, seed);
    }

    static uint32_t hash(const doris_udf::FloatVal& v, int seed) {
        return HashUtil::hash(&v.val, 4, seed);
    }

    static uint32_t hash(const doris_udf::DoubleVal& v, int seed) {
        return HashUtil::hash(&v.val, 8, seed);
    }

    static uint32_t hash(const doris_udf::StringVal& v, int seed) {
        return HashUtil::hash(v.ptr, v.len, seed);
    }

    static uint32_t hash(const doris_udf::DateTimeVal& v, int seed) {
        DateTimeValue tv = DateTimeValue::from_datetime_val(v);
        return tv.hash(seed);
    }

    static uint32_t hash(const doris_udf::DecimalV2Val& v, int seed) {
        return HashUtil::hash(&v.val, 16, seed);
    }

    static uint32_t hash(const doris_udf::LargeIntVal& v, int seed) {
        return HashUtil::hash(&v.val, 8, seed);
    }

    static uint64_t hash64(const doris_udf::BooleanVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 1, seed);
    }

    static uint64_t hash64(const doris_udf::TinyIntVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 1, seed);
    }

    static uint64_t hash64(const doris_udf::SmallIntVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 2, seed);
    }

    static uint64_t hash64(const doris_udf::IntVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 4, seed);
    }

    static uint64_t hash64(const doris_udf::BigIntVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 8, seed);
    }

    static uint64_t hash64(const doris_udf::FloatVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 4, seed);
    }

    static uint64_t hash64(const doris_udf::DoubleVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 8, seed);
    }

    static uint64_t hash64(const doris_udf::StringVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(v.ptr, v.len, seed);
    }

    static uint64_t hash64(const doris_udf::DateTimeVal& v, int64_t seed) {
        DateTimeValue tv = DateTimeValue::from_datetime_val(v);
        return HashUtil::fnv_hash64(&tv, 12, seed);
    }

    static uint64_t hash64(const doris_udf::DecimalV2Val& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 16, seed);
    }

    static uint64_t hash64(const doris_udf::LargeIntVal& v, int64_t seed) {
        return HashUtil::fnv_hash64(&v.val, 8, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::BooleanVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 1, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::TinyIntVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 1, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::SmallIntVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 2, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::IntVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 4, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::BigIntVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 8, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::FloatVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 4, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::DoubleVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 8, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::StringVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(v.ptr, v.len, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::DateTimeVal& v, int64_t seed) {
        DateTimeValue tv = DateTimeValue::from_datetime_val(v);
        return HashUtil::murmur_hash64A(&tv, 12, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::DecimalV2Val& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 16, seed);
    }

    static uint64_t hash64_murmur(const doris_udf::LargeIntVal& v, int64_t seed) {
        return HashUtil::murmur_hash64A(&v.val, 8, seed);
    }

    template <typename Val>
    static Val min_val(FunctionContext* ctx) {
        if constexpr (std::is_same_v<Val, StringVal>) {
            return StringVal();
        } else if constexpr (std::is_same_v<Val, DateTimeVal>) {
            DateTimeVal val;
            type_limit<DateTimeValue>::min().to_datetime_val(&val);
            return val;
        } else if constexpr (std::is_same_v<Val, DecimalV2Val>) {
            DecimalV2Val val;
            type_limit<DecimalV2Value>::min().to_decimal_val(&val);
            return val;
        } else if constexpr (std::is_same_v<Val, DateV2Val>) {
            DateV2Val val;
            type_limit<doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>>::min()
                    .to_datev2_val(&val);
            return val;
        } else if constexpr (std::is_same_v<Val, DateTimeV2Val>) {
            DateTimeV2Val val;
            type_limit<
                    doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>>::min()
                    .to_datetimev2_val(&val);
            return val;
        } else {
            return Val(type_limit<decltype(std::declval<Val>().val)>::min());
        }
    }

    template <typename Val>
    static Val max_val(FunctionContext* ctx) {
        if constexpr (std::is_same_v<Val, StringVal>) {
            StringValue sv = type_limit<StringValue>::max();
            StringVal max_val;
            max_val.ptr = ctx->allocate(sv.len);
            memcpy(max_val.ptr, sv.ptr, sv.len);
            max_val.len = sv.len;

            return max_val;
        } else if constexpr (std::is_same_v<Val, DateTimeVal>) {
            DateTimeVal val;
            type_limit<DateTimeValue>::max().to_datetime_val(&val);
            return val;
        } else if constexpr (std::is_same_v<Val, DecimalV2Val>) {
            DecimalV2Val val;
            type_limit<DecimalV2Value>::max().to_decimal_val(&val);
            return val;
        } else if constexpr (std::is_same_v<Val, DateV2Val>) {
            DateV2Val val;
            type_limit<doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>>::max()
                    .to_datev2_val(&val);
            return val;
        } else if constexpr (std::is_same_v<Val, DateTimeV2Val>) {
            DateTimeV2Val val;
            type_limit<
                    doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>>::max()
                    .to_datetimev2_val(&val);
            return val;
        } else {
            return Val(type_limit<decltype(std::declval<Val>().val)>::max());
        }
    }

    // Returns the byte size of *Val for type t.
    static int any_val_size(const TypeDescriptor& t) {
        switch (t.type) {
        case TYPE_BOOLEAN:
            return sizeof(doris_udf::BooleanVal);

        case TYPE_TINYINT:
            return sizeof(doris_udf::TinyIntVal);

        case TYPE_SMALLINT:
            return sizeof(doris_udf::SmallIntVal);

        case TYPE_INT:
            return sizeof(doris_udf::IntVal);

        case TYPE_BIGINT:
            return sizeof(doris_udf::BigIntVal);

        case TYPE_LARGEINT:
            return sizeof(doris_udf::LargeIntVal);

        case TYPE_FLOAT:
            return sizeof(doris_udf::FloatVal);

        case TYPE_DOUBLE:
            return sizeof(doris_udf::DoubleVal);

        case TYPE_OBJECT:
        case TYPE_QUANTILE_STATE:
        case TYPE_HLL:
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING:
            return sizeof(doris_udf::StringVal);

        case TYPE_DATEV2:
            return sizeof(doris_udf::DateV2Val);
        case TYPE_DATETIMEV2:
            return sizeof(doris_udf::DateTimeV2Val);
        case TYPE_DATE:
        case TYPE_DATETIME:
            return sizeof(doris_udf::DateTimeVal);

        case TYPE_DECIMALV2:
            return sizeof(doris_udf::DecimalV2Val);

        case TYPE_ARRAY:
            return sizeof(doris_udf::CollectionVal);

        default:
            DCHECK(false) << t;
            return 0;
        }
    }

    /// Returns the byte alignment of *Val for type t.
    static int any_val_alignment(const TypeDescriptor& t) {
        switch (t.type) {
        case TYPE_BOOLEAN:
            return alignof(BooleanVal);
        case TYPE_TINYINT:
            return alignof(TinyIntVal);
        case TYPE_SMALLINT:
            return alignof(SmallIntVal);
        case TYPE_INT:
            return alignof(IntVal);
        case TYPE_BIGINT:
            return alignof(BigIntVal);
        case TYPE_LARGEINT:
            return alignof(LargeIntVal);
        case TYPE_FLOAT:
            return alignof(FloatVal);
        case TYPE_DOUBLE:
            return alignof(DoubleVal);
        case TYPE_OBJECT:
        case TYPE_QUANTILE_STATE:
        case TYPE_HLL:
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING:
            return alignof(StringVal);
        case TYPE_DATETIME:
        case TYPE_DATE:
            return alignof(DateTimeVal);
        case TYPE_DATEV2:
            return alignof(DateV2Val);
        case TYPE_DATETIMEV2:
            return alignof(DateTimeV2Val);
        case TYPE_DECIMALV2:
            return alignof(DecimalV2Val);
        case TYPE_ARRAY:
            return alignof(doris_udf::CollectionVal);
        default:
            DCHECK(false) << t;
            return 0;
        }
    }

    static std::string to_string(const StringVal& v) {
        return std::string(reinterpret_cast<char*>(v.ptr), v.len);
    }

    static StringVal from_string(FunctionContext* ctx, const std::string& s) {
        StringVal val = from_buffer(ctx, s.c_str(), s.size());
        return val;
    }

    static void TruncateIfNecessary(const FunctionContext::TypeDesc& type, StringVal* val) {
        if (type.type == FunctionContext::TYPE_VARCHAR || type.type == FunctionContext::TYPE_CHAR) {
            DCHECK(type.len >= 0);
            val->len = std::min(val->len, (int64_t)type.len);
        }
    }

    static StringVal from_buffer(FunctionContext* ctx, const char* ptr, int64_t len) {
        StringVal result(ctx, len);
        memcpy(result.ptr, ptr, len);
        return result;
    }

    static StringVal from_string_temp(FunctionContext* ctx, const std::string& s) {
        StringVal val = from_buffer_temp(ctx, s.c_str(), s.size());
        return val;
    }

    static StringVal from_buffer_temp(FunctionContext* ctx, const char* ptr, int64_t len) {
        StringVal result = StringVal::create_temp_string_val(ctx, len);
        memcpy(result.ptr, ptr, len);
        return result;
    }

    static FunctionContext::TypeDesc column_type_to_type_desc(const TypeDescriptor& type);

    // Utility to put val into an AnyVal struct
    static void set_any_val(const void* slot, const TypeDescriptor& type, doris_udf::AnyVal* dst) {
        if (slot == nullptr) {
            dst->is_null = true;
            return;
        }

        dst->is_null = false;
        switch (type.type) {
        case TYPE_NULL:
            return;
        case TYPE_BOOLEAN:
            reinterpret_cast<doris_udf::BooleanVal*>(dst)->val =
                    *reinterpret_cast<const bool*>(slot);
            return;
        case TYPE_TINYINT:
            reinterpret_cast<doris_udf::TinyIntVal*>(dst)->val =
                    *reinterpret_cast<const int8_t*>(slot);
            return;
        case TYPE_SMALLINT:
            reinterpret_cast<doris_udf::SmallIntVal*>(dst)->val =
                    *reinterpret_cast<const int16_t*>(slot);
            return;
        case TYPE_INT:
            reinterpret_cast<doris_udf::IntVal*>(dst)->val =
                    *reinterpret_cast<const int32_t*>(slot);
            return;
        case TYPE_BIGINT:
            reinterpret_cast<doris_udf::BigIntVal*>(dst)->val =
                    *reinterpret_cast<const int64_t*>(slot);
            return;
        case TYPE_LARGEINT:
            memcpy(&reinterpret_cast<doris_udf::LargeIntVal*>(dst)->val, slot, sizeof(__int128));
            return;
        case TYPE_FLOAT:
            reinterpret_cast<doris_udf::FloatVal*>(dst)->val =
                    *reinterpret_cast<const float*>(slot);
            return;
        case TYPE_TIME:
        case TYPE_DOUBLE:
            reinterpret_cast<doris_udf::DoubleVal*>(dst)->val =
                    *reinterpret_cast<const double*>(slot);
            return;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_QUANTILE_STATE:
        case TYPE_STRING:
            reinterpret_cast<const StringValue*>(slot)->to_string_val(
                    reinterpret_cast<doris_udf::StringVal*>(dst));
            return;
        case TYPE_DECIMALV2:
            reinterpret_cast<doris_udf::DecimalV2Val*>(dst)->val =
                    reinterpret_cast<const PackedInt128*>(slot)->value;
            return;
        case TYPE_DECIMAL32:
            reinterpret_cast<doris_udf::Decimal32Val*>(dst)->val =
                    *reinterpret_cast<const int32_t*>(slot);
            return;
        case TYPE_DECIMAL64:
            reinterpret_cast<doris_udf::Decimal64Val*>(dst)->val =
                    *reinterpret_cast<const int64_t*>(slot);
            return;
        case TYPE_DECIMAL128:
            memcpy(&reinterpret_cast<doris_udf::Decimal128Val*>(dst)->val, slot, sizeof(__int128));
            return;

        case TYPE_DATE:
            reinterpret_cast<const DateTimeValue*>(slot)->to_datetime_val(
                    reinterpret_cast<doris_udf::DateTimeVal*>(dst));
            return;
        case TYPE_DATETIME:
            reinterpret_cast<const DateTimeValue*>(slot)->to_datetime_val(
                    reinterpret_cast<doris_udf::DateTimeVal*>(dst));

        case TYPE_DATEV2:
            reinterpret_cast<
                    const doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*>(slot)
                    ->to_datev2_val(reinterpret_cast<doris_udf::DateV2Val*>(dst));
            return;
        case TYPE_DATETIMEV2:
            reinterpret_cast<
                    const doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*>(
                    slot)
                    ->to_datev2_val(reinterpret_cast<doris_udf::DateV2Val*>(dst));
            return;
        case TYPE_ARRAY:
            reinterpret_cast<const CollectionValue*>(slot)->to_collection_val(
                    reinterpret_cast<CollectionVal*>(dst));
            return;
        default:
            DCHECK(false) << "NYI";
        }
    }

    /// Templated equality functions. These assume the input values are not nullptr.
    template <typename T>
    static bool equals(const PrimitiveType& type, const T& x, const T& y) {
        return equals_internal(x, y);
    }

    /// Templated equality functions. These assume the input values are not nullptr.
    template <typename T>
    static bool equals(const T& x, const T& y) {
        return equals_internal(x, y);
    }

    template <typename T>
    static bool equals(const TypeDescriptor& type, const T& x, const T& y) {
        return equals_internal(x, y);
    }

    template <typename T>
    static bool equals(const FunctionContext::TypeDesc& type, const T& x, const T& y) {
        return equals_internal(x, y);
    }

private:
    /// Implementations of Equals().
    template <typename T>
    static bool equals_internal(const T& x, const T& y);
};

template <typename T>
inline bool AnyValUtil::equals_internal(const T& x, const T& y) {
    DCHECK(!x.is_null);
    DCHECK(!y.is_null);
    return x.val == y.val;
}

template <>
inline bool AnyValUtil::equals_internal(const StringVal& x, const StringVal& y) {
    DCHECK(!x.is_null);
    DCHECK(!y.is_null);
    StringValue x_sv = StringValue::from_string_val(x);
    StringValue y_sv = StringValue::from_string_val(y);
    return x_sv == y_sv;
}

template <>
inline bool AnyValUtil::equals_internal(const DateTimeVal& x, const DateTimeVal& y) {
    DCHECK(!x.is_null);
    DCHECK(!y.is_null);
    DateTimeValue x_tv = DateTimeValue::from_datetime_val(x);
    DateTimeValue y_tv = DateTimeValue::from_datetime_val(y);
    return x_tv == y_tv;
}

template <>
inline bool AnyValUtil::equals_internal(const DecimalV2Val& x, const DecimalV2Val& y) {
    DCHECK(!x.is_null);
    DCHECK(!y.is_null);
    return x == y;
}

// Creates the corresponding AnyVal subclass for type. The object is added to the pool.
doris_udf::AnyVal* create_any_val(ObjectPool* pool, const TypeDescriptor& type);

/// Allocates an AnyVal subclass of 'type' from 'pool'. The AnyVal's memory is
/// initialized to all 0's. Returns a MemLimitExceeded() error with message
/// 'mem_limit_exceeded_msg' if the allocation cannot be made because of a memory
/// limit.
Status allocate_any_val(RuntimeState* state, MemPool* pool, const TypeDescriptor& type,
                        const std::string& mem_limit_exceeded_msg, AnyVal** result);

} // namespace doris
