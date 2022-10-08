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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/raw-value.h
// and modified by Doris

#pragma once

#include <string>

#include "common/consts.h"
#include "common/logging.h"
#include "runtime/string_value.h"
#include "runtime/types.h"
#include "util/hash_util.hpp"
#include "util/types.h"

namespace doris {

class MemPool;
class SlotDescriptor;
class Tuple;

// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
public:
    // Ascii output precision for double/float
    static const int ASCII_PRECISION;

    // Convert 'value' into ascii and write to 'stream'. nullptr turns into NULL. 'scale'
    // determines how many digits after the decimal are printed for floating point numbers,
    // -1 indicates to use the stream's current formatting.
    static void print_value(const void* value, const TypeDescriptor& type, int scale,
                            std::stringstream* stream);

    // write ascii value to string instead of stringstream.
    static void print_value(const void* value, const TypeDescriptor& type, int scale,
                            std::string* str);

    // Writes the byte representation of a value to a stringstream character-by-character
    static void print_value_as_bytes(const void* value, const TypeDescriptor& type,
                                     std::stringstream* stream);

    static uint32_t get_hash_value(const void* value, const PrimitiveType& type) {
        return get_hash_value(value, type, 0);
    }

    static uint32_t get_hash_value(const void* value, const PrimitiveType& type, uint32_t seed);

    // Returns hash value for 'value' interpreted as 'type'.  The resulting hash value
    // is combined with the seed value.
    static uint32_t get_hash_value(const void* value, const TypeDescriptor& type, uint32_t seed) {
        return get_hash_value(value, type.type, seed);
    }

    static uint32_t get_hash_value(const void* value, const TypeDescriptor& type) {
        return get_hash_value(value, type.type, 0);
    }

    // Get the hash value using the fvn hash function.  Using different seeds with FVN
    // results in different hash functions.  get_hash_value() does not have this property
    // and cannot be safely used as the first step in data repartitioning.
    // However, get_hash_value() can be significantly faster.
    // TODO: fix get_hash_value
    static uint32_t get_hash_value_fvn(const void* value, const PrimitiveType& type, uint32_t seed);

    static uint32_t get_hash_value_fvn(const void* value, const TypeDescriptor& type,
                                       uint32_t seed) {
        return get_hash_value_fvn(value, type.type, seed);
    }

    // Get the hash value using the fvn hash function.  Using different seeds with FVN
    // results in different hash functions.  get_hash_value() does not have this property
    // and cannot be safely used as the first step in data repartitioning.
    // However, get_hash_value() can be significantly faster.
    // TODO: fix get_hash_value
    static uint32_t zlib_crc32(const void* value, const TypeDescriptor& type, uint32_t seed);

    // Same as the up function, only use in vec exec engine.
    static uint32_t zlib_crc32(const void* value, size_t len, const TypeDescriptor& type,
                               uint32_t seed);

    // Compares both values.
    // Return value is < 0  if v1 < v2, 0 if v1 == v2, > 0 if v1 > v2.
    static int compare(const void* v1, const void* v2, const TypeDescriptor& type);

    // Writes the bytes of a given value into the slot of a tuple.
    // For string values, the string data is copied into memory allocated from 'pool'
    // only if pool is non-nullptr.
    static void write(const void* value, Tuple* tuple, const SlotDescriptor* slot_desc,
                      MemPool* pool);

    // Writes 'src' into 'dst' for type.
    // For string values, the string data is copied into 'pool' if pool is non-nullptr.
    // src must be non-nullptr.
    static void write(const void* src, void* dst, const TypeDescriptor& type, MemPool* pool);

    // Writes 'src' into 'dst' for type.
    // String values are copied into *buffer and *buffer is updated by the length. *buf
    // must be preallocated to be large enough.
    static void write(const void* src, const TypeDescriptor& type, void* dst, uint8_t** buf);

    // Returns true if v1 == v2.
    // This is more performant than compare() == 0 for string equality, mostly because of
    // the length comparison check.
    static bool eq(const void* v1, const void* v2, const TypeDescriptor& type);

    static bool lt(const void* v1, const void* v2, const TypeDescriptor& type);
};

inline bool RawValue::lt(const void* v1, const void* v2, const TypeDescriptor& type) {
    const StringValue* string_value1;
    const StringValue* string_value2;

    switch (type.type) {
    case TYPE_BOOLEAN:
        return *reinterpret_cast<const bool*>(v1) < *reinterpret_cast<const bool*>(v2);

    case TYPE_TINYINT:
        return *reinterpret_cast<const int8_t*>(v1) < *reinterpret_cast<const int8_t*>(v2);

    case TYPE_SMALLINT:
        return *reinterpret_cast<const int16_t*>(v1) < *reinterpret_cast<const int16_t*>(v2);

    case TYPE_INT:
        return *reinterpret_cast<const int32_t*>(v1) < *reinterpret_cast<const int32_t*>(v2);

    case TYPE_BIGINT:
        return *reinterpret_cast<const int64_t*>(v1) < *reinterpret_cast<const int64_t*>(v2);

    case TYPE_FLOAT:
        return *reinterpret_cast<const float*>(v1) < *reinterpret_cast<const float*>(v2);

    case TYPE_DOUBLE:
        return *reinterpret_cast<const double*>(v1) < *reinterpret_cast<const double*>(v2);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_STRING:
        string_value1 = reinterpret_cast<const StringValue*>(v1);
        string_value2 = reinterpret_cast<const StringValue*>(v2);
        return string_value1->lt(*string_value2);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return *reinterpret_cast<const DateTimeValue*>(v1) <
               *reinterpret_cast<const DateTimeValue*>(v2);

    case TYPE_DATEV2:
        return *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*>(v1) <
               *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*>(v2);

    case TYPE_DATETIMEV2:
        return *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*>(v1) <
               *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*>(v2);

    case TYPE_DECIMALV2:
        return reinterpret_cast<const PackedInt128*>(v1)->value <
               reinterpret_cast<const PackedInt128*>(v2)->value;

    case TYPE_DECIMAL32:
        return *reinterpret_cast<const int32_t*>(v1) < *reinterpret_cast<const int32_t*>(v2);
    case TYPE_DECIMAL64:
        return *reinterpret_cast<const int64_t*>(v1) < *reinterpret_cast<const int64_t*>(v2);
    case TYPE_DECIMAL128:
        return reinterpret_cast<const PackedInt128*>(v1)->value <
               reinterpret_cast<const PackedInt128*>(v2)->value;

    case TYPE_LARGEINT:
        return reinterpret_cast<const PackedInt128*>(v1)->value <
               reinterpret_cast<const PackedInt128*>(v2)->value;

    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    };
}
inline bool RawValue::eq(const void* v1, const void* v2, const TypeDescriptor& type) {
    const StringValue* string_value1;
    const StringValue* string_value2;

    switch (type.type) {
    case TYPE_BOOLEAN:
        return *reinterpret_cast<const bool*>(v1) == *reinterpret_cast<const bool*>(v2);

    case TYPE_TINYINT:
        return *reinterpret_cast<const int8_t*>(v1) == *reinterpret_cast<const int8_t*>(v2);

    case TYPE_SMALLINT:
        return *reinterpret_cast<const int16_t*>(v1) == *reinterpret_cast<const int16_t*>(v2);

    case TYPE_INT:
        return *reinterpret_cast<const int32_t*>(v1) == *reinterpret_cast<const int32_t*>(v2);

    case TYPE_BIGINT:
        return *reinterpret_cast<const int64_t*>(v1) == *reinterpret_cast<const int64_t*>(v2);

    case TYPE_FLOAT:
        return *reinterpret_cast<const float*>(v1) == *reinterpret_cast<const float*>(v2);

    case TYPE_DOUBLE:
        return *reinterpret_cast<const double*>(v1) == *reinterpret_cast<const double*>(v2);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_STRING:
        string_value1 = reinterpret_cast<const StringValue*>(v1);
        string_value2 = reinterpret_cast<const StringValue*>(v2);
        return string_value1->eq(*string_value2);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return *reinterpret_cast<const DateTimeValue*>(v1) ==
               *reinterpret_cast<const DateTimeValue*>(v2);

    case TYPE_DATEV2:
        return *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*>(v1) ==
               *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*>(v2);

    case TYPE_DATETIMEV2:
        return *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*>(
                       v1) ==
               *reinterpret_cast<
                       const vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*>(v2);

    case TYPE_DECIMALV2:
        return reinterpret_cast<const PackedInt128*>(v1)->value ==
               reinterpret_cast<const PackedInt128*>(v2)->value;

    case TYPE_DECIMAL32:
        return *reinterpret_cast<const int32_t*>(v1) == *reinterpret_cast<const int32_t*>(v2);
    case TYPE_DECIMAL64:
        return *reinterpret_cast<const int64_t*>(v1) == *reinterpret_cast<const int64_t*>(v2);
    case TYPE_DECIMAL128:
        return reinterpret_cast<const PackedInt128*>(v1)->value ==
               reinterpret_cast<const PackedInt128*>(v2)->value;

    case TYPE_LARGEINT:
        return reinterpret_cast<const PackedInt128*>(v1)->value ==
               reinterpret_cast<const PackedInt128*>(v2)->value;

    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    };
}

// Use boost::hash_combine for corner cases.  boost::hash_combine is reimplemented
// here to use int32t's (instead of size_t)
// boost::hash_combine does:
//  seed ^= v + 0x9e3779b9 + (seed << 6) + (seed >> 2);
inline uint32_t RawValue::get_hash_value(const void* v, const PrimitiveType& type, uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_STRING: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        return HashUtil::hash(string_value->ptr, string_value->len, seed);
    }

    case TYPE_BOOLEAN: {
        uint32_t value = *reinterpret_cast<const bool*>(v) + 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    case TYPE_TINYINT:
        return HashUtil::hash(v, 1, seed);

    case TYPE_SMALLINT:
        return HashUtil::hash(v, 2, seed);

    case TYPE_INT:
        return HashUtil::hash(v, 4, seed);

    case TYPE_BIGINT:
        return HashUtil::hash(v, 8, seed);

    case TYPE_FLOAT:
        return HashUtil::hash(v, 4, seed);

    case TYPE_DOUBLE:
        return HashUtil::hash(v, 8, seed);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return HashUtil::hash(v, 16, seed);

    case TYPE_DATEV2:
        return HashUtil::hash(v, 4, seed);

    case TYPE_DATETIMEV2:
        return HashUtil::hash(v, 8, seed);

    case TYPE_DECIMALV2:
        return HashUtil::hash(v, 16, seed);
    case TYPE_DECIMAL32:
        return HashUtil::hash(v, 4, seed);
    case TYPE_DECIMAL64:
        return HashUtil::hash(v, 8, seed);
    case TYPE_DECIMAL128:
        return HashUtil::hash(v, 16, seed);

    case TYPE_LARGEINT:
        return HashUtil::hash(v, 16, seed);

    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

inline uint32_t RawValue::get_hash_value_fvn(const void* v, const PrimitiveType& type,
                                             uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_STRING: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        return HashUtil::fnv_hash(string_value->ptr, string_value->len, seed);
    }

    case TYPE_BOOLEAN: {
        uint32_t value = *reinterpret_cast<const bool*>(v) + 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    case TYPE_TINYINT:
        return HashUtil::fnv_hash(v, 1, seed);

    case TYPE_SMALLINT:
        return HashUtil::fnv_hash(v, 2, seed);

    case TYPE_INT:
        return HashUtil::fnv_hash(v, 4, seed);

    case TYPE_BIGINT:
        return HashUtil::fnv_hash(v, 8, seed);

    case TYPE_FLOAT:
        return HashUtil::fnv_hash(v, 4, seed);

    case TYPE_DOUBLE:
        return HashUtil::fnv_hash(v, 8, seed);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return HashUtil::fnv_hash(v, 16, seed);

    case TYPE_DATEV2:
        return HashUtil::fnv_hash(v, 4, seed);

    case TYPE_DATETIMEV2:
        return HashUtil::fnv_hash(v, 8, seed);

    case TYPE_DECIMALV2:
        return HashUtil::fnv_hash(v, 16, seed);
    case TYPE_DECIMAL32:
        return HashUtil::fnv_hash(v, 4, seed);
    case TYPE_DECIMAL64:
        return HashUtil::fnv_hash(v, 8, seed);
    case TYPE_DECIMAL128:
        return HashUtil::fnv_hash(v, 16, seed);

    case TYPE_LARGEINT:
        return HashUtil::fnv_hash(v, 16, seed);

    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

// NOTE: this is just for split data, decimal use old doris hash function
// Because crc32 hardware is not equal with zlib crc32
inline uint32_t RawValue::zlib_crc32(const void* v, const TypeDescriptor& type, uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type.type) {
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_STRING: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        return HashUtil::zlib_crc_hash(string_value->ptr, string_value->len, seed);
    }
    case TYPE_CHAR: {
        // TODO(zc): ugly, use actual value to compute hash value
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        int len = 0;
        while (len < string_value->len) {
            if (string_value->ptr[len] == '\0') {
                break;
            }
            len++;
        }
        return HashUtil::zlib_crc_hash(string_value->ptr, len, seed);
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return HashUtil::zlib_crc_hash(v, 1, seed);
    case TYPE_SMALLINT:
        return HashUtil::zlib_crc_hash(v, 2, seed);
    case TYPE_INT:
    case TYPE_DATEV2:
    case TYPE_DECIMAL32:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_BIGINT:
    case TYPE_DATETIMEV2:
    case TYPE_DECIMAL64:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_LARGEINT:
    case TYPE_DECIMAL128:
        return HashUtil::zlib_crc_hash(v, 16, seed);
    case TYPE_FLOAT:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_DOUBLE:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_DATE:
    case TYPE_DATETIME: {
        const DateTimeValue* date_val = (const DateTimeValue*)v;
        char buf[64];
        int len = date_val->to_buffer(buf);
        return HashUtil::zlib_crc_hash(buf, len, seed);
    }

    case TYPE_DECIMALV2: {
        const DecimalV2Value* dec_val = (const DecimalV2Value*)v;
        int64_t int_val = dec_val->int_value();
        int32_t frac_val = dec_val->frac_value();
        seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), seed);
        return HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
    }
    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

// NOTE: this is just for split data, decimal use old doris hash function
// Because crc32 hardware is not equal with zlib crc32
inline uint32_t RawValue::zlib_crc32(const void* v, size_t len, const TypeDescriptor& type,
                                     uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type.type) {
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_STRING:
    case TYPE_CHAR: {
        return HashUtil::zlib_crc_hash(v, len, seed);
    }

    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return HashUtil::zlib_crc_hash(v, 1, seed);
    case TYPE_SMALLINT:
        return HashUtil::zlib_crc_hash(v, 2, seed);
    case TYPE_INT:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_BIGINT:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_LARGEINT:
        return HashUtil::zlib_crc_hash(v, 16, seed);
    case TYPE_FLOAT:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_DOUBLE:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_DATE:
    case TYPE_DATETIME: {
        auto* date_val = (const vectorized::VecDateTimeValue*)v;
        char buf[64];
        int len = date_val->to_buffer(buf);
        return HashUtil::zlib_crc_hash(buf, len, seed);
    }

    case TYPE_DATEV2: {
        auto* date_v2_val = (const vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*)v;
        char buf[64];
        int date_v2_len = date_v2_val->to_buffer(buf);
        return HashUtil::zlib_crc_hash(buf, date_v2_len, seed);
    }

    case TYPE_DATETIMEV2: {
        auto* date_v2_val =
                (const vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*)v;
        char buf[64];
        int date_v2_len = date_v2_val->to_buffer(buf);
        return HashUtil::zlib_crc_hash(buf, date_v2_len, seed);
    }

    case TYPE_DECIMALV2: {
        const DecimalV2Value* dec_val = (const DecimalV2Value*)v;
        int64_t int_val = dec_val->int_value();
        int32_t frac_val = dec_val->frac_value();
        seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), seed);
        return HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
    }
    case TYPE_DECIMAL32:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_DECIMAL64:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_DECIMAL128:
        return HashUtil::zlib_crc_hash(v, 16, seed);
    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

} // namespace doris
