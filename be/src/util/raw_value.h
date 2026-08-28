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

#include "common/check.h"
#include "common/consts.h"
#include "common/logging.h"
#include "core/data_type/define_primitive_type.h"
#include "core/packed_int128.h"
#include "core/string_ref.h"
#include "util/hash_util.hpp"

namespace doris {
class SlotDescriptor;

// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
public:
    // Same as the up function, only use in vec exec engine.
    static uint32_t zlib_crc32(const void* value, size_t len, const PrimitiveType& type,
                               uint32_t seed);

    // Treat the canonical distribution bytes of a value as an unsigned integer with the first byte
    // as the least-significant byte, then append it to the preceding distribution columns. The
    // returned value is kept modulo mod throughout, so values of any byte width and any number of
    // columns do not require a wide integer.
    static uint32_t identity_hash(const void* value, size_t len, const PrimitiveType& type,
                                  uint32_t seed, uint32_t mod);
};

inline uint32_t RawValue::identity_hash(const void* v, size_t len, const PrimitiveType& type,
                                         uint32_t seed, uint32_t mod) {
    DCHECK_GT(mod, 0);
    auto append_little_endian = [&seed, mod](const void* value, size_t size) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(value);
        uint64_t remainder = seed;
        size_t bytes_since_mod = 0;
        for (size_t i = size; i > 0; --i) {
            remainder = remainder * 256 + bytes[i - 1];
            if (++bytes_since_mod == sizeof(uint32_t)) {
                remainder %= mod;
                bytes_since_mod = 0;
            }
        }
        seed = static_cast<uint32_t>(remainder % mod);
    };

    if (v == nullptr) {
        static constexpr uint32_t NULL_VALUE = 0;
        append_little_endian(&NULL_VALUE, sizeof(NULL_VALUE));
        return seed;
    }

    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
    case TYPE_HLL:
    case TYPE_STRING:
    case TYPE_CHAR:
        append_little_endian(v, len);
        break;
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        append_little_endian(v, 1);
        break;
    case TYPE_SMALLINT:
        append_little_endian(v, 2);
        break;
    case TYPE_INT:
    case TYPE_FLOAT:
    case TYPE_DATEV2:
    case TYPE_DECIMAL32:
    case TYPE_IPV4:
        append_little_endian(v, 4);
        break;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
    case TYPE_TIMEV2:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
    case TYPE_DECIMAL64:
        append_little_endian(v, 8);
        break;
    case TYPE_LARGEINT:
    case TYPE_DECIMAL128I:
    case TYPE_IPV6:
        append_little_endian(v, 16);
        break;
    case TYPE_DECIMAL256:
        append_little_endian(v, 32);
        break;
    case TYPE_DATE:
    case TYPE_DATETIME: {
        const auto* date_val = reinterpret_cast<const VecDateTimeValue*>(v);
        char buf[64];
        int date_len = date_val->to_buffer(buf);
        append_little_endian(buf, date_len);
        break;
    }
    case TYPE_DECIMALV2: {
        const auto* dec_val = reinterpret_cast<const DecimalV2Value*>(v);
        int64_t int_val = dec_val->int_value();
        int32_t frac_val = dec_val->frac_value();
        append_little_endian(&frac_val, sizeof(frac_val));
        append_little_endian(&int_val, sizeof(int_val));
        break;
    }
    default:
        DORIS_CHECK(false) << "invalid type: " << type;
    }
    return seed;
}

// NOTE: this is just for split data, decimal use old doris hash function
// Because crc32 hardware is not equal with zlib crc32
inline uint32_t RawValue::zlib_crc32(const void* v, size_t len, const PrimitiveType& type,
                                     uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_STRING:
    case TYPE_CHAR: {
        return HashUtil::zlib_crc_hash(v, (uint32_t)len, seed);
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
        const auto* date_val = reinterpret_cast<const VecDateTimeValue*>(v);
        char buf[64];
        int date_len = date_val->to_buffer(buf);
        return HashUtil::zlib_crc_hash(buf, date_len, seed);
    }

    case TYPE_DATEV2: {
        return HashUtil::zlib_crc_hash(v, 4, seed);
    }

    case TYPE_DATETIMEV2: {
        return HashUtil::zlib_crc_hash(v, 8, seed);
    }

    case TYPE_TIMESTAMPTZ: {
        return HashUtil::zlib_crc_hash(v, 8, seed);
    }

    case TYPE_DECIMALV2: {
        const auto* dec_val = reinterpret_cast<const DecimalV2Value*>(v);
        int64_t int_val = dec_val->int_value();
        int32_t frac_val = dec_val->frac_value();
        seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), seed);
        return HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
    }
    case TYPE_DECIMAL32:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_DECIMAL64:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_DECIMAL128I:
        return HashUtil::zlib_crc_hash(v, 16, seed);
    case TYPE_DECIMAL256:
        return HashUtil::zlib_crc_hash(v, 32, seed);
    case TYPE_IPV4:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_IPV6:
        return HashUtil::zlib_crc_hash(v, 16, seed);
    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}
} // namespace doris
