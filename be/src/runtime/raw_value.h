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
#include "runtime/types.h"
#include "util/hash_util.hpp"
#include "util/types.h"
#include "vec/common/string_ref.h"

namespace doris {

class SlotDescriptor;

// Useful utility functions for runtime values (which are passed around as void*).
class RawValue {
public:
    // Same as the up function, only use in vec exec engine.
    static uint32_t zlib_crc32(const void* value, size_t len, const PrimitiveType& type,
                               uint32_t seed);
};

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
        auto* date_val = (const VecDateTimeValue*)v;
        char buf[64];
        int len = date_val->to_buffer(buf);
        return HashUtil::zlib_crc_hash(buf, len, seed);
    }

    case TYPE_DATEV2: {
        return HashUtil::zlib_crc_hash(v, 4, seed);
    }

    case TYPE_DATETIMEV2: {
        return HashUtil::zlib_crc_hash(v, 8, seed);
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
    case TYPE_DECIMAL128I:
        return HashUtil::zlib_crc_hash(v, 16, seed);
    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

} // namespace doris
