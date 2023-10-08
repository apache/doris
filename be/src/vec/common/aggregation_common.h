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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/AggregationCommon.h
// and modified by Doris

#pragma once

#include <array>

#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/sip_hash.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"

namespace doris::vectorized {

inline size_t get_bitmap_size(size_t key_number) {
    return (key_number + 7) / 8;
}

using Sizes = std::vector<size_t>;

template <typename T>
std::vector<T> pack_fixeds(size_t row_numbers, const ColumnRawPtrs& key_columns,
                           const Sizes& key_sizes, const ColumnRawPtrs& nullmap_columns) {
    size_t bitmap_size = get_bitmap_size(nullmap_columns.size());

    std::vector<T> result(row_numbers);
    size_t offset = 0;
    if (bitmap_size > 0) {
        for (size_t j = 0; j < nullmap_columns.size(); j++) {
            if (!nullmap_columns[j]) {
                continue;
            }
            size_t bucket = j / 8;
            size_t offset = j % 8;
            const auto& data =
                    assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
            for (size_t i = 0; i < row_numbers; ++i) {
                *((char*)(&result[i]) + bucket) |= data[i] << offset;
            }
        }
        offset += bitmap_size;
    }

    for (size_t j = 0; j < key_columns.size(); ++j) {
        const char* data = key_columns[j]->get_raw_data().data;

        auto foo = [&]<typename Fixed>(Fixed zero) {
            CHECK_EQ(sizeof(Fixed), key_sizes[j]);
            if (nullmap_columns.size() && nullmap_columns[j]) {
                const auto& nullmap =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    // make sure null cell is filled by 0x0
                    memcpy_fixed<Fixed>((char*)(&result[i]) + offset,
                                        nullmap[i] ? (char*)&zero : data + i * sizeof(Fixed));
                }
            } else {
                for (size_t i = 0; i < row_numbers; ++i) {
                    memcpy_fixed<Fixed>((char*)(&result[i]) + offset, data + i * sizeof(Fixed));
                }
            }
        };

        if (key_sizes[j] == 1) {
            foo(int8_t());
        } else if (key_sizes[j] == 2) {
            foo(int16_t());
        } else if (key_sizes[j] == 4) {
            foo(int32_t());
        } else if (key_sizes[j] == 8) {
            foo(int64_t());
        } else if (key_sizes[j] == 16) {
            foo(UInt128());
        } else {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "pack_fixeds input invalid key size, key_size={}", key_sizes[j]);
        }
        offset += key_sizes[j];
    }
    return result;
}

/// Hash a set of keys into a UInt128 value.
inline UInt128 hash128(size_t i, size_t keys_size, const ColumnRawPtrs& key_columns) {
    UInt128 key;
    SipHash hash;

    for (size_t j = 0; j < keys_size; ++j) {
        key_columns[j]->update_hash_with_value(i, hash);
    }

    hash.get128(key.low, key.high);

    return key;
}

/// Copy keys to the pool. Then put into pool StringRefs to them and return the pointer to the first.
inline StringRef* place_keys_in_pool(size_t keys_size, StringRefs& keys, Arena& pool) {
    for (size_t j = 0; j < keys_size; ++j) {
        char* place = pool.alloc(keys[j].size);
        memcpy_small_allow_read_write_overflow15(place, keys[j].data, keys[j].size);
        keys[j].data = place;
    }

    /// Place the StringRefs on the newly copied keys in the pool.
    char* res = pool.aligned_alloc(keys_size * sizeof(StringRef), alignof(StringRef));
    memcpy_small_allow_read_write_overflow15(res, keys.data(), keys_size * sizeof(StringRef));

    return reinterpret_cast<StringRef*>(res);
}

/** Serialize keys into a continuous chunk of memory.
  */
inline StringRef serialize_keys_to_pool_contiguous(size_t i, size_t keys_size,
                                                   const ColumnRawPtrs& key_columns, Arena& pool) {
    const char* begin = nullptr;

    size_t sum_size = 0;
    for (size_t j = 0; j < keys_size; ++j) {
        sum_size += key_columns[j]->serialize_value_into_arena(i, pool, begin).size;
    }

    return {begin, sum_size};
}

} // namespace doris::vectorized
