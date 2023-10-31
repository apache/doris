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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnMap.cpp
// and modified by Doris

#include "vec/columns/column_map.h"

#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <limits>
#include <memory>
#include <vector>

#include "common/status.h"
#include "vec/common/arena.h"
#include "vec/common/typeid_cast.h"
#include "vec/common/unaligned.h"

class SipHash;

namespace doris::vectorized {

/** A column of map values.
  */
std::string ColumnMap::get_name() const {
    return "Map(" + keys_column->get_name() + ", " + values_column->get_name() + ")";
}

ColumnMap::ColumnMap(MutableColumnPtr&& keys, MutableColumnPtr&& values, MutableColumnPtr&& offsets)
        : keys_column(std::move(keys)),
          values_column(std::move(values)),
          offsets_column(std::move(offsets)) {
    const COffsets* offsets_concrete = typeid_cast<const COffsets*>(offsets_column.get());

    if (!offsets_concrete) {
        LOG(FATAL) << "offsets_column must be a ColumnUInt64";
    }

    if (!offsets_concrete->empty() && keys && values) {
        auto last_offset = offsets_concrete->get_data().back();

        /// This will also prevent possible overflow in offset.
        if (keys_column->size() != last_offset) {
            LOG(FATAL) << "offsets_column has data inconsistent with key_column";
        }
        if (values_column->size() != last_offset) {
            LOG(FATAL) << "offsets_column has data inconsistent with value_column";
        }
    }
}

// todo. here to resize every row map
MutableColumnPtr ColumnMap::clone_resized(size_t to_size) const {
    auto res = ColumnMap::create(get_keys().clone_empty(), get_values().clone_empty(),
                                 COffsets::create());
    if (to_size == 0) {
        return res;
    }

    size_t from_size = size();

    if (to_size <= from_size) {
        res->get_offsets().assign(get_offsets().begin(), get_offsets().begin() + to_size);
        res->get_keys().insert_range_from(get_keys(), 0, get_offsets()[to_size - 1]);
        res->get_values().insert_range_from(get_values(), 0, get_offsets()[to_size - 1]);
    } else {
        /// Copy column and append empty arrays for extra elements.
        Offset64 offset = 0;
        if (from_size > 0) {
            res->get_offsets().assign(get_offsets().begin(), get_offsets().end());
            res->get_keys().insert_range_from(get_keys(), 0, get_keys().size());
            res->get_values().insert_range_from(get_values(), 0, get_values().size());
            offset = get_offsets().back();
        }
        res->get_offsets().resize(to_size);
        for (size_t i = from_size; i < to_size; ++i) {
            res->get_offsets()[i] = offset;
        }
    }
    return res;
}

// to support field functions
Field ColumnMap::operator[](size_t n) const {
    size_t start_offset = offset_at(n);
    size_t element_size = size_at(n);

    if (element_size > max_array_size_as_field) {
        LOG(FATAL) << "element size " << start_offset
                   << " is too large to be manipulated as single map field,"
                   << "maximum size " << max_array_size_as_field;
    }

    Array k(element_size), v(element_size);

    for (size_t i = 0; i < element_size; ++i) {
        k[i] = get_keys()[start_offset + i];
        v[i] = get_values()[start_offset + i];
    }

    return Map {k, v};
}

// here to compare to below
void ColumnMap::get(size_t n, Field& res) const {
    res = operator[](n);
}

StringRef ColumnMap::get_data_at(size_t n) const {
    LOG(FATAL) << "Method get_data_at is not supported for " << get_name();
}

void ColumnMap::insert_data(const char*, size_t) {
    LOG(FATAL) << "Method insert_data is not supported for " << get_name();
}

void ColumnMap::insert(const Field& x) {
    const auto& map = doris::vectorized::get<const Map&>(x);
    CHECK_EQ(map.size(), 2);
    const auto& k_f = doris::vectorized::get<const Array&>(map[0]);
    const auto& v_f = doris::vectorized::get<const Array&>(map[1]);

    size_t element_size = k_f.size();

    for (size_t i = 0; i < element_size; ++i) {
        keys_column->insert(k_f[i]);
        values_column->insert(v_f[i]);
    }
    get_offsets().push_back(get_offsets().back() + element_size);
}

void ColumnMap::insert_default() {
    auto last_offset = get_offsets().back();
    get_offsets().push_back(last_offset);
}

void ColumnMap::pop_back(size_t n) {
    auto& offsets_data = get_offsets();
    DCHECK(n <= offsets_data.size());
    size_t elems_size = offsets_data.back() - offset_at(offsets_data.size() - n);

    DCHECK_EQ(keys_column->size(), values_column->size());
    if (elems_size) {
        keys_column->pop_back(elems_size);
        values_column->pop_back(elems_size);
    }

    offsets_data.resize_assume_reserved(offsets_data.size() - n);
}

void ColumnMap::insert_from(const IColumn& src_, size_t n) {
    DCHECK(n < src_.size());
    const ColumnMap& src = assert_cast<const ColumnMap&>(src_);
    size_t size = src.size_at(n);
    size_t offset = src.offset_at(n);

    if ((!get_keys().is_nullable() && src.get_keys().is_nullable()) ||
        (!get_values().is_nullable() && src.get_values().is_nullable())) {
        DCHECK(false);
    } else if ((get_keys().is_nullable() && !src.get_keys().is_nullable()) ||
               (get_values().is_nullable() && !src.get_values().is_nullable())) {
        DCHECK(false);
    } else {
        keys_column->insert_range_from(src.get_keys(), offset, size);
        values_column->insert_range_from(src.get_values(), offset, size);
    }

    get_offsets().push_back(get_offsets().back() + size);
}

void ColumnMap::insert_indices_from(const IColumn& src, const int* indices_begin,
                                    const int* indices_end) {
    for (auto x = indices_begin; x != indices_end; ++x) {
        if (*x == -1) {
            ColumnMap::insert_default();
        } else {
            ColumnMap::insert_from(src, *x);
        }
    }
}

void ColumnMap::insert_indices_from_join(const IColumn& src, const uint32_t* indices_begin,
                                         const uint32_t* indices_end) {
    for (auto x = indices_begin; x != indices_end; ++x) {
        if (*x == 0) {
            ColumnMap::insert_default();
        } else {
            ColumnMap::insert_from(src, *x);
        }
    }
}

StringRef ColumnMap::serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const {
    size_t array_size = size_at(n);
    size_t offset = offset_at(n);

    char* pos = arena.alloc_continue(sizeof(array_size), begin);
    memcpy(pos, &array_size, sizeof(array_size));
    StringRef res(pos, sizeof(array_size));

    for (size_t i = 0; i < array_size; ++i) {
        auto value_ref = get_keys().serialize_value_into_arena(offset + i, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    for (size_t i = 0; i < array_size; ++i) {
        auto value_ref = get_values().serialize_value_into_arena(offset + i, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    return res;
}

const char* ColumnMap::deserialize_and_insert_from_arena(const char* pos) {
    size_t array_size = unaligned_load<size_t>(pos);
    pos += 2 * sizeof(array_size);

    for (size_t i = 0; i < array_size; ++i) {
        pos = get_keys().deserialize_and_insert_from_arena(pos);
    }

    for (size_t i = 0; i < array_size; ++i) {
        pos = get_values().deserialize_and_insert_from_arena(pos);
    }

    get_offsets().push_back(get_offsets().back() + array_size);
    return pos;
}

void ColumnMap::update_hash_with_value(size_t n, SipHash& hash) const {
    size_t kv_size = size_at(n);
    size_t offset = offset_at(n);

    hash.update(reinterpret_cast<const char*>(&kv_size), sizeof(kv_size));
    for (size_t i = 0; i < kv_size; ++i) {
        get_keys().update_hash_with_value(offset + i, hash);
        get_values().update_hash_with_value(offset + i, hash);
    }
}

void ColumnMap::update_hashes_with_value(std::vector<SipHash>& hashes,
                                         const uint8_t* __restrict null_data) const {
    SIP_HASHES_FUNCTION_COLUMN_IMPL();
}

void ColumnMap::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                         const uint8_t* __restrict null_data) const {
    auto& offsets = get_offsets();
    if (null_data) {
        for (size_t i = start; i < end; ++i) {
            if (null_data[i] == 0) {
                size_t kv_size = offsets[i] - offsets[i - 1];
                if (kv_size == 0) {
                    hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&kv_size),
                                                      sizeof(kv_size), hash);
                } else {
                    get_keys().update_xxHash_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                    get_values().update_xxHash_with_value(offsets[i - 1], offsets[i], hash,
                                                          nullptr);
                }
            }
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            size_t kv_size = offsets[i] - offsets[i - 1];
            if (kv_size == 0) {
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&kv_size),
                                                  sizeof(kv_size), hash);
            } else {
                get_keys().update_xxHash_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                get_values().update_xxHash_with_value(offsets[i - 1], offsets[i], hash, nullptr);
            }
        }
    }
}

void ColumnMap::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                      const uint8_t* __restrict null_data) const {
    auto& offsets = get_offsets();
    if (null_data) {
        for (size_t i = start; i < end; ++i) {
            if (null_data[i] == 0) {
                size_t kv_size = offsets[i] - offsets[i - 1];
                if (kv_size == 0) {
                    hash = HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(&kv_size),
                                                   sizeof(kv_size), hash);
                } else {
                    get_keys().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                    get_values().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                }
            }
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            size_t kv_size = offsets[i] - offsets[i - 1];
            if (kv_size == 0) {
                hash = HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(&kv_size),
                                               sizeof(kv_size), hash);
            } else {
                get_keys().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                get_values().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
            }
        }
    }
}

void ColumnMap::update_hashes_with_value(uint64_t* hashes, const uint8_t* null_data) const {
    size_t s = size();
    if (null_data) {
        for (size_t i = 0; i < s; ++i) {
            // every row
            if (null_data[i] == 0) {
                update_xxHash_with_value(i, i + 1, hashes[i], nullptr);
            }
        }
    } else {
        for (size_t i = 0; i < s; ++i) {
            update_xxHash_with_value(i, i + 1, hashes[i], nullptr);
        }
    }
}

void ColumnMap::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                       uint32_t offset, const uint8_t* __restrict null_data) const {
    auto s = rows;
    DCHECK(s == size());

    if (null_data) {
        for (size_t i = 0; i < s; ++i) {
            // every row
            if (null_data[i] == 0) {
                update_crc_with_value(i, i + 1, hash[i], nullptr);
            }
        }
    } else {
        for (size_t i = 0; i < s; ++i) {
            update_crc_with_value(i, i + 1, hash[i], nullptr);
        }
    }
}

void ColumnMap::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }

    const ColumnMap& src_concrete = assert_cast<const ColumnMap&>(src);

    if (start + length > src_concrete.size()) {
        LOG(FATAL) << "Parameter out of bound in ColumnMap::insert_range_from method. [start("
                   << std::to_string(start) << ") + length(" << std::to_string(length)
                   << ") > offsets.size(" << std::to_string(src_concrete.size()) << ")]";
    }

    size_t nested_offset = src_concrete.offset_at(start);
    size_t nested_length = src_concrete.offset_at(start + length) - nested_offset;

    keys_column->insert_range_from(src_concrete.get_keys(), nested_offset, nested_length);
    values_column->insert_range_from(src_concrete.get_values(), nested_offset, nested_length);

    auto& cur_offsets = get_offsets();
    const auto& src_offsets = src_concrete.get_offsets();

    if (start == 0 && cur_offsets.empty()) {
        cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
    } else {
        size_t old_size = cur_offsets.size();
        // -1 is ok, because PaddedPODArray pads zeros on the left.
        size_t prev_max_offset = cur_offsets.back();
        cur_offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i) {
            cur_offsets[old_size + i] = src_offsets[start + i] - nested_offset + prev_max_offset;
        }
    }
}

ColumnPtr ColumnMap::filter(const Filter& filt, ssize_t result_size_hint) const {
    auto k_arr =
            ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
                    ->filter(filt, result_size_hint);
    auto v_arr =
            ColumnArray::create(values_column->assume_mutable(), offsets_column->assume_mutable())
                    ->filter(filt, result_size_hint);
    return ColumnMap::create(assert_cast<const ColumnArray&>(*k_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*v_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*k_arr).get_offsets_ptr());
}

size_t ColumnMap::filter(const Filter& filter) {
    MutableColumnPtr copied_off = offsets_column->clone_empty();
    copied_off->insert_range_from(*offsets_column, 0, offsets_column->size());
    ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
            ->filter(filter);
    ColumnArray::create(values_column->assume_mutable(), copied_off->assume_mutable())
            ->filter(filter);
    return get_offsets().size();
}

ColumnPtr ColumnMap::permute(const Permutation& perm, size_t limit) const {
    // Make a temp column array
    auto k_arr =
            ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
                    ->permute(perm, limit);
    auto v_arr =
            ColumnArray::create(values_column->assume_mutable(), offsets_column->assume_mutable())
                    ->permute(perm, limit);

    return ColumnMap::create(assert_cast<const ColumnArray&>(*k_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*v_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*k_arr).get_offsets_ptr());
}

ColumnPtr ColumnMap::replicate(const Offsets& offsets) const {
    // Make a temp column array for reusing its replicate function
    auto k_arr =
            ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
                    ->replicate(offsets);
    auto v_arr =
            ColumnArray::create(values_column->assume_mutable(), offsets_column->assume_mutable())
                    ->replicate(offsets);
    auto res = ColumnMap::create(assert_cast<const ColumnArray&>(*k_arr).get_data_ptr(),
                                 assert_cast<const ColumnArray&>(*v_arr).get_data_ptr(),
                                 assert_cast<const ColumnArray&>(*k_arr).get_offsets_ptr());
    return res;
}

void ColumnMap::replicate(const uint32_t* indexs, size_t target_size, IColumn& column) const {
    auto& res = reinterpret_cast<ColumnMap&>(column);

    // Make a temp column array for reusing its replicate function
    ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
            ->replicate(indexs, target_size, res.keys_column->assume_mutable_ref());
    ColumnArray::create(values_column->assume_mutable(), offsets_column->assume_mutable())
            ->replicate(indexs, target_size, res.values_column->assume_mutable_ref());
}

MutableColumnPtr ColumnMap::get_shrinked_column() {
    MutableColumns new_columns(2);

    if (keys_column->is_column_string() || keys_column->is_column_array() ||
        keys_column->is_column_map() || keys_column->is_column_struct()) {
        new_columns[0] = keys_column->get_shrinked_column();
    } else {
        new_columns[0] = keys_column->get_ptr();
    }

    if (values_column->is_column_string() || values_column->is_column_array() ||
        values_column->is_column_map() || values_column->is_column_struct()) {
        new_columns[1] = values_column->get_shrinked_column();
    } else {
        new_columns[1] = values_column->get_ptr();
    }

    return ColumnMap::create(new_columns[0]->assume_mutable(), new_columns[1]->assume_mutable(),
                             offsets_column->assume_mutable());
}

void ColumnMap::reserve(size_t n) {
    get_offsets().reserve(n);
    keys_column->reserve(n);
    values_column->reserve(n);
}

void ColumnMap::resize(size_t n) {
    auto last_off = get_offsets().back();
    get_offsets().resize_fill(n, last_off);
}

size_t ColumnMap::byte_size() const {
    return keys_column->byte_size() + values_column->byte_size() + offsets_column->byte_size();
    ;
}

size_t ColumnMap::allocated_bytes() const {
    return keys_column->allocated_bytes() + values_column->allocated_bytes() +
           get_offsets().allocated_bytes();
}

} // namespace doris::vectorized
