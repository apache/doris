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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnConst.cpp
// and modified by Doris

#include "vec/columns/column_const.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <utility>

#include "runtime/raw_value.h"
#include "util/hash_util.hpp"
#include "vec/columns/columns_common.h"
#include "vec/common/sip_hash.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris::vectorized {

ColumnConst::ColumnConst(const ColumnPtr& data_, size_t s_) : data(data_), s(s_) {
    /// Squash Const of Const.
    while (const ColumnConst* const_data = typeid_cast<const ColumnConst*>(data.get())) {
        data = const_data->get_data_column_ptr();
    }

    if (data->size() != 1) {
        LOG(FATAL) << fmt::format(
                "Incorrect size of nested column in constructor of ColumnConst: {}, must be 1.",
                data->size());
    }
}

ColumnPtr ColumnConst::convert_to_full_column() const {
    return data->replicate(Offsets(1, s));
}

ColumnPtr ColumnConst::remove_low_cardinality() const {
    return ColumnConst::create(data->convert_to_full_column_if_low_cardinality(), s);
}

ColumnPtr ColumnConst::filter(const Filter& filt, ssize_t /*result_size_hint*/) const {
    column_match_filter_size(s, filt.size());

    return ColumnConst::create(data, count_bytes_in_filter(filt));
}

size_t ColumnConst::filter(const Filter& filter) {
    column_match_filter_size(s, filter.size());

    const auto result_size = count_bytes_in_filter(filter);
    resize(result_size);
    return result_size;
}

ColumnPtr ColumnConst::replicate(const Offsets& offsets) const {
    column_match_offsets_size(s, offsets.size());

    size_t replicated_size = 0 == s ? 0 : offsets.back();
    return ColumnConst::create(data, replicated_size);
}

void ColumnConst::replicate(const uint32_t* counts, size_t target_size, IColumn& column) const {
    if (s == 0) return;
    auto& res = reinterpret_cast<ColumnConst&>(column);
    res.s = target_size;
}

ColumnPtr ColumnConst::permute(const Permutation& perm, size_t limit) const {
    if (limit == 0) {
        limit = s;
    } else {
        limit = std::min(s, limit);
    }

    if (perm.size() < limit) {
        LOG(FATAL) << fmt::format("Size of permutation ({}) is less than required ({})",
                                  perm.size(), limit);
    }

    return ColumnConst::create(data, limit);
}

void ColumnConst::update_hashes_with_value(std::vector<SipHash>& hashes,
                                           const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    DCHECK(hashes.size() == size());
    auto real_data = data->get_data_at(0);
    if (real_data.data == nullptr) {
        for (auto& hash : hashes) {
            hash.update(0);
        }
    } else {
        for (auto& hash : hashes) {
            hash.update(real_data.data, real_data.size);
        }
    }
}

void ColumnConst::update_crcs_with_value(uint32_t* __restrict hashes, doris::PrimitiveType type,
                                         uint32_t rows, uint32_t offset,
                                         const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    DCHECK(rows == size());
    auto real_data = data->get_data_at(0);
    if (real_data.data == nullptr) {
        for (int i = 0; i < rows; ++i) {
            hashes[i] = HashUtil::zlib_crc_hash_null(hashes[i]);
        }
    } else {
        for (int i = 0; i < rows; ++i) {
            hashes[i] = RawValue::zlib_crc32(real_data.data, real_data.size, type, hashes[i]);
        }
    }
}

void ColumnConst::update_hashes_with_value(uint64_t* __restrict hashes,
                                           const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto real_data = data->get_data_at(0);
    auto real_size = size();
    if (real_data.data == nullptr) {
        for (int i = 0; i < real_size; ++i) {
            hashes[i] = HashUtil::xxHash64NullWithSeed(hashes[i]);
        }
    } else {
        for (int i = 0; i < real_size; ++i) {
            hashes[i] = HashUtil::xxHash64WithSeed(real_data.data, real_data.size, hashes[i]);
        }
    }
}

MutableColumns ColumnConst::scatter(ColumnIndex num_columns, const Selector& selector) const {
    if (s != selector.size()) {
        LOG(FATAL) << fmt::format("Size of selector ({}) doesn't match size of column ({})",
                                  selector.size(), s);
    }

    std::vector<size_t> counts = count_columns_size_in_selector(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        res[i] = clone_resized(counts[i]);
    }

    return res;
}

void ColumnConst::get_permutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/,
                                  Permutation& res) const {
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }
}

void ColumnConst::get_indices_of_non_default_rows(Offsets64& indices, size_t from,
                                                  size_t limit) const {
    if (!data->is_default_at(0)) {
        size_t to = limit && from + limit < size() ? from + limit : size();
        indices.reserve(indices.size() + to - from);
        for (size_t i = from; i < to; ++i) {
            indices.push_back(i);
        }
    }
}

ColumnPtr ColumnConst::index(const IColumn& indexes, size_t limit) const {
    if (limit == 0) {
        limit = indexes.size();
    }
    if (indexes.size() < limit) {
        LOG(FATAL) << "Size of indexes  is less than required " << std::to_string(limit);
    }
    return ColumnConst::create(data, limit);
}

std::pair<ColumnPtr, size_t> check_column_const_set_readability(const IColumn& column,
                                                                const size_t row_num) noexcept {
    std::pair<ColumnPtr, size_t> result;
    if (is_column_const(column)) {
        result.first = static_cast<const ColumnConst&>(column).get_data_column_ptr();
        result.second = 0;
    } else {
        result.first = column.get_ptr();
        result.second = row_num;
    }
    return result;
}

std::pair<const ColumnPtr&, bool> unpack_if_const(const ColumnPtr& ptr) noexcept {
    if (is_column_const(*ptr)) {
        return std::make_pair(
                std::cref(static_cast<const ColumnConst&>(*ptr).get_data_column_ptr()), true);
    }
    return std::make_pair(std::cref(ptr), false);
}

void default_preprocess_parameter_columns(ColumnPtr* columns, const bool* col_const,
                                          const std::initializer_list<size_t>& parameters,
                                          Block& block, const ColumnNumbers& arg_indexes) noexcept {
    if (std::all_of(parameters.begin(), parameters.end(),
                    [&](size_t const_index) -> bool { return col_const[const_index]; })) {
        // only need to avoid expanding when all parameters are const
        for (auto index : parameters) {
            columns[index] = static_cast<const ColumnConst&>(
                                     *block.get_by_position(arg_indexes[index]).column)
                                     .get_data_column_ptr();
        }
    } else {
        // no need to avoid expanding for this rare situation
        for (auto index : parameters) {
            if (col_const[index]) {
                columns[index] = static_cast<const ColumnConst&>(
                                         *block.get_by_position(arg_indexes[index]).column)
                                         .convert_to_full_column();
            } else {
                columns[index] = block.get_by_position(arg_indexes[index]).column;
            }
        }
    }
}
} // namespace doris::vectorized
