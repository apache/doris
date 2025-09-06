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

#include "vec/columns/columns_common.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

ColumnPtr squash_const(const ColumnPtr& col) {
    ColumnPtr res = col;
    while (const auto* c = typeid_cast<const ColumnConst*>(res.get())) {
        res = c->get_data_column_ptr();
    }
    return res;
}

ColumnConst::ColumnConst(const ColumnPtr& data_, size_t s_, bool create_with_empty,
                         bool need_squash)
        : data(need_squash ? squash_const(data_) : data_), s(s_) {
    if (data->empty() != create_with_empty) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Incorrect size of nested column in constructor of ColumnConst: {}, "
                               "create_with_empty: {}.",
                               data->size(), create_with_empty);
    }

    if (data->size() != 1 && !create_with_empty) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "Incorrect size of nested column in constructor of ColumnConst: {}, must be 1.",
                data->size());
    }
}

ColumnPtr ColumnConst::convert_to_full_column() const {
    // clone_resized(0) will make ColumnVariant loss type information
    // so we use clone_resized(1) as possible workaround
    auto result = data->clone_resized(std::min(1UL, s));
    if (s > 1) {
        result->insert_many_from(*data, 0, s - 1);
    }
    return result;
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

MutableColumnPtr ColumnConst::permute(const Permutation& perm, size_t limit) const {
    if (limit == 0) {
        limit = s;
    } else {
        limit = std::min(s, limit);
    }

    if (perm.size() < limit) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of permutation ({}) is less than required ({})", perm.size(),
                               limit);
    }

    return ColumnConst::create(data, limit);
}

void ColumnConst::get_permutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/,
                                  Permutation& res) const {
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }
}

std::pair<ColumnPtr, size_t> check_column_const_set_readability(const IColumn& column,
                                                                size_t row_num) noexcept {
    std::pair<ColumnPtr, size_t> result;
    if (is_column_const(column)) {
        result.first = assert_cast<const ColumnConst&>(column).get_data_column_ptr();
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
                std::cref(assert_cast<const ColumnConst&>(*ptr).get_data_column_ptr()), true);
    }
    return std::make_pair(std::cref(ptr), false);
}

void default_preprocess_parameter_columns(ColumnPtr* columns, const bool* col_const,
                                          const std::initializer_list<size_t>& parameters,
                                          Block& block, const ColumnNumbers& arg_indexes) {
    if (std::all_of(parameters.begin(), parameters.end(),
                    [&](size_t const_index) -> bool { return col_const[const_index]; })) {
        // only need to avoid expanding when all parameters are const
        for (auto index : parameters) {
            columns[index] = assert_cast<const ColumnConst&>(
                                     *block.get_by_position(arg_indexes[index]).column)
                                     .get_data_column_ptr();
        }
    } else {
        // no need to avoid expanding for this rare situation
        for (auto index : parameters) {
            if (col_const[index]) {
                columns[index] = assert_cast<const ColumnConst&>(
                                         *block.get_by_position(arg_indexes[index]).column)
                                         .convert_to_full_column();
            } else {
                columns[index] = block.get_by_position(arg_indexes[index]).column;
            }
        }
    }
}
} // namespace doris::vectorized
