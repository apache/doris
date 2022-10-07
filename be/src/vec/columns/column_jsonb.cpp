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

#include "vec/columns/column_jsonb.h"

#include "util/jsonb_parser.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/unaligned.h"

namespace doris::vectorized {

MutableColumnPtr ColumnJsonb::clone_resized(size_t to_size) const {
    auto res = ColumnJsonb::create();
    res->column_string = column_string->clone_resized(to_size);
    return res;
}

void ColumnJsonb::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) return;

    const ColumnJsonb& src_concrete = reinterpret_cast<const ColumnJsonb&>(src);

    column_string->insert_range_from(*src_concrete.column_string, start, length);
}

void ColumnJsonb::insert_indices_from(const IColumn& src, const int* indices_begin,
                                      const int* indices_end) {
    const ColumnJsonb& src_concrete = reinterpret_cast<const ColumnJsonb&>(src);
    column_string->insert_indices_from(*src_concrete.column_string, indices_begin, indices_end);
}

ColumnPtr ColumnJsonb::filter(const Filter& filt, ssize_t result_size_hint) const {
    if (size() == 0) return ColumnJsonb::create();

    auto res = ColumnJsonb::create();
    res->column_string = column_string->filter(filt, result_size_hint);

    return res;
}

ColumnPtr ColumnJsonb::permute(const Permutation& perm, size_t limit) const {
    auto res = ColumnJsonb::create();
    res->column_string = column_string->permute(perm, limit);

    return res;
}

StringRef ColumnJsonb::serialize_value_into_arena(size_t n, Arena& arena,
                                                  char const*& begin) const {
    return column_string->serialize_value_into_arena(n, arena, begin);
}

const char* ColumnJsonb::deserialize_and_insert_from_arena(const char* pos) {
    return column_string->deserialize_and_insert_from_arena(pos);
}

size_t ColumnJsonb::get_max_row_byte_size() const {
    return column_string->get_max_row_byte_size();
}

void ColumnJsonb::serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                                size_t max_row_byte_size) const {
    column_string->serialize_vec(keys, num_rows, max_row_byte_size);
}

void ColumnJsonb::serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                              const uint8_t* null_map,
                                              size_t max_row_byte_size) const {
    column_string->serialize_vec_with_null_map(keys, num_rows, null_map, max_row_byte_size);
}

template <typename Type>
ColumnPtr ColumnJsonb::index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const {
    if (limit == 0) return ColumnJsonb::create();

    auto res = ColumnJsonb::create();
    res->column_string = get_column_string()->index_impl<Type>(indexes, limit);

    return res;
}

template <bool positive>
struct ColumnJsonb::less {
    const ColumnJsonb& parent;
    explicit less(const ColumnJsonb& parent_) : parent(parent_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        LOG(FATAL) << "compare jsonb not supported";
        return 0;
    }
};

void ColumnJsonb::get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                  Permutation& res) const {
    column_string->get_permutation(reverse, limit, nan_direction_hint, res);
    LOG(FATAL) << "compare jsonb not supported";
}

ColumnPtr ColumnJsonb::replicate(const Offsets& replicate_offsets) const {
    size_t col_size = size();
    if (col_size != replicate_offsets.size()) {
        LOG(FATAL) << "Size of offsets doesn't match size of column.";
    }

    auto res = ColumnJsonb::create();

    if (0 == col_size) return res;

    res->column_string = column_string->replicate(replicate_offsets);

    return res;
}

void ColumnJsonb::replicate(const uint32_t* counts, size_t target_size, IColumn& column,
                            size_t begin, int count_sz) const {
    size_t col_size = count_sz < 0 ? size() : count_sz;
    if (0 == col_size) return;

    auto& res = reinterpret_cast<ColumnJsonb&>(column);

    res.column_string->replicate(counts, target_size, *res.column_string);
}

void ColumnJsonb::reserve(size_t n) {
    column_string->reserve(n);
}

MutableColumnPtr ColumnJsonb::get_shrinked_column() {
    auto shrinked_column = ColumnJsonb::create();
    shrinked_column->column_string = column_string->get_shrinked_column();
    return shrinked_column;
}

void ColumnJsonb::resize(size_t n) {
    column_string->resize(n);
}

void ColumnJsonb::get_extremes(Field& min, Field& max) const {
    size_t col_size = size();

    if (col_size == 0) return;

    size_t min_idx = 0;
    size_t max_idx = 0;

    less<true> less_op(*this);

    for (size_t i = 1; i < col_size; ++i) {
        if (less_op(i, min_idx))
            min_idx = i;
        else if (less_op(max_idx, i))
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}

void ColumnJsonb::protect() {
    column_string->protect();
}

} // namespace doris::vectorized
