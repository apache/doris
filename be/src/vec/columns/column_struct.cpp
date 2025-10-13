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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnStruct.cpp
// and modified by Doris

#include "vec/columns/column_struct.h"

#include <functional>

#include "pdqsort.h"
#include "runtime/primitive_type.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/sort_block.h"

class SipHash;
namespace doris {
namespace vectorized {
class Arena;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

std::string ColumnStruct::get_name() const {
    std::stringstream res;
    res << "Struct(";
    bool is_first = true;
    for (const auto& column : columns) {
        if (!is_first) {
            res << ", ";
        }
        is_first = false;
        res << column->get_name();
    }
    res << ")";
    return res.str();
}

ColumnStruct::ColumnStruct(MutableColumns&& mutable_columns) {
    columns.reserve(mutable_columns.size());
    for (auto& column : mutable_columns) {
        if (is_column_const(*column)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "ColumnStruct cannot have ColumnConst as its element");
            __builtin_unreachable();
        }
        columns.push_back(std::move(column));
    }
}

ColumnStruct::MutablePtr ColumnStruct::create(const Columns& columns) {
    for (const auto& column : columns) {
        if (is_column_const(*column)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "ColumnStruct cannot have ColumnConst as its element");
            __builtin_unreachable();
        }
    }
    auto column_struct = ColumnStruct::create(MutableColumns());
    column_struct->columns.assign(columns.begin(), columns.end());
    return column_struct;
}

ColumnStruct::MutablePtr ColumnStruct::create(const TupleColumns& tuple_columns) {
    for (const auto& column : tuple_columns) {
        if (is_column_const(*column)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "ColumnStruct cannot have ColumnConst as its element");
            __builtin_unreachable();
        }
    }
    auto column_struct = ColumnStruct::create(MutableColumns());
    column_struct->columns = tuple_columns;
    return column_struct;
}

MutableColumnPtr ColumnStruct::clone_resized(size_t new_size) const {
    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->clone_resized(new_size);
    }
    return ColumnStruct::create(std::move(new_columns));
}

Field ColumnStruct::operator[](size_t n) const {
    Field res;
    get(n, res);
    return res;
}

void ColumnStruct::get(size_t n, Field& res) const {
    const size_t tuple_size = columns.size();

    res = Field::create_field<TYPE_STRUCT>(Tuple());
    Tuple& res_tuple = res.get<Tuple&>();
    res_tuple.reserve(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        res_tuple.push_back((*columns[i])[n]);
    }
}

void ColumnStruct::insert(const Field& x) {
    DCHECK_EQ(x.get_type(), PrimitiveType::TYPE_STRUCT);
    const auto& tuple = x.get<const Tuple&>();
    const size_t tuple_size = columns.size();
    if (tuple.size() != tuple_size) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Cannot insert value of different size into tuple. field tuple size "
                               "{}, columns size {}",
                               tuple.size(), tuple_size);
    }

    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert(tuple[i]);
    }
}

void ColumnStruct::insert_from(const IColumn& src_, size_t n) {
    const ColumnStruct& src = assert_cast<const ColumnStruct&>(src_);

    const size_t tuple_size = columns.size();
    if (src.columns.size() != tuple_size) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Cannot insert value of different size into tuple.");
        __builtin_unreachable();
    }

    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert_from(*src.columns[i], n);
    }
}

void ColumnStruct::insert_default() {
    for (auto& column : columns) {
        column->insert_default();
    }
}

void ColumnStruct::pop_back(size_t n) {
    for (auto& column : columns) {
        column->pop_back(n);
    }
}

StringRef ColumnStruct::serialize_value_into_arena(size_t n, Arena& arena,
                                                   char const*& begin) const {
    char* pos = arena.alloc_continue(serialize_size_at(n), begin);
    return {pos, serialize_impl(pos, n)};
}

size_t ColumnStruct::serialize_size_at(size_t row) const {
    size_t sz = 0;
    for (const auto& column : columns) {
        sz += column->serialize_size_at(row);
    }

    return sz;
}

size_t ColumnStruct::deserialize_impl(const char* pos) {
    size_t sz = 0;
    for (auto& column : columns) {
        sz += column->deserialize_impl(pos + sz);
    }
    return sz;
}

size_t ColumnStruct::serialize_impl(char* pos, const size_t row) const {
    size_t sz = 0;
    for (const auto& column : columns) {
        sz += column->serialize_impl(pos + sz, row);
    }

    DCHECK_EQ(sz, serialize_size_at(row));
    return sz;
}

const char* ColumnStruct::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

int ColumnStruct::compare_at(size_t n, size_t m, const IColumn& rhs_,
                             int nan_direction_hint) const {
    const ColumnStruct& rhs = assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(rhs_);

    const size_t lhs_tuple_size = columns.size();
    const size_t rhs_tuple_size = rhs.tuple_size();
    const size_t min_size = std::min(lhs_tuple_size, rhs_tuple_size);
    for (size_t i = 0; i < min_size; ++i) {
        if (int res = columns[i]->compare_at(n, m, *rhs.columns[i], nan_direction_hint); res) {
            return res;
        }
    }
    return lhs_tuple_size > rhs_tuple_size ? 1 : (lhs_tuple_size < rhs_tuple_size ? -1 : 0);
}

void ColumnStruct::update_hash_with_value(size_t n, SipHash& hash) const {
    for (const auto& column : columns) {
        column->update_hash_with_value(n, hash);
    }
}

void ColumnStruct::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                            const uint8_t* __restrict null_data) const {
    for (const auto& column : columns) {
        column->update_xxHash_with_value(start, end, hash, nullptr);
    }
}

void ColumnStruct::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                         const uint8_t* __restrict null_data) const {
    for (const auto& column : columns) {
        column->update_crc_with_value(start, end, hash, nullptr);
    }
}

void ColumnStruct::update_hashes_with_value(uint64_t* __restrict hashes,
                                            const uint8_t* __restrict null_data) const {
    for (const auto& column : columns) {
        column->update_hashes_with_value(hashes, null_data);
    }
}

void ColumnStruct::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type,
                                          uint32_t rows, uint32_t offset,
                                          const uint8_t* __restrict null_data) const {
    auto s = size();
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

void ColumnStruct::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                       const uint32_t* indices_end) {
    const auto& src_concrete = assert_cast<const ColumnStruct&>(src);
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i]->insert_indices_from(src_concrete.get_column(i), indices_begin, indices_end);
    }
}

void ColumnStruct::insert_many_from(const IColumn& src, size_t position, size_t length) {
    const auto& src_concrete = assert_cast<const ColumnStruct&>(src);
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i]->insert_many_from(src_concrete.get_column(i), position, length);
    }
}

void ColumnStruct::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert_range_from(
                *assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(src).columns[i],
                start, length);
    }
}

void ColumnStruct::insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                                     size_t length) {
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert_range_from_ignore_overflow(
                *assert_cast<const ColumnStruct&, TypeCheckOnRelease::DISABLE>(src).columns[i],
                start, length);
    }
}

ColumnPtr ColumnStruct::filter(const Filter& filt, ssize_t result_size_hint) const {
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->filter(filt, result_size_hint);
    }
    return ColumnStruct::create(new_columns);
}

size_t ColumnStruct::filter(const Filter& filter) {
    const size_t tuple_size = columns.size();

    size_t result_size = 0;
    for (size_t i = 0; i < tuple_size; ++i) {
        const auto this_result_size = columns[i]->filter(filter);
        CHECK(result_size == 0 || result_size == this_result_size);
        result_size = this_result_size;
    }
    return result_size;
}

MutableColumnPtr ColumnStruct::permute(const Permutation& perm, size_t limit) const {
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->permute(perm, limit);
    }

    return ColumnStruct::create(new_columns);
}

void ColumnStruct::shrink_padding_chars() {
    for (auto& column : columns) {
        column->shrink_padding_chars();
    }
}

void ColumnStruct::reserve(size_t n) {
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i) {
        get_column(i).reserve(n);
    }
}

//please check you real need size in data column, When it mixes various data types， eg: string column with int column
void ColumnStruct::resize(size_t n) {
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i) {
        get_column(i).resize(n);
    }
}

size_t ColumnStruct::byte_size() const {
    size_t res = 0;
    for (const auto& column : columns) {
        res += column->byte_size();
    }
    return res;
}

size_t ColumnStruct::allocated_bytes() const {
    size_t res = 0;
    for (const auto& column : columns) {
        res += column->allocated_bytes();
    }
    return res;
}

bool ColumnStruct::has_enough_capacity(const IColumn& src) const {
    const auto& src_concrete = assert_cast<const ColumnStruct&>(src);
    for (size_t i = 0; i < columns.size(); ++i) {
        if (!columns[i]->has_enough_capacity(*src_concrete.columns[i])) {
            return false;
        }
    }
    return true;
}

void ColumnStruct::for_each_subcolumn(ColumnCallback callback) {
    for (auto& column : columns) {
        callback(column);
    }
}

bool ColumnStruct::structure_equals(const IColumn& rhs) const {
    if (const auto* rhs_tuple = typeid_cast<const ColumnStruct*>(&rhs)) {
        const size_t tuple_size = columns.size();
        if (tuple_size != rhs_tuple->columns.size()) {
            return false;
        }

        for (size_t i = 0; i < tuple_size; ++i) {
            if (!columns[i]->structure_equals(*rhs_tuple->columns[i])) {
                return false;
            }
        }
        return true;
    } else {
        return false;
    }
}

template <bool positive>
struct ColumnStruct::less {
    const ColumnStruct& parent;
    const int nan_direction_hint;
    explicit less(const ColumnStruct& parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        int res = 0;
        for (auto& col : parent.get_columns()) {
            if (res = col->compare_at(lhs, rhs, *col.get(), nan_direction_hint); res) {
                // if res != 0 , here is something different ,just return
                break;
            }
        }
        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnStruct::get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                   IColumn::Permutation& res) const {
    size_t s = size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }

    if (reverse) {
        pdqsort(res.begin(), res.end(), ColumnStruct::less<false>(*this, nan_direction_hint));
    } else {
        pdqsort(res.begin(), res.end(), ColumnStruct::less<true>(*this, nan_direction_hint));
    }
}

void ColumnStruct::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                               IColumn::Permutation& perms, EqualRange& range,
                               bool last_column) const {
    sorter->sort_column(static_cast<const ColumnStruct&>(*this), flags, perms, range, last_column);
}

void ColumnStruct::serialize_vec(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        // Used in hash_map_context.h, this address is allocated via Arena,
        // but passed through StringRef, so using const_cast is acceptable.
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

void ColumnStruct::deserialize_vec(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

size_t ColumnStruct::get_max_row_byte_size() const {
    size_t max_row_byte_sz = 0;
    for (const auto& col : columns) {
        max_row_byte_sz += col->get_max_row_byte_size();
    }
    return max_row_byte_sz;
}

void ColumnStruct::replace_float_special_values() {
    for (auto& col : columns) {
        col->replace_float_special_values();
    }
}

} // namespace doris::vectorized
