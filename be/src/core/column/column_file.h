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

#pragma once

#include <array>

#include "core/column/column.h"
#include "core/cow.h"

namespace doris {

// Complete physical FILE values: uri, offset, size, content_type, checksum, inline.
// All six children are nullable; parent SQL NULL belongs to an outer ColumnNullable.
// Input-boundary validation is responsible for URI/range/metadata validity.
class ColumnFile final : public COWHelper<IColumn, ColumnFile> {
private:
    friend class COWHelper<IColumn, ColumnFile>;

    ColumnFile();
    explicit ColumnFile(MutableColumns&& columns);
    ColumnFile(const ColumnFile&) = default;

public:
    static constexpr size_t NUM_CHILDREN = 6;
    using FileColumns = std::array<IColumn::WrappedPtr, NUM_CHILDREN>;

    std::string get_name() const override { return "File"; }
    size_t size() const override { return _columns[0]->size(); }
    size_t tuple_size() const { return NUM_CHILDREN; }
    bool is_variable_length() const override { return true; }
    bool is_exclusive() const override {
        return IColumn::is_exclusive() &&
               std::all_of(_columns.begin(), _columns.end(),
                           [](const auto& column) { return column->is_exclusive(); });
    }
    void sanity_check() const override;
    bool structure_equals(const IColumn& rhs) const override;

    const IColumn& get_column(size_t i) const { return *_columns[i]; }
    IColumn& get_column(size_t i) { return *_columns[i]; }
    const FileColumns& get_columns() const { return _columns; }
    Columns get_columns_copy() const { return {_columns.begin(), _columns.end()}; }
    const ColumnPtr& get_column_ptr(size_t i) const { return _columns[i]; }
    ColumnPtr& get_column_ptr(size_t i) { return _columns[i]; }

    MutableColumnPtr clone_resized(size_t size) const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    void insert(const Field& value) override;
    void insert_from(const IColumn& src, size_t n) override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;
    void insert_indices_from(const IColumn& src, const uint32_t* begin,
                             const uint32_t* end) override;
    void insert_many_from(const IColumn& src, size_t position, size_t length) override;
    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override;
    void insert_default() override;
    void insert_many_defaults(size_t length) override;
    void pop_back(size_t n) override;
    void reserve(size_t n) override;
    void resize(size_t n) override;
    void clear() override;
    void erase(size_t start, size_t length) override;
    ColumnPtr filter(const Filter& filter, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;
    Status filter_by_selector(const uint16_t* selector, size_t count,
                              IColumn* destination) const override;
    MutableColumnPtr permute(const Permutation& permutation, size_t limit) const override;
    ColumnPtr convert_column_if_overflow() override;

    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;
    void for_each_subcolumn(ColumnCallback callback) const override;

    StringRef serialize_value_into_arena(size_t n, Arena& arena, const char*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t serialize_size_at(size_t row) const override;
    size_t serialize_impl(char* pos, size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    void serialize(StringRef* keys, size_t num_rows) const override;
    void deserialize(StringRef* keys, size_t num_rows) override;
    size_t get_max_row_byte_size() const override;

    // These hashes cover serialized physical values for internal integrity only.
    // They do not grant FILE SQL equality, hashing, or deduplication semantics.
    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data = nullptr) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type, uint32_t rows,
                                uint32_t offset = 0,
                                const uint8_t* __restrict null_data = nullptr) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;
    void update_crc32c_batch(uint32_t* __restrict hashes,
                             const uint8_t* __restrict null_map) const override;
    void update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                              const uint8_t* __restrict null_map) const override;

    int compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, HybridSorter& sorter,
                         Permutation& res) const override;
    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, Permutation& permutation,
                     EqualRange& range, bool last_column) const override;

protected:
    void mutate_subcolumns() override;

private:
    FileColumns _columns;
};

} // namespace doris
