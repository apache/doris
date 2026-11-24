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
// Query-wide FE source IDs and native row IDs follow value copies and internal serialization.
// The query owns all source definitions; columns never hold storage configuration or SDK handles.
class ColumnFile final : public COWHelper<IColumn, ColumnFile> {
private:
    friend class COWHelper<IColumn, ColumnFile>;

    ColumnFile();
    explicit ColumnFile(MutableColumns&& columns, MutableColumnPtr blob_source_ids = nullptr,
                        MutableColumnPtr blob_row_ids = nullptr);
    ColumnFile(const ColumnFile&) = default;

public:
    static constexpr size_t NUM_CHILDREN = 6;
    using FileColumns = std::array<IColumn::WrappedPtr, NUM_CHILDREN>;

    std::string get_name() const override { return "File"; }
    size_t size() const override { return _columns[0]->size(); }
    size_t tuple_size() const { return NUM_CHILDREN; }
    bool is_variable_length() const override { return true; }
    bool is_exclusive() const override {
        return IColumn::is_exclusive() && (!_blob_source_ids || _blob_source_ids->is_exclusive()) &&
               (!_blob_row_ids || _blob_row_ids->is_exclusive()) &&
               std::all_of(_columns.begin(), _columns.end(),
                           [](const auto& column) { return column->is_exclusive(); });
    }
    void sanity_check() const override;
    const IColumn& get_column(size_t i) const { return *_columns[i]; }
    IColumn& get_column(size_t i) { return *_columns[i]; }
    const FileColumns& get_columns() const { return _columns; }
    Columns get_columns_copy() const { return {_columns.begin(), _columns.end()}; }
    const ColumnPtr& get_column_ptr(size_t i) const { return _columns[i]; }
    ColumnPtr& get_column_ptr(size_t i) { return _columns[i]; }
    uint32_t blob_source_id_at(size_t row) const;
    uint64_t blob_row_id_at(size_t row) const;
    // Null pointers represent zero IDs for every row. Both columns are allocated together.
    const IColumn* blob_source_ids() const { return _blob_source_ids.get(); }
    const IColumn* blob_row_ids() const { return _blob_row_ids.get(); }
    void set_blob_ids(MutableColumnPtr blob_source_ids, MutableColumnPtr blob_row_ids);
    // Pad missing Blob IDs with zeros to match the value row count, preserving existing IDs.
    // Used by producers that populate the public children directly (e.g. STRUCT -> FILE).
    void fill_missing_blob_ids();
    // True if any row has a nonzero Blob source ID.
    bool has_blob_references() const;
    // Release both ID columns, representing zero IDs without allocating storage.
    void reset_blob_ids();

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
    void for_each_subcolumn(ColumnCallback callback) override;

    StringRef serialize_value_into_arena(size_t n, Arena& arena, const char*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t serialize_size_at(size_t row) const override;
    size_t serialize_impl(char* pos, size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    void serialize_vec(StringRef* keys, size_t num_rows) const override;
    void deserialize_vec(StringRef* keys, size_t num_rows) override;
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

private:
    FileColumns _columns;
    void _ensure_blob_ids(size_t previous_rows);
    void _append_blob_reference(uint32_t source_id, uint64_t row_id, size_t previous_rows,
                                size_t length = 1);
    void _append_blob_references(const ColumnFile& source, size_t start, size_t length);
    // Either both pointers are null (all IDs are zero), or both columns are row-aligned.
    // IDs into the current query's source dictionary. Zero means unbound.
    IColumn::WrappedPtr _blob_source_ids;
    // Native row IDs within each row's source, rather than positions in this column.
    IColumn::WrappedPtr _blob_row_ids;
};

} // namespace doris
