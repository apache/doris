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

#include "core/column/column.h"
#include "core/cow.h"
#include "core/data_type/file_schema_descriptor.h"

namespace doris {

class ColumnFile final : public COWHelper<IColumn, ColumnFile> {
private:
    friend class COWHelper<IColumn, ColumnFile>;
    explicit ColumnFile(MutableColumnPtr data);
    WrappedPtr _data;

public:
    using Base = COWHelper<IColumn, ColumnFile>;
    static MutablePtr create(const FileSchemaDescriptor& schema);
    static MutablePtr create_from_jsonb(MutableColumnPtr data);

    std::string get_name() const override { return "File"; }
    void sanity_check() const override { _data->sanity_check(); }
    ColumnPtr convert_column_if_overflow() override;
    size_t size() const override { return _data->size(); }
    bool is_variable_length() const override { return _data->is_variable_length(); }
    bool is_exclusive() const override { return _data->is_exclusive() && IColumn::is_exclusive(); }

    Field operator[](size_t n) const override { return (*_data)[n]; }
    void get(size_t n, Field& res) const override { _data->get(n, res); }
    StringRef get_data_at(size_t n) const override { return _data->get_data_at(n); }

    void insert(const Field& x) override;
    void insert_from(const IColumn& src, size_t n) override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;
    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;
    void insert_many_from(const IColumn& src, size_t position, size_t length) override;
    void insert_default() override;
    void pop_back(size_t n) override;

    MutableColumnPtr clone_resized(size_t size) const override;
    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t serialize_size_at(size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    size_t serialize_impl(char* pos, size_t row) const override;

    int compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const override;
    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data = nullptr) const override;
    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset = 0,
                                const uint8_t* __restrict null_data = nullptr) const override;
    void update_crc32c_batch(uint32_t* __restrict hashes,
                             const uint8_t* __restrict null_map) const override;
    void update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                              const uint8_t* __restrict null_map) const override;

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;
    MutableColumnPtr permute(const Permutation& perm, size_t limit) const override;

    void reserve(size_t n) override { _data->reserve(n); }
    void resize(size_t n) override { _data->resize(n); }
    size_t byte_size() const override { return _data->byte_size(); }
    bool has_enough_capacity(const IColumn& src) const override;
    size_t allocated_bytes() const override { return _data->allocated_bytes(); }
    void for_each_subcolumn(ColumnCallback callback) override { callback(_data); }
    bool structure_equals(const IColumn& rhs) const override;
    void clear() override { _data->clear(); }
    void erase(size_t start, size_t length) override { _data->erase(start, length); }
    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, HybridSorter& sorter,
                         IColumn::Permutation& res) const override;
    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;
    void serialize(StringRef* keys, size_t num_rows) const override;
    void deserialize(StringRef* keys, size_t num_rows) override;
    size_t get_max_row_byte_size() const override;
    void replace_float_special_values() override;
    void shrink_padding_chars() override { _data->shrink_padding_chars(); }

    const IColumn& get_jsonb_column() const { return *_data; }
    IColumn& get_jsonb_column() { return *_data; }
    MutableColumnPtr get_mutable_jsonb_column_ptr() { return _data->get_ptr(); }
    void set_jsonb_column_ptr(MutableColumnPtr data) { _data = WrappedPtr(std::move(data)); }
    Status check_schema(const FileSchemaDescriptor& schema) const;
};

} // namespace doris
