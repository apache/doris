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

#include <glog/logging.h>
#include <pdqsort.h>

#include <cstddef>

#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_view.h"

namespace doris {

// Stores the raw OGC WKB payload for GEOMETRY and GEOGRAPHY. The primitive type is
// part of the column identity so spatial values cannot be substituted for VARBINARY.
class ColumnSpatial final : public COWHelper<IColumn, ColumnSpatial> {
private:
    using Self = ColumnSpatial;
    friend class COWHelper<IColumn, ColumnSpatial>;
    template <bool positive>
    struct less;

public:
    using Container = PaddedPODArray<doris::StringView>;

private:
    explicit ColumnSpatial(PrimitiveType primitive_type) : _primitive_type(primitive_type) {
        DCHECK(primitive_type == TYPE_GEOMETRY || primitive_type == TYPE_GEOGRAPHY);
    }
    ColumnSpatial(const ColumnSpatial& src) : _primitive_type(src._primitive_type) {
        _data.reserve(src._data.size());
        for (const auto& value : src._data) {
            insert_data(value.data(), value.size());
        }
    }

public:
    std::string get_name() const override { return "ColumnSpatial"; }
    PrimitiveType get_primitive_type() const { return _primitive_type; }
    size_t size() const override { return _data.size(); }
    const Container& get_data() const { return _data; }
    void resize(size_t n) override { _data.resize(n); }
    void clear() override {
        _data.clear();
        _arena.clear();
    }
    Field operator[](size_t n) const override {
        return Field::create_field<TYPE_VARBINARY>(_data[n]);
    }
    void get(size_t n, Field& res) const override {
        res = Field::create_field<TYPE_VARBINARY>(_data[n]);
    }
    StringRef get_data_at(size_t n) const override { return _data[n].to_string_ref(); }
    void insert(const Field& x) override {
        const auto& value = x.get<TYPE_VARBINARY>();
        insert_data(value.data(), value.size());
    }
    void insert_from(const IColumn& src, size_t n) override {
        const auto& spatial = assert_cast<const ColumnSpatial&>(src);
        DCHECK_EQ(_primitive_type, spatial._primitive_type);
        const auto value = spatial.get_data_at(n);
        insert_data(value.data, value.size);
    }
    void insert_data(const char* pos, size_t length) override;
    void insert_default() override { _data.push_back(doris::StringView()); }
    int compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, HybridSorter& sorter,
                         IColumn::Permutation& res) const override;
    size_t get_max_row_byte_size() const override;
    void deserialize_vec(StringRef* keys, size_t num_rows) override;
    void serialize_vec(StringRef* keys, size_t num_rows) const override;
    void pop_back(size_t n) override { resize(size() - n); }
    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    MutableColumnPtr clone_resized(size_t size) const override;
    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;
    size_t allocated_bytes() const override { return _data.allocated_bytes() + _arena.size(); }
    size_t byte_size() const override {
        return _data.size() * sizeof(doris::StringView) + _arena.used_size();
    }
    bool has_enough_capacity(const IColumn& src) const override;
    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const IColumn::Filter& filter) override;
    MutableColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;
    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override;
    void insert_many_strings(const StringRef* strings, size_t num) override;
    void insert_many_strings_overflow(const StringRef* strings, size_t num,
                                      size_t max_length) override;
    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

private:
    size_t deserialize_impl(const char* pos) override;
    size_t serialize_impl(char* pos, size_t row) const override;
    size_t serialize_size_at(size_t row) const override;
    Container _data;
    Arena _arena;
    const PrimitiveType _primitive_type;
};

} // namespace doris
