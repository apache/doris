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

#include <cstddef>

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_view.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class ColumnVarbinary final : public COWHelper<IColumn, ColumnVarbinary> {
private:
    using Self = ColumnVarbinary;
    friend class COWHelper<IColumn, ColumnVarbinary>;

public:
    using Container = PaddedPODArray<doris::StringView>;
    ColumnVarbinary() = default;
    ColumnVarbinary(const size_t n) : _data(n) {}

private:
    ColumnVarbinary(const ColumnVarbinary& src)
            : _data(src._data.begin(), src._data.end()),
              _buffer(src._buffer.begin(), src._buffer.end()) {}

public:
    std::string get_name() const override { return "ColumnVarbinary"; }

    size_t size() const override { return _data.size(); }

    const Container& get_data() const { return _data; }

    Container& get_data() { return _data; }

    // Notice: after this buffer maybe have some useless data
    void resize(size_t n) override { _data.resize(n); }

    void clear() override {
        _data.clear();
        _buffer.clear();
    }

    Field operator[](size_t n) const override {
        return Field::create_field<TYPE_VARBINARY>(_data[n]);
    }

    void get(size_t n, Field& res) const override {
        res = Field::create_field<TYPE_VARBINARY>(_data[n]);
    }

    StringRef get_data_at(size_t n) const override { return _data[n].to_string_ref(); }

    void insert(const Field& x) override {
        auto value = vectorized::get<const doris::StringView&>(x);
        insert_data(value.data(), value.size());
    }

    void insert_from(const IColumn& src, size_t n) override {
        const auto& src_col = assert_cast<const ColumnVarbinary&>(src);
        auto value = src_col.get_data_at(n);
        insert_data(value.data, value.size);
    }

    void insert_data(const char* pos, size_t length) override {
        if (length <= doris::StringView::kInlineSize) {
            insert_inline_data(pos, length);
        } else {
            insert_to_buffer(pos, length);
        }
    }

    void insert_inline_data(const char* pos, size_t length) {
        DCHECK(length <= doris::StringView::kInlineSize);
        _data.push_back(doris::StringView(pos, cast_set<uint32_t>(length)));
    }

    void insert_to_buffer(const char* pos, size_t length) {
        auto old_size = _buffer.size();
        _buffer.resize(old_size + length);
        auto* dst = _buffer.data() + old_size;
        memcpy(dst, pos, length);
        _data.push_back(doris::StringView(dst, cast_set<uint32_t>(length)));
    }

    void insert_default() override { _data.push_back(doris::StringView()); }

    void pop_back(size_t n) override { resize(size() - n); }

    StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                         char const*& begin) const override {
        char* pos = arena.alloc_continue(serialize_size_at(n), begin);
        return {pos, serialize_impl(pos, n)};
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        return pos + deserialize_impl(pos);
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    MutableColumnPtr clone_resized(size_t size) const override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    size_t allocated_bytes() const override {
        return _data.allocated_bytes() + _buffer.allocated_bytes();
    }

    size_t byte_size() const override {
        size_t bytes = _data.size() * sizeof(doris::StringView);
        bytes += _buffer.size() * sizeof(_buffer[0]);
        return bytes;
    }

    bool has_enough_capacity(const IColumn& src) const override {
        const auto& src_col = assert_cast<const ColumnVarbinary&>(src);
        return _data.capacity() - _data.size() > src_col.size();
    }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;

    size_t filter(const IColumn::Filter& filter) override;

    MutableColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override;

    size_t deserialize_impl(const char* pos) override {
        const auto value_size = unaligned_load<uint32_t>(pos);
        pos += sizeof(value_size);
        insert_data(pos, value_size);
        return value_size + sizeof(value_size);
    }

    size_t serialize_impl(char* pos, const size_t row) const override {
        const auto* value_data = _data[row].data();
        uint32_t value_size = _data[row].size();
        memcpy_fixed<uint32_t>(pos, (char*)(&value_size));
        memcpy(pos + sizeof(uint32_t), value_data, value_size);
        return value_size + sizeof(uint32_t);
    }

    size_t serialize_size_at(size_t row) const override { return _data[row].size(); }

    MutableColumnPtr convert_to_string_column() const;

protected:
    Container _data;
    PaddedPODArray<UInt8> _buffer;
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized
