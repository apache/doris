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

#include "vec/columns/column.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"

namespace doris::vectorized {

class ColumnFixedLengthObject final : public COWHelper<IColumn, ColumnFixedLengthObject> {
private:
    using Self = ColumnFixedLengthObject;
    friend class COWHelper<IColumn, ColumnFixedLengthObject>;
    friend class OlapBlockDataConvertor;

public:
    using Container = PaddedPODArray<uint8_t>;

private:
    ColumnFixedLengthObject() = delete;
    ColumnFixedLengthObject(const size_t _item_size_) : _item_size(_item_size_), _item_count(0) {}
    ColumnFixedLengthObject(const ColumnFixedLengthObject& src)
            : _item_size(src._item_size),
              _item_count(src._item_count),
              _data(src._data.begin(), src._data.end()) {}

public:
    const char* get_family_name() const override { return "ColumnFixedLengthObject"; }

    size_t size() const override { return _item_count; }

    const Container& get_data() const { return _data; }

    Container& get_data() { return _data; }

    void resize(size_t n) override {
        DCHECK(_item_size > 0) << "_item_size should be greater than 0";
        _data.resize(n * _item_size);
        _item_count = n;
    }

    MutableColumnPtr clone_resized(size_t size) const override {
        auto res = this->create(_item_size);

        if (size > 0) {
            auto& new_col = assert_cast<Self&>(*res);
            new_col.resize(size);
            auto* new_data = new_col._data.data();

            size_t count = std::min(this->size(), size);
            memcpy(new_data, _data.data(), count * _item_size);

            if (size > count) memset(new_data + count * _item_size, 0, (size - count) * _item_size);
        }

        return res;
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        const Self& src_vec = assert_cast<const Self&>(src);
        auto origin_size = size();
        auto new_size = indices_end - indices_begin;
        if (_item_size == 0) {
            _item_size = src_vec._item_size;
        }
        DCHECK(_item_size == src_vec._item_size) << "dst and src should have the same _item_size";
        resize(origin_size + new_size);

        for (int i = 0; i < new_size; ++i) {
            int offset = indices_begin[i];
            if (offset > -1) {
                memcpy(&_data[(origin_size + i) * _item_size], &src_vec._data[offset * _item_size],
                       _item_size);
            } else {
                memset(&_data[(origin_size + i) * _item_size], 0, _item_size);
            }
        }
    }

    void clear() override {
        _data.clear();
        _item_count = 0;
    }

    [[noreturn]] Field operator[](size_t n) const override {
        LOG(FATAL) << "operator[] not supported";
    }

    void get(size_t n, Field& res) const override { LOG(FATAL) << "get not supported"; }

    [[noreturn]] StringRef get_data_at(size_t n) const override {
        LOG(FATAL) << "get_data_at not supported";
    }

    void insert(const Field& x) override { LOG(FATAL) << "insert not supported"; }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        LOG(FATAL) << "insert_range_from not supported";
    }

    void insert_data(const char* pos, size_t length) override {
        LOG(FATAL) << "insert_data not supported";
    }

    void insert_default() override { LOG(FATAL) << "insert_default not supported"; }

    void pop_back(size_t n) override { LOG(FATAL) << "pop_back not supported"; }

    StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                         char const*& begin) const override {
        LOG(FATAL) << "serialize_value_into_arena not supported";
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        LOG(FATAL) << "deserialize_and_insert_from_arena not supported";
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        LOG(FATAL) << "update_hash_with_value not supported";
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt,
                                  ssize_t result_size_hint) const override {
        LOG(FATAL) << "filter not supported";
    }

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override {
        LOG(FATAL) << "permute not supported";
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const override {
        LOG(FATAL) << "compare_at not supported";
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported";
    }

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& offsets) const override {
        LOG(FATAL) << "replicate not supported";
    }

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not supported";
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        LOG(FATAL) << "append_data_by_selector is not supported!";
    }

    void get_extremes(Field& min, Field& max) const override {
        LOG(FATAL) << "get_extremes not supported";
    }

    size_t byte_size() const override { return _data.size(); }

    size_t item_size() const { return _item_size; }

    void set_item_size(size_t size) {
        DCHECK(_item_count == 0 || size == _item_size)
                << "cannot reset _item_size of ColumnFixedLengthObject";
        _item_size = size;
    }

    size_t allocated_bytes() const override { return _data.allocated_bytes(); }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data not supported";
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data_default not supported";
    }

protected:
    size_t _item_size;
    size_t _item_count;
    Container _data;
};
} // namespace doris::vectorized
