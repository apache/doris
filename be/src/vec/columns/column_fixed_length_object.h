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

#include "vec/columns/column.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/sip_hash.h"

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

    bool can_be_inside_nullable() const override { return true; }

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

            if (size > count) {
                memset(new_data + count * _item_size, 0, (size - count) * _item_size);
            }
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

    void insert_indices_from_join(const IColumn& src, const uint32_t* indices_begin,
                                  const uint32_t* indices_end) override {
        const Self& src_vec = assert_cast<const Self&>(src);
        auto origin_size = size();
        auto new_size = indices_end - indices_begin;
        if (_item_size == 0) {
            _item_size = src_vec._item_size;
        }
        DCHECK(_item_size == src_vec._item_size) << "dst and src should have the same _item_size";
        resize(origin_size + new_size);

        for (uint32_t i = 0; i < new_size; ++i) {
            auto offset = indices_begin[i];
            if (offset) {
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

    StringRef get_data_at(size_t n) const override {
        return StringRef(reinterpret_cast<const char*>(&_data[n * _item_size]), _item_size);
    }

    void insert(const Field& x) override { LOG(FATAL) << "insert not supported"; }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        const ColumnFixedLengthObject& src_col = assert_cast<const ColumnFixedLengthObject&>(src);
        CHECK_EQ(src_col._item_size, _item_size);

        if (length == 0) {
            return;
        }

        if (start + length > src_col._item_count) {
            LOG(FATAL) << fmt::format(
                    "Parameters start = {}, length = {} are out of bound in "
                    "ColumnFixedLengthObject::insert_range_from method (data.size() = {})",
                    start, length, src_col._item_count);
        }

        size_t old_size = size();
        resize(old_size + length);
        memcpy(&_data[old_size * _item_size], &src_col._data[start * _item_size],
               length * _item_size);
    }

    void insert_from(const IColumn& src, size_t n) override {
        const ColumnFixedLengthObject& src_col = assert_cast<const ColumnFixedLengthObject&>(src);
        DCHECK(_item_size == src_col._item_size) << "dst and src should have the same _item_size  "
                                                 << _item_size << " " << src_col._item_size;
        size_t old_size = size();
        resize(old_size + 1);
        memcpy(&_data[old_size * _item_size], &src_col._data[n * _item_size], _item_size);
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
        hash.update(reinterpret_cast<const char*>(_data.data() + n * _item_size), _item_size);
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt,
                                  ssize_t result_size_hint) const override {
        LOG(FATAL) << "filter not supported";
    }

    [[noreturn]] size_t filter(const IColumn::Filter&) override {
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

    void replicate(const uint32_t* indexs, size_t target_size, IColumn& column) const override {
        LOG(FATAL) << "not support";
    }

    ColumnPtr index(const IColumn& indexes, size_t limit) const override {
        LOG(FATAL) << "index not supported";
    }

    void get_indices_of_non_default_rows(IColumn::Offsets64& indices, size_t from,
                                         size_t limit) const override {
        LOG(FATAL) << "get_indices_of_non_default_rows not supported in ColumnDictionary";
    }

    ColumnPtr replicate(const IColumn::Offsets& offsets) const override {
        size_t size = _item_count;
        column_match_offsets_size(size, offsets.size());
        auto res = doris::vectorized::ColumnFixedLengthObject::create(_item_size);
        if (0 == size) {
            return res;
        }
        res->resize(offsets.back());
        typename Self::Container& res_data = res->get_data();

        IColumn::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i) {
            size_t size_to_replicate = offsets[i] - prev_offset;
            for (size_t j = 0; j < size_to_replicate; ++j) {
                memcpy(&res_data[(prev_offset + j) * _item_size], &_data[i * _item_size],
                       _item_size);
            }
            prev_offset = offsets[i];
        }

        return res;
    }

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not supported";
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        this->template append_data_by_selector_impl<Self>(res, selector);
    }

    size_t byte_size() const override { return _data.size(); }

    size_t item_size() const { return _item_size; }

    void set_item_size(size_t size) {
        DCHECK(_item_count == 0 || size == _item_size)
                << "cannot reset _item_size of ColumnFixedLengthObject";
        _item_size = size;
    }

    size_t allocated_bytes() const override { return _data.allocated_bytes(); }

    //NOTICE: here is replace: this[self_row] = rhs[row]
    //But column string is replaced all when self_row = 0
    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        DCHECK(_item_size == assert_cast<const Self&>(rhs)._item_size)
                << _item_size << " " << assert_cast<const Self&>(rhs)._item_size;
        auto obj = assert_cast<const Self&>(rhs).get_data_at(row);
        memcpy(&_data[self_row * _item_size], obj.data, _item_size);
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data_default not supported";
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        const auto old_size = size();
        const auto begin_offset = offsets[0];
        const size_t total_mem_size = offsets[num] - begin_offset;
        resize(old_size + num);
        memcpy(_data.data() + old_size, data + begin_offset, total_mem_size);
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }

        size_t old_count = _item_count;
        resize(old_count + num);
        auto dst = _data.data() + old_count * _item_size;
        for (size_t i = 0; i < num; i++) {
            auto src = data_array + start_offset_array[i];
            uint32_t len = len_array[i];
            dst += i * _item_size;
            memcpy(dst, src, len);
        }
    }

protected:
    size_t _item_size;
    size_t _item_count;
    Container _data;
};
} // namespace doris::vectorized
