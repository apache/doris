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
#include "vec/common/memcmp_small.h"
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
    ColumnFixedLengthObject() = delete;

private:
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
        DCHECK_GT(_item_size, 0) << "_item_size should be greater than 0";
        _data.resize(n * _item_size);
        _item_count = n;
    }

    MutableColumnPtr clone_resized(size_t size) const override {
        auto res = create(_item_size);

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

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override {
        const Self& src_vec = assert_cast<const Self&>(src);
        auto origin_size = size();
        auto new_size = indices_end - indices_begin;
        if (_item_size == 0) {
            _item_size = src_vec._item_size;
        }
        DCHECK_EQ(_item_size, src_vec._item_size) << "dst and src should have the same _item_size";
        resize(origin_size + new_size);

        for (uint32_t i = 0; i < new_size; ++i) {
            memcpy(&_data[(origin_size + i) * _item_size],
                   &src_vec._data[indices_begin[i] * _item_size], _item_size);
        }
    }

    void clear() override {
        _data.clear();
        _item_count = 0;
    }

    Field operator[](size_t n) const override {
        return {_data.data() + n * _item_size, _item_size};
    }

    void get(size_t n, Field& res) const override {
        res.assign_string(_data.data() + n * _item_size, _item_size);
    }

    StringRef get_data_at(size_t n) const override {
        return {reinterpret_cast<const char*>(&_data[n * _item_size]), _item_size};
    }

    void insert(const Field& x) override {
        insert_data(vectorized::get<const String&>(x).data(), _item_size);
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        const auto& src_col = assert_cast<const ColumnFixedLengthObject&>(src);
        CHECK_EQ(src_col._item_size, _item_size);

        if (length == 0) {
            return;
        }

        if (start + length > src_col._item_count) {
            throw doris::Exception(
                    doris::ErrorCode::INTERNAL_ERROR,
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
        const auto& src_col = assert_cast<const ColumnFixedLengthObject&>(src);
        DCHECK(_item_size == src_col._item_size) << "dst and src should have the same _item_size  "
                                                 << _item_size << " " << src_col._item_size;
        insert_data((const char*)(&src_col._data[n * _item_size]), _item_size);
    }

    void insert_data(const char* pos, size_t length) override {
        size_t old_size = size();
        resize(old_size + 1);
        memcpy(&_data[old_size * _item_size], pos, _item_size);
    }

    void insert_default() override {
        size_t old_size = size();
        resize(old_size + 1);
        memset(&_data[old_size * _item_size], 0, _item_size);
    }

    void pop_back(size_t n) override {
        DCHECK_GE(_item_count, n);
        resize(_item_count - n);
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                         char const*& begin) const override {
        char* pos = arena.alloc_continue(_item_size, begin);
        memcpy(pos, &_data[n * _item_size], _item_size);
        return {pos, _item_size};
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        insert_data(pos, _item_size);
        return pos + _item_size;
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        hash.update(reinterpret_cast<const char*>(_data.data() + n * _item_size), _item_size);
    }

    ColumnPtr filter(const IColumn::Filter& filter, ssize_t result_size_hint) const override {
        column_match_filter_size(size(), filter.size());
        auto res = create(_item_size);
        res->resize(result_size_hint);

        for (size_t i = 0, pos = 0; i < filter.size(); i++) {
            if (filter[i]) {
                memcpy(&res->_data[pos * _item_size], &_data[i * _item_size], _item_size);
                pos++;
            }
        }
        return res;
    }

    size_t filter(const IColumn::Filter& filter) override {
        size_t pos = 0;
        for (size_t i = 0; i < filter.size(); i++) {
            if (filter[i]) {
                memcpy(&_data[pos * _item_size], &_data[i * _item_size], _item_size);
                pos++;
            }
        }
        resize(pos);
        return pos;
    }

    ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override {
        if (limit == 0) {
            limit = size();
        } else {
            limit = std::min(size(), limit);
        }

        auto res = ColumnFixedLengthObject::create(_item_size);
        res->resize(limit);
        for (size_t i = 0; i < limit; ++i) {
            memcpy_small_allow_read_write_overflow15(res->_data.data() + i * _item_size,
                                                     _data.data() + perm[i] * _item_size,
                                                     _item_size);
        }
        return res;
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

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        this->template append_data_by_selector_impl<Self>(res, selector);
    }

    void append_data_by_selector(MutableColumnPtr& res, const IColumn::Selector& selector,
                                 size_t begin, size_t end) const override {
        this->template append_data_by_selector_impl<Self>(res, selector, begin, end);
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
        auto* dst = _data.data() + old_count * _item_size;
        for (size_t i = 0; i < num; i++) {
            auto* src = data_array + start_offset_array[i];
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
