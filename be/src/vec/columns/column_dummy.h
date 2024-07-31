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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/IColumnDummy.h
// and modified by Doris

#pragma once

#include "vec/columns/column.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/pod_array.h"

namespace doris::vectorized {

/** Base class for columns-constants that contain a value that is not in the `Field`.
  * Not a full-fledged column and is used in a special way.
  */
class IColumnDummy : public IColumn {
public:
    IColumnDummy() : s(0) {}
    IColumnDummy(size_t s_) : s(s_) {}

public:
    virtual MutableColumnPtr clone_dummy(size_t s_) const = 0;

    MutableColumnPtr clone_resized(size_t s) const override { return clone_dummy(s); }
    size_t size() const override { return s; }
    void insert_default() override { ++s; }
    void pop_back(size_t n) override { s -= n; }
    size_t byte_size() const override { return 0; }
    size_t allocated_bytes() const override { return 0; }
    int compare_at(size_t, size_t, const IColumn&, int) const override { return 0; }

    [[noreturn]] Field operator[](size_t) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Cannot get value from {}", get_name());
        __builtin_unreachable();
    }

    void get(size_t, Field&) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Cannot get value from {}", get_name());
    }

    void insert(const Field&) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Cannot insert element into {}",
                               get_name());
    }

    StringRef get_data_at(size_t) const override { return {}; }

    void insert_data(const char*, size_t) override { ++s; }

    void clear() override {};

    StringRef serialize_value_into_arena(size_t /*n*/, Arena& arena,
                                         char const*& begin) const override {
        return {arena.alloc_continue(0, begin), 0};
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        ++s;
        return pos;
    }

    void insert_from(const IColumn&, size_t) override { ++s; }

    void insert_range_from(const IColumn& /*src*/, size_t /*start*/, size_t length) override {
        s += length;
    }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override {
        s += (indices_end - indices_begin);
    }

    ColumnPtr filter(const Filter& filt, ssize_t /*result_size_hint*/) const override {
        return clone_dummy(count_bytes_in_filter(filt));
    }

    size_t filter(const Filter& filter) override {
        const auto result_size = count_bytes_in_filter(filter);
        s = result_size;
        return result_size;
    }

    ColumnPtr permute(const Permutation& perm, size_t limit) const override {
        if (s != perm.size()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Size of permutation doesn't match size of column.");
            __builtin_unreachable();
        }

        return clone_dummy(limit ? std::min(s, limit) : s);
    }

    void get_permutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/,
                         Permutation& res) const override {
        res.resize(s);
        for (size_t i = 0; i < s; ++i) res[i] = i;
    }

    ColumnPtr replicate(const Offsets& offsets) const override {
        column_match_offsets_size(s, offsets.size());

        return clone_dummy(offsets.back());
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        size_t num_rows = size();

        if (num_rows < selector.size()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Size of selector: {}, is larger than size of column:{}",
                                   selector.size(), num_rows);
        }

        res->reserve(num_rows);

        for (size_t i = 0; i < selector.size(); ++i) res->insert_from(*this, selector[i]);
    }

    void append_data_by_selector(MutableColumnPtr& res, const IColumn::Selector& selector,
                                 size_t begin, size_t end) const override {
        size_t num_rows = size();

        if (num_rows < selector.size()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Size of selector: {}, is larger than size of column:{}",
                                   selector.size(), num_rows);
        }

        res->reserve(num_rows);

        for (size_t i = begin; i < end; ++i) res->insert_from(*this, selector[i]);
    }

    void addSize(size_t delta) { s += delta; }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call the method in column dummy");
        __builtin_unreachable();
    }

protected:
    size_t s;
};

} // namespace doris::vectorized
