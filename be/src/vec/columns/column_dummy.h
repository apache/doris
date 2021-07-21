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
        LOG(FATAL) << "Cannot get value from " << get_name();
    }

    void get(size_t, Field&) const override {
        LOG(FATAL) << "Cannot get value from " << get_name();
    }

    void insert(const Field&) override {
        LOG(FATAL) << "Cannot insert element into " << get_name();
    }

    StringRef get_data_at(size_t) const override { return {}; }

    void insert_data(const char*, size_t) override { ++s; }

    StringRef serialize_value_into_arena(size_t /*n*/, Arena& arena,
                                         char const*& begin) const override {
        return {arena.alloc_continue(0, begin), 0};
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        ++s;
        return pos;
    }

    void update_hash_with_value(size_t /*n*/, SipHash& /*hash*/) const override {}

    void insert_from(const IColumn&, size_t) override { ++s; }

    void insert_range_from(const IColumn& /*src*/, size_t /*start*/, size_t length) override {
        s += length;
    }

    ColumnPtr filter(const Filter& filt, ssize_t /*result_size_hint*/) const override {
        return clone_dummy(count_bytes_in_filter(filt));
    }

    ColumnPtr permute(const Permutation& perm, size_t limit) const override {
        if (s != perm.size()) {
            LOG(FATAL) << "Size of permutation doesn't match size of column.";
        }

        return clone_dummy(limit ? std::min(s, limit) : s);
    }

    void get_permutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/,
                         Permutation& res) const override {
        res.resize(s);
        for (size_t i = 0; i < s; ++i) res[i] = i;
    }

    ColumnPtr replicate(const Offsets& offsets) const override {
        if (s != offsets.size()) {
            LOG(FATAL) << "Size of offsets doesn't match size of column.";
        }

        return clone_dummy(offsets.back());
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override {
        if (s != selector.size()) {
            LOG(FATAL) << "Size of selector doesn't match size of column.";
        }

        std::vector<size_t> counts(num_columns);
        for (auto idx : selector) ++counts[idx];

        MutableColumns res(num_columns);
        for (size_t i = 0; i < num_columns; ++i) res[i] = clone_resized(counts[i]);

        return res;
    }

    void get_extremes(Field&, Field&) const override {}

    void addSize(size_t delta) { s += delta; }

    bool is_dummy() const override { return true; }

protected:
    size_t s;
};

} // namespace doris::vectorized
