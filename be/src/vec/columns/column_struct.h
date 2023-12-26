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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnTuple.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <stdint.h>
#include <sys/types.h>

#include <algorithm>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/sip_hash.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

class SipHash;

namespace doris {
namespace vectorized {
class Arena;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/** Column, that is just group of few another columns.
  */
class ColumnStruct final : public COWHelper<IColumn, ColumnStruct> {
private:
    friend class COWHelper<IColumn, ColumnStruct>;

    using TupleColumns = std::vector<WrappedPtr>;
    TupleColumns columns;

    template <bool positive>
    struct Less;

    explicit ColumnStruct(MutableColumns&& mutable_columns);
    ColumnStruct(const ColumnStruct&) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnStruct>;
    static Ptr create(const Columns& columns);
    static Ptr create(const TupleColumns& columns);
    static Ptr create(Columns&& arg) { return create(arg); }

    template <typename Arg,
              typename = typename std::enable_if<std::is_rvalue_reference<Arg&&>::value>::type>
    static MutablePtr create(Arg&& arg) {
        return Base::create(std::forward<Arg>(arg));
    }

    std::string get_name() const override;
    bool is_column_struct() const override { return true; }
    const char* get_family_name() const override { return "Struct"; }
    MutableColumnPtr clone_empty() const override;
    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override { return columns.at(0)->size(); }

    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;

    bool is_default_at(size_t n) const override;
    [[noreturn]] StringRef get_data_at(size_t n) const override {
        LOG(FATAL) << "Method get_data_at is not supported for " + get_name();
        __builtin_unreachable();
    }
    [[noreturn]] void insert_data(const char* pos, size_t length) override {
        LOG(FATAL) << "Method insert_data is not supported for " + get_name();
        __builtin_unreachable();
    }
    void insert(const Field& x) override;
    void insert_from(const IColumn& src_, size_t n) override;
    void insert_default() override;
    void pop_back(size_t n) override;
    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(std::vector<SipHash>& hashes,
                                  const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data = nullptr) const override;

    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset = 0,
                                const uint8_t* __restrict null_data = nullptr) const override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override {
        LOG(FATAL) << "get_permutation not implemented";
    }
    void append_data_by_selector(MutableColumnPtr& res, const Selector& selector) const override {
        return append_data_by_selector_impl<ColumnStruct>(res, selector);
    }
    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        const auto& r = assert_cast<const ColumnStruct&>(rhs);

        for (size_t idx = 0; idx < columns.size(); ++idx) {
            columns[idx]->replace_column_data(r.get_column(idx), row, self_row);
        }
    }

    void replace_column_data_default(size_t self_row = 0) override {
        DCHECK(size() > self_row);
        for (size_t idx = 0; idx < columns.size(); ++idx) {
            columns[idx]->replace_column_data_default(self_row);
        }
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    ColumnPtr replicate(const Offsets& offsets) const override;
    void replicate(const uint32_t* counts, size_t target_size, IColumn& column) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override;

    // ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs_,
                                int nan_direction_hint) const override {
        LOG(FATAL) << "compare_at not implemented";
        __builtin_unreachable();
    }

    MutableColumnPtr get_shrinked_column() override;

    void reserve(size_t n) override;
    void resize(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    void for_each_subcolumn(ColumnCallback callback) override;
    bool structure_equals(const IColumn& rhs) const override;

    size_t tuple_size() const { return columns.size(); }

    const IColumn& get_column(size_t idx) const { return *columns[idx]; }
    IColumn& get_column(size_t idx) { return *columns[idx]; }

    const TupleColumns& get_columns() const { return columns; }
    Columns get_columns_copy() const { return {columns.begin(), columns.end()}; }

    const ColumnPtr& get_column_ptr(size_t idx) const { return columns[idx]; }
    ColumnPtr& get_column_ptr(size_t idx) { return columns[idx]; }

    void clear() override {
        for (auto col : columns) {
            col->clear();
        }
    }
};

} // namespace doris::vectorized
