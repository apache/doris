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

#include <cassert>
#include <cstring>

#include "runtime/jsonb_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/pod_array.h"
#include "vec/common/sip_hash.h"
#include "vec/core/field.h"

namespace doris::vectorized {
class ColumnJsonb final : public COWHelper<IColumn, ColumnJsonb> {
public:
    using Char = ColumnString::Char;
    using Chars = ColumnString::Chars;
    using ColumnStringPtr = ColumnString::MutablePtr;

private:
    friend class COWHelper<IColumn, ColumnJsonb>;

    WrappedPtr column_string;

    template <bool positive>
    struct less;

    template <bool positive>
    struct lessWithCollation;

    ColumnJsonb() : column_string(std::move(ColumnString::create())) {}

    ColumnJsonb(const ColumnJsonb& src)
            : column_string(std::move(src.column_string->clone_resized(src.column_string->size()))) {}

public:
    const char* get_family_name() const override { return "JSONB"; }

    size_t size() const override { return column_string->size(); }

    size_t byte_size() const override { return column_string->byte_size(); }

    size_t allocated_bytes() const override {
        return column_string->allocated_bytes();
    }

    void protect() override;

    MutableColumnPtr clone_resized(size_t to_size) const override;

    Field operator[](size_t n) const override {
        assert(n < size());
        StringRef s = column_string->get_data_at(n);
        // TODO check
        Field field = JsonbField(s.data, s.size);
        return field;
    }

    void get(size_t n, Field& res) const override {
        assert(n < size());
        StringRef s = column_string->get_data_at(n);
        // TODO check
        res.assign_jsonb(s.data, s.size);
    }

    StringRef get_data_at(size_t n) const override {
        return column_string->get_data_at(n);
    }

/// Suppress gcc 7.3.1 warning: '*((void*)&<anonymous> +8)' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    void insert(const Field& x) override {
        const JsonbField& j = doris::vectorized::get<const JsonbField&>(x);
        column_string->insert_data(j.get_value(), j.get_size());
    }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    void insert_from(const IColumn& src_, size_t n) override {
        const ColumnJsonb& src = assert_cast<const ColumnJsonb&>(src_);
        column_string->insert_from(*src.column_string, n);
    }

    void insert_data(const char* pos, size_t length) override {
        column_string->insert_data(pos, length);
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets_,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }

        size_t new_size = offsets_[num] - offsets_[0] + num * sizeof(char);
        const size_t old_size = chars.size();
        chars.resize(new_size + old_size);

        auto* data_ptr = chars.data();
        size_t offset = old_size;

        for (size_t i = 0; i != num; ++i) {
            uint32_t len = offsets_[i + 1] - offsets_[i];
            if (LIKELY(len)) {
                memcpy(data_ptr + offset, data + offsets_[i], len);
                offset += len;
            }
            data_ptr[offset] = 0;
            offset += 1;
            offsets.push_back(offset);
        }
        DCHECK(offset == chars.size());
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        column_string->insert_many_binary_data(data_array, len_array, start_offset_array, num);
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t num, uint32_t dict_num) override {
        column_string->insert_many_dict_data(data_array, start_index, dict, num, dict_num);
    }

    void pop_back(size_t n) override {
        column_string->pop_back(n);
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;

    const char* deserialize_and_insert_from_arena(const char* pos) override;

    size_t get_max_row_byte_size() const override;

    void serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                       size_t max_row_byte_size) const override;

    void serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                     const uint8_t* null_map,
                                     size_t max_row_byte_size) const override;

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        column_string->update_hash_with_value(n, hash);
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation& perm, size_t limit) const override;

    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const;

    void insert_default() override {
        JsonBinaryValue empty_jsonb("{}");
        insert_data(empty_jsonb.value(), empty_jsonb.size());
    }

    void insert_many_defaults(size_t length) override {
        JsonBinaryValue empty_jsonb("{}");
        for (size_t i = 0; i < length; i++) {
            insert_default();
        }
    }

    int compare_at(size_t n, size_t m, const IColumn& rhs_,
                   int /*nan_direction_hint*/) const override {
        LOG(FATAL) << "Not support compraing between JSONB value";
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override;

    ColumnPtr replicate(const Offsets& replicate_offsets) const override;

    void replicate(const uint32_t* counts, size_t target_size, IColumn& column, size_t begin = 0,
                   int count_sz = -1) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override {
        return scatter_impl<ColumnJsonb>(num_columns, selector);
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        append_data_by_selector_impl<ColumnString>(res, selector);
    }
    //    void gather(ColumnGathererStream & gatherer_stream) override;

    void reserve(size_t n) override;

    void resize(size_t n) override;

    void get_extremes(Field& min, Field& max) const override;

    bool can_be_inside_nullable() const override { return true; }

    bool is_column_string() const override { return true; }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnJsonb);
    }

    ColumnString* get_column_string() const { return (ColumnString*)column_string.get(); }

    Chars& get_chars() { return get_column_string()->get_chars(); }

    const Chars& get_chars() const { return get_column_string()->get_chars(); }

    Offsets& get_offsets() { return get_column_string()->get_offsets(); }

    const Offsets& get_offsets() const { return get_column_string()->get_offsets(); }

    void clear() override {
        column_string->clear();
    }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        column_string->replace_column_data(rhs, row, self_row);
    }

    // should replace according to 0,1,2... ,size,0,1,2...
    void replace_column_data_default(size_t self_row = 0) override {
        column_string->replace_column_data_default(self_row);
    }

    MutableColumnPtr get_shrinked_column() override;
};
}; // namespace doris::vectorized