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
    using Char = UInt8;
    using Chars = PaddedPODArray<UInt8>;

private:
    friend class COWHelper<IColumn, ColumnJsonb>;

    Offsets offsets;

    Chars chars;

    size_t ALWAYS_INLINE offset_at(ssize_t i) const { return offsets[i - 1]; }

    size_t ALWAYS_INLINE size_at(ssize_t i) const { return offsets[i] - offsets[i - 1]; }

    template <bool positive>
    struct less;

    template <bool positive>
    struct lessWithCollation;

    ColumnJsonb() = default;

    ColumnJsonb(const ColumnJsonb& src)
            : offsets(src.offsets.begin(), src.offsets.end()),
              chars(src.chars.begin(), src.chars.end()) {}

public:
    const char* get_family_name() const override { return "JSONB"; }

    size_t size() const override { return offsets.size(); }

    size_t byte_size() const override { return chars.size() + offsets.size() * sizeof(offsets[0]); }

    size_t allocated_bytes() const override {
        return chars.allocated_bytes() + offsets.allocated_bytes();
    }

    void protect() override;

    MutableColumnPtr clone_resized(size_t to_size) const override;

    Field operator[](size_t n) const override {
        assert(n < size());
        return Field(&chars[offset_at(n)], size_at(n) - 1);
    }

    void get(size_t n, Field& res) const override {
        assert(n < size());
        res.assign_jsonb(&chars[offset_at(n)], size_at(n) - 1);
    }

    StringRef get_data_at(size_t n) const override {
        assert(n < size());
        return StringRef(&chars[offset_at(n)], size_at(n) - 1);
    }

/// Suppress gcc 7.3.1 warning: '*((void*)&<anonymous> +8)' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    void insert(const Field& x) override {
        const JsonbField& s = doris::vectorized::get<const JsonbField&>(x);

        const size_t old_size = chars.size();
        const size_t size_to_append = s.get_size() + 1;
        const size_t new_size = old_size + size_to_append;

        chars.resize(new_size);

        memcpy(chars.data() + old_size, s.get_value(), size_to_append - 1);
        chars.data()[new_size - 1] = 0;
        offsets.push_back(new_size);
    }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    void insert_from(const IColumn& src_, size_t n) override {
        const ColumnJsonb& src = assert_cast<const ColumnJsonb&>(src_);
        const size_t size_to_append =
                src.offsets[n] - src.offsets[n - 1]; /// -1th index is Ok, see PaddedPODArray.

        if (size_to_append == 1) {
            /// shortcut for empty jsonb
            chars.push_back(0);
            offsets.push_back(chars.size());
        } else {
            const size_t old_size = chars.size();
            const size_t offset = src.offsets[n - 1];
            const size_t new_size = old_size + size_to_append;

            chars.resize(new_size);
            memcpy_small_allow_read_write_overflow15(chars.data() + old_size, &src.chars[offset],
                                                     size_to_append);
            offsets.push_back(new_size);
        }
    }

    void insert_data(const char* pos, size_t length) override {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length + 1;

        chars.resize(new_size);
        if (length) memcpy(chars.data() + old_size, pos, length);
        chars[old_size + length] = 0;
        offsets.push_back(new_size);
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        size_t new_size = 0;
        for (size_t i = 0; i < num; i++) {
            new_size += len_array[i] + 1;
        }

        const size_t old_size = chars.size();
        chars.resize(old_size + new_size);

        Char* data = chars.data();
        size_t offset = old_size;
        for (size_t i = 0; i < num; i++) {
            uint32_t len = len_array[i];
            uint32_t start_offset = start_offset_array[i];
            if (len) memcpy(data + offset, data_array + start_offset, len);
            data[offset + len] = 0;
            offset += len + 1;
            offsets.push_back(offset);
        }
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t num, uint32_t /*dict_num*/) override {
        for (size_t end_index = start_index + num; start_index < end_index; ++start_index) {
            int32_t codeword = data_array[start_index];
            insert_data(dict[codeword].data, dict[codeword].size);
        }
    }

    void pop_back(size_t n) override {
        size_t nested_n = offsets.back() - offset_at(offsets.size() - n);
        chars.resize(chars.size() - nested_n);
        offsets.resize_assume_reserved(offsets.size() - n);
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
        size_t string_size = size_at(n);
        size_t offset = offset_at(n);

        hash.update(reinterpret_cast<const char*>(&string_size), sizeof(string_size));
        hash.update(reinterpret_cast<const char*>(&chars[offset]), string_size);
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
        size_t new_size = 0;
        for (size_t i = 0; i < length; i++) {
            new_size += empty_jsonb.size() + 1;
        }

        const size_t old_size = chars.size();
        chars.resize(old_size + new_size);

        Char* data = chars.data();
        size_t offset = old_size;
        for (size_t i = 0; i < length; i++) {
            uint32_t len = empty_jsonb.size();
            if (len) memcpy(data + offset, empty_jsonb.value(), len);
            data[offset + len] = 0;
            offset += len + empty_jsonb.size();
            offsets.push_back(offset);
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

    Chars& get_chars() { return chars; }

    const Chars& get_chars() const { return chars; }

    Offsets& get_offsets() { return offsets; }

    const Offsets& get_offsets() const { return offsets; }

    void clear() override {
        chars.clear();
        offsets.clear();
    }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        const auto& r = assert_cast<const ColumnJsonb&>(rhs);
        auto data = r.get_data_at(row);

        if (!self_row) {
            chars.clear();
            offsets[self_row] = data.size + 1;
        } else {
            offsets[self_row] = offsets[self_row - 1] + data.size + 1;
        }

        chars.insert(data.data, data.data + data.size + 1);
    }

    // should replace according to 0,1,2... ,size,0,1,2...
    void replace_column_data_default(size_t self_row = 0) override {
        DCHECK(size() > self_row);

        if (!self_row) {
            chars.clear();
            offsets[self_row] = 1;
        } else {
            offsets[self_row] = offsets[self_row - 1] + 1;
        }

        chars.emplace_back(0);
    }

    MutableColumnPtr get_shrinked_column() override;
};
}; // namespace doris::vectorized