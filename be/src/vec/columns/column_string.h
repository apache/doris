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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnString.h
// and modified by Doris

#pragma once

#include <cassert>
#include <cstring>

#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/pod_array.h"
#include "vec/common/sip_hash.h"
#include "vec/core/field.h"

namespace doris::vectorized {

/** Column for String values.
  */
class ColumnString final : public COWHelper<IColumn, ColumnString> {
public:
    using Char = UInt8;
    using Chars = PaddedPODArray<UInt8>;

private:
    friend class COWHelper<IColumn, ColumnString>;
    friend class OlapBlockDataConvertor;

    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    Offsets offsets;

    /// Bytes of strings, placed contiguously.
    /// For convenience, every string ends with terminating zero byte. Note that strings could contain zero bytes in the middle.
    Chars chars;

    size_t ALWAYS_INLINE offset_at(ssize_t i) const { return offsets[i - 1]; }

    /// Size of i-th element, including terminating zero.
    size_t ALWAYS_INLINE size_at(ssize_t i) const { return offsets[i] - offsets[i - 1]; }

    template <bool positive>
    struct less;

    template <bool positive>
    struct lessWithCollation;

    ColumnString() = default;

    ColumnString(const ColumnString& src)
            : offsets(src.offsets.begin(), src.offsets.end()),
              chars(src.chars.begin(), src.chars.end()) {}

public:
    const char* get_family_name() const override { return "String"; }

    size_t size() const override { return offsets.size(); }

    size_t byte_size() const override { return chars.size() + offsets.size() * sizeof(offsets[0]); }

    size_t allocated_bytes() const override {
        return chars.allocated_bytes() + offsets.allocated_bytes();
    }

    void protect() override;

    MutableColumnPtr clone_resized(size_t to_size) const override;

    MutableColumnPtr get_shrinked_column() override;

    Field operator[](size_t n) const override {
        assert(n < size());
        return Field(&chars[offset_at(n)], size_at(n));
    }

    void get(size_t n, Field& res) const override {
        assert(n < size());
        res.assign_string(&chars[offset_at(n)], size_at(n));
    }

    StringRef get_data_at(size_t n) const override {
        assert(n < size());
        return StringRef(&chars[offset_at(n)], size_at(n));
    }

/// Suppress gcc 7.3.1 warning: '*((void*)&<anonymous> +8)' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    void insert(const Field& x) override {
        const String& s = doris::vectorized::get<const String&>(x);
        const size_t old_size = chars.size();
        const size_t size_to_append = s.size();
        const size_t new_size = old_size + size_to_append;

        chars.resize(new_size);
        memcpy(chars.data() + old_size, s.c_str(), size_to_append);
        offsets.push_back(new_size);
    }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    void insert_from(const IColumn& src_, size_t n) override {
        const ColumnString& src = assert_cast<const ColumnString&>(src_);
        const size_t size_to_append =
                src.offsets[n] - src.offsets[n - 1]; /// -1th index is Ok, see PaddedPODArray.

        if (!size_to_append) {
            /// shortcut for empty string
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
        const size_t new_size = old_size + length;

        if (length) {
            chars.resize(new_size);
            memcpy(chars.data() + old_size, pos, length);
        }
        offsets.push_back(new_size);
    }

    void insert_data_without_reserve(const char* pos, size_t length) {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length;

        if (length) {
            chars.resize(new_size);
            memcpy(chars.data() + old_size, pos, length);
        }
        offsets.push_back_without_reserve(new_size);
    }

    /// Before insert strings, the caller should calculate the total size of strings,
    /// and reserve the chars & the offsets.
    void insert_many_strings_without_reserve(const StringRef* strings, size_t num) {
        Char* data = chars.data();
        size_t offset = chars.size();
        size_t length = 0;

        const char* ptr = strings[0].data;
        for (size_t i = 0; i != num; i++) {
            uint32_t len = strings[i].size;
            length += len;
            offset += len;
            offsets.push_back(offset);

            if (i != num - 1 && strings[i].data + len == strings[i + 1].data) {
                continue;
            }
            memcpy(data, ptr, length);
            data += length;
            if (LIKELY(i != num - 1)) {
                ptr = strings[i + 1].data;
                length = 0;
            }
        }
        chars.resize(offset);
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets_,
                                            const size_t num) override {
        static_assert(sizeof(offsets_[0]) == sizeof(*offsets.data()));
        if (UNLIKELY(num == 0)) {
            return;
        }
        const auto old_size = chars.size();
        const auto begin_offset = offsets_[0];
        const auto total_mem_size = offsets_[num] - begin_offset;
        if (LIKELY(total_mem_size > 0)) {
            chars.resize(total_mem_size + old_size);
            memcpy(chars.data() + old_size, data + begin_offset, total_mem_size);
        }
        const auto old_rows = offsets.size();
        auto tail_offset = offsets.back();
        DCHECK(tail_offset == old_size);
        offsets.resize(old_rows + num);
        auto* offsets_ptr = &offsets[old_rows];

        for (size_t i = 0; i < num; ++i) {
            offsets_ptr[i] = tail_offset + offsets_[i + 1] - begin_offset;
        }
        DCHECK(chars.size() == offsets.back());
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        size_t new_size = 0;
        for (size_t i = 0; i < num; i++) {
            new_size += len_array[i];
        }

        const size_t old_size = chars.size();
        chars.resize(old_size + new_size);

        Char* data = chars.data();
        size_t offset = old_size;
        for (size_t i = 0; i < num; i++) {
            uint32_t len = len_array[i];
            uint32_t start_offset = start_offset_array[i];
            // memcpy will deal len == 0, not do it here
            memcpy(data + offset, data_array + start_offset, len);
            offset += len;
            offsets.push_back(offset);
        }
    };

    void insert_many_strings(const StringRef* strings, size_t num) override {
        size_t new_size = 0;
        for (size_t i = 0; i < num; i++) {
            new_size += strings[i].size;
        }

        const size_t old_size = chars.size();
        chars.resize(old_size + new_size);

        Char* data = chars.data();
        size_t offset = old_size;
        for (size_t i = 0; i < num; i++) {
            uint32_t len = strings[i].size;
            if (len) {
                memcpy(data + offset, strings[i].data, len);
                offset += len;
            }
            offsets.push_back(offset);
        }
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t num, uint32_t /*dict_num*/) override {
        size_t offset_size = offsets.size();
        size_t old_size = chars.size();
        size_t new_size = old_size;
        offsets.resize(offsets.size() + num);

        for (size_t i = 0; i < num; i++) {
            int32_t codeword = data_array[i + start_index];
            new_size += dict[codeword].size;
            offsets[offset_size + i] = new_size;
        }

        chars.resize(new_size);

        for (size_t i = start_index; i < start_index + num; i++) {
            int32_t codeword = data_array[i];
            auto& src = dict[codeword];
            memcpy(chars.data() + old_size, src.data, src.size);
            old_size += src.size;
        }
    }

    void pop_back(size_t n) override {
        size_t nested_n = offsets.back() - offset_at(offsets.size() - n);
        chars.resize(chars.size() - nested_n);
        offsets.resize_assume_reserved(offsets.size() - n);
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;

    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void deserialize_vec(std::vector<StringRef>& keys, const size_t num_rows) override;

    size_t get_max_row_byte_size() const override;

    void serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                       size_t max_row_byte_size) const override;

    void serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                     const uint8_t* null_map,
                                     size_t max_row_byte_size) const override;

    void deserialize_vec_with_null_map(std::vector<StringRef>& keys, const size_t num_rows,
                                       const uint8_t* null_map) override;

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        size_t string_size = size_at(n);
        size_t offset = offset_at(n);

        // TODO: Rethink we really need to update the string_size?
        hash.update(reinterpret_cast<const char*>(&string_size), sizeof(string_size));
        hash.update(reinterpret_cast<const char*>(&chars[offset]), string_size);
    }

    void update_hashes_with_value(std::vector<SipHash>& hashes,
                                  const uint8_t* __restrict null_data) const override {
        SIP_HASHES_FUNCTION_COLUMN_IMPL();
    }

    void update_crcs_with_value(std::vector<uint64_t>& hashes, PrimitiveType type,
                                const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override {
        auto s = size();
        if (null_data) {
            for (int i = 0; i < s; i++) {
                if (null_data[i] == 0) {
                    size_t string_size = size_at(i);
                    size_t offset = offset_at(i);
                    hashes[i] = HashUtil::xxHash64WithSeed(
                            reinterpret_cast<const char*>(&chars[offset]), string_size, hashes[i]);
                }
            }
        } else {
            for (int i = 0; i < s; i++) {
                size_t string_size = size_at(i);
                size_t offset = offset_at(i);
                hashes[i] = HashUtil::xxHash64WithSeed(
                        reinterpret_cast<const char*>(&chars[offset]), string_size, hashes[i]);
            }
        }
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation& perm, size_t limit) const override;

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const;

    void insert_default() override { offsets.push_back(chars.size()); }

    void insert_many_defaults(size_t length) override {
        offsets.resize_fill(offsets.size() + length, chars.size());
    }

    int compare_at(size_t n, size_t m, const IColumn& rhs_,
                   int /*nan_direction_hint*/) const override {
        const ColumnString& rhs = assert_cast<const ColumnString&>(rhs_);
        return memcmp_small_allow_overflow15(chars.data() + offset_at(n), size_at(n),
                                             rhs.chars.data() + rhs.offset_at(m), rhs.size_at(m));
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override;

    ColumnPtr replicate(const Offsets& replicate_offsets) const override;

    void replicate(const uint32_t* counts, size_t target_size, IColumn& column, size_t begin = 0,
                   int count_sz = -1) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override {
        return scatter_impl<ColumnString>(num_columns, selector);
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
        return typeid(rhs) == typeid(ColumnString);
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
        const auto& r = assert_cast<const ColumnString&>(rhs);
        auto data = r.get_data_at(row);

        if (!self_row) {
            chars.clear();
            offsets[self_row] = data.size;
        } else {
            offsets[self_row] = offsets[self_row - 1] + data.size;
        }

        chars.insert(data.data, data.data + data.size);
    }

    // should replace according to 0,1,2... ,size,0,1,2...
    void replace_column_data_default(size_t self_row = 0) override {
        DCHECK(size() > self_row);

        if (!self_row) {
            chars.clear();
            offsets[self_row] = 0;
        } else {
            offsets[self_row] = offsets[self_row - 1];
        }
    }

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8>& cmp_res,
                          uint8* __restrict filter) const override;
};

} // namespace doris::vectorized
