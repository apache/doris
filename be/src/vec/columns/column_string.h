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

#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/pod_array.h"
#include "vec/common/sip_hash.h"
#include "vec/core/field.h"

class Collator;

namespace doris::vectorized {

/** Column for String values.
  */
class ColumnString final : public COWHelper<IColumn, ColumnString> {
public:
    using Char = UInt8;
    using Chars = PaddedPODArray<UInt8>;

private:
    friend class COWHelper<IColumn, ColumnString>;

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

    Field operator[](size_t n) const override {
        assert(n < size());
        return Field(&chars[offset_at(n)], size_at(n) - 1);
    }

    void get(size_t n, Field& res) const override {
        assert(n < size());
        res.assign_string(&chars[offset_at(n)], size_at(n) - 1);
    }

    StringRef get_data_at(size_t n) const override {
        assert(n < size());
        return StringRef(&chars[offset_at(n)], size_at(n) - 1);
    }

    StringRef get_data_at_with_terminating_zero(size_t n) const override {
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
        const size_t size_to_append = s.size() + 1;
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

        if (size_to_append == 1) {
            /// shortcut for empty string
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

    /// Like getData, but inserting data should be zero-ending (i.e. length is 1 byte greater than real string size).
    void insert_data_with_terminating_zero(const char* pos, size_t length) {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length;

        chars.resize(new_size);
        memcpy(chars.data() + old_size, pos, length);
        offsets.push_back(new_size);
    }

    void pop_back(size_t n) override {
        size_t nested_n = offsets.back() - offset_at(offsets.size() - n);
        chars.resize(chars.size() - nested_n);
        offsets.resize_assume_reserved(offsets.size() - n);
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;

    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        size_t string_size = size_at(n);
        size_t offset = offset_at(n);

        hash.update(reinterpret_cast<const char*>(&string_size), sizeof(string_size));
        hash.update(reinterpret_cast<const char*>(&chars[offset]), string_size);
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation& perm, size_t limit) const override;

    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const;

    void insert_default() override {
        chars.push_back(0);
        offsets.push_back(offsets.back() + 1);
    }

    int compare_at(size_t n, size_t m, const IColumn& rhs_,
                  int /*nan_direction_hint*/) const override {
        const ColumnString& rhs = assert_cast<const ColumnString&>(rhs_);
        return memcmp_small_allow_overflow15(chars.data() + offset_at(n), size_at(n) - 1,
                                          rhs.chars.data() + rhs.offset_at(m), rhs.size_at(m) - 1);
    }

    /// Variant of compare_at for string comparison with respect of collation.
    int compare_at_with_collation(size_t n, size_t m, const IColumn& rhs_,
                               const Collator& collator) const;

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    /// Sorting with respect of collation.
    void get_permutation_with_collation(const Collator& collator, bool reverse, size_t limit,
                                     Permutation& res) const;

    ColumnPtr replicate(const Offsets& replicate_offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override {
        return scatter_impl<ColumnString>(num_columns, selector);
    }

    //    void gather(ColumnGathererStream & gatherer_stream) override;

    void reserve(size_t n) override;

    void get_extremes(Field& min, Field& max) const override;

    bool can_be_inside_nullable() const override { return true; }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnString);
    }

    Chars& get_chars() { return chars; }
    const Chars& get_chars() const { return chars; }

    Offsets& get_offsets() { return offsets; }
    const Offsets& get_offsets() const { return offsets; }
};

} // namespace doris::vectorized
