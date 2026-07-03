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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnVector.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

#include "common/compare.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/cow.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/pod_array_fwd.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/uint24.h"
#include "core/value/vdatetime_value.h"
#include "util/unaligned.h"

class SipHash;

namespace doris {
template <int JoinOpType>
struct ProcessHashTableProbe;
}

namespace doris {
class Arena;
class ColumnSorter;
} // namespace doris

namespace doris {

/** A template for columns that use a simple array to store.
 */
template <PrimitiveType T>
class ColumnVector final : public COWHelper<IColumn, ColumnVector<T>> {
    static_assert(is_int_or_bool(T) || is_ip(T) || is_date_type(T) || is_float_or_double(T) ||
                  T == TYPE_TIMEV2 || T == TYPE_UINT32 || T == TYPE_UINT64 ||
                  T == TYPE_TIMESTAMPTZ);

private:
    using Self = ColumnVector;
    friend class COWHelper<IColumn, Self>;

    template <int JoinOpType>
    friend struct doris::ProcessHashTableProbe;

    struct less;
    struct greater;

public:
    using value_type = typename PrimitiveTypeTraits<T>::CppType;
    using Container = PaddedPODArray<value_type>;
    using ImmContainer = std::span<const value_type>;

    ColumnVector() = default;
    explicit ColumnVector(const size_t n) : data(n) {}
    explicit ColumnVector(const size_t n, const value_type x) : data(n, x) {}
    ColumnVector(const ColumnVector& src) {
        data.reserve(src.size());
        const auto values = src.immutable_data();
        data.insert(values.begin(), values.end());
    }

    /// Sugar constructor.
    ColumnVector(std::initializer_list<value_type> il) : data {il} {}

public:
    size_t size() const override { return data.size() + _external_size; }

    StringRef get_data_at(size_t n) const override {
        auto values = immutable_data();
        return {reinterpret_cast<const char*>(&values[n]), sizeof(values[n])};
    }

    void insert_from(const IColumn& src, size_t n) override {
        materialize_external_data();
        data.push_back(
                assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(src).immutable_data()[n]);
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        materialize_external_data();
        data.push_back(unaligned_load<value_type>(pos));
    }

    void insert_many_vals(value_type val, size_t n) {
        materialize_external_data();
        auto old_size = data.size();
        data.resize(old_size + n);
        std::fill(data.data() + old_size, data.data() + old_size + n, val);
    }

    void insert_many_from(const IColumn& src, size_t position, size_t length) override;

    void insert_range_of_integer(value_type begin, value_type end) {
        if constexpr (!is_float_or_double(T) && T != TYPE_TIMEV2 && T != TYPE_TIMESTAMPTZ &&
                      !is_date_type(T)) {
            materialize_external_data();
            auto old_size = data.size();
            auto new_size = old_size + static_cast<size_t>(end - begin);
            data.resize(new_size);
            std::iota(data.begin() + old_size, data.begin() + new_size, begin);
        } else {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "double column not support insert_range_of_integer");
        }
    }

    void insert_date_column(const char* data_ptr, size_t num) {
        materialize_external_data();
        data.reserve(data.size() + num);
        constexpr size_t input_value_size = sizeof(uint24_t);

        for (int i = 0; i < num; i++) {
            uint24_t val = 0;
            memcpy((char*)(&val), data_ptr, input_value_size);
            data_ptr += input_value_size;

            VecDateTimeValue date;
            date.set_olap_date(val);
            data.push_back_without_reserve(date);
        }
    }

    void insert_datetime_column(const char* data_ptr, size_t num) {
        materialize_external_data();
        data.reserve(data.size() + num);
        size_t value_size = sizeof(uint64_t);
        for (int i = 0; i < num; i++) {
            const char* cur_ptr = data_ptr + value_size * i;
            auto value = unaligned_load<uint64_t>(cur_ptr);
            VecDateTimeValue datetime = VecDateTimeValue::create_from_olap_datetime(value);
            this->insert_data(reinterpret_cast<char*>(&datetime), 0);
        }
    }

    /*
        use by date, datetime, basic type
    */
    void insert_many_fix_len_data(const char* data_ptr, size_t num) override {
        if (T == TYPE_DATE) {
            insert_date_column(data_ptr, num);
        } else if (T == TYPE_DATETIME) {
            insert_datetime_column(data_ptr, num);
        } else {
            insert_many_raw_data(data_ptr, num);
        }
    }

    void insert_many_fix_len_data_with_owner(const char* data_ptr, size_t num,
                                             std::shared_ptr<void> owner) override {
        if constexpr (T == TYPE_DATE || T == TYPE_DATETIME) {
            // The legacy DATE/DATETIME encodings stored on page are not byte-identical to the
            // in-memory VecDateTimeValue layout. They must keep the existing decode-and-convert
            // path instead of adopting page memory directly.
            insert_many_fix_len_data(data_ptr, num);
            return;
        }
        const bool can_use_external_data =
                owner != nullptr && data.empty() && !_has_external_data() &&
                reinterpret_cast<uintptr_t>(data_ptr) % alignof(value_type) == 0;
        if (can_use_external_data) {
            // The page decoder already owns decoded fixed-width values in naturally aligned page
            // memory. Keep the page owner and adopt one read-only view instead of copying it into
            // the local PODArray. This deliberately supports only one page-backed view, matching
            // SR's single-resource model. If a later append brings another page range, the normal
            // append path materializes this view first and then copies the new values.
            _set_external_data(reinterpret_cast<const value_type*>(data_ptr), num,
                               std::move(owner));
            return;
        }
        insert_many_fix_len_data(data_ptr, num);
    }

    void insert_many_raw_data(const char* data_ptr, size_t num) override {
        DCHECK(data_ptr);
        materialize_external_data();
        auto old_size = data.size();
        data.resize(old_size + num);
        memcpy(data.data() + old_size, data_ptr, num * sizeof(value_type));
    }

    void insert_default() override {
        materialize_external_data();
        data.push_back(default_value());
    }

    void insert_many_defaults(size_t length) override {
        materialize_external_data();
        size_t old_size = data.size();
        data.resize(old_size + length);
        std::fill(data.data() + old_size, data.data() + old_size + length, default_value());
    }

    void pop_back(size_t n) override {
        materialize_external_data();
        data.resize_assume_reserved(data.size() - n);
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;

    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void deserialize(StringRef* keys, const size_t num_rows) override;
    void deserialize_with_nullable(StringRef* keys, const size_t num_rows,
                                   PaddedPODArray<UInt8>& null_map) override;

    size_t get_max_row_byte_size() const override;

    void serialize(StringRef* keys, size_t num_rows) const override;
    void serialize_with_nullable(StringRef* keys, size_t num_rows, const bool has_null,
                                 const uint8_t* __restrict null_map) const override;

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override {
        auto values = immutable_data();
        if (null_data) {
            for (size_t i = start; i < end; i++) {
                if (null_data[i] == 0) {
                    hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&values[i]),
                                                      sizeof(value_type), hash);
                }
            }
        } else {
            for (size_t i = start; i < end; i++) {
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&values[i]),
                                                  sizeof(value_type), hash);
            }
        }
    }

    void ALWAYS_INLINE update_crc_with_value_without_null(size_t idx, uint32_t& hash) const {
        auto values = immutable_data();
        if constexpr (is_date_or_datetime(T)) {
            char buf[64];
            const auto& date_val = (const VecDateTimeValue&)values[idx];
            auto len = date_val.to_buffer(buf);
            hash = HashUtil::zlib_crc_hash(buf, len, hash);

        } else {
            hash = HashUtil::zlib_crc_hash(&values[idx], sizeof(value_type), hash);
        }
    }

    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override {
        if (null_data) {
            for (size_t i = start; i < end; i++) {
                if (null_data[i] == 0) {
                    update_crc_with_value_without_null(i, hash);
                }
            }
        } else {
            for (size_t i = start; i < end; i++) {
                update_crc_with_value_without_null(i, hash);
            }
        }
    }

    void update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                              const uint8_t* __restrict null_map) const override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;

    void update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;

    void update_crc32c_batch(uint32_t* __restrict hashes,
                             const uint8_t* __restrict null_map) const override;

    void update_crc32c_batch_default_on_null(uint32_t* __restrict hashes,
                                             const uint8_t* __restrict null_map) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;

    size_t byte_size() const override { return size() * sizeof(value_type); }

    size_t allocated_bytes() const override {
        // External data points into page-cache owned memory. Count only ColumnVector metadata here;
        // charging the page bytes again would double-count memory already tracked by the storage
        // page cache.
        return data.allocated_bytes();
    }

    bool has_enough_capacity(const IColumn& src) const override {
        // Capacity reuse is meaningful only for the mutable local PODArray. A page-backed column
        // has immutable external data, so append-style reuse must first materialize and should not
        // be selected by generic block reuse heuristics.
        if (_has_external_data()) {
            return false;
        }
        const auto& src_vec = assert_cast<const ColumnVector&>(src);
        return data.capacity() - data.size() > src_vec.size();
    }

    void insert_value(const value_type value) {
        materialize_external_data();
        data.push_back(value);
    }

    Status filter_by_selector(const uint16_t* sel, size_t sel_size,
                              IColumn* col_ptr) const override {
        const auto values = immutable_data();
        Self* output = assert_cast<Self*>(col_ptr);
        auto& res_data = output->get_data();
        DCHECK(res_data.empty())
                << "filter_by_selector requires the destination column to be empty";
        res_data.resize(sel_size);
        for (size_t i = 0; i < sel_size; i++) {
            // A lazily decoded fixed-length column may still point at one external page. Read
            // through immutable_data() so selector filtering has the same semantics before and
            // after materialization.
            res_data[i] = values[sel[i]];
        }
        return Status::OK();
    }

    /// This method implemented in header because it could be possibly devirtualized.
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const override {
        const auto lhs_values = immutable_data();
        const auto rhs_values =
                assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs_).immutable_data();
        return Compare::compare(lhs_values[n], rhs_values[m]);
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, HybridSorter& sorter,
                         IColumn::Permutation& res) const override;

    void reserve(size_t n) override { data.reserve(n); }

    void resize(size_t n) override {
        materialize_external_data();
        data.resize(n);
    }

    std::string get_name() const override { return type_to_string(T); }

    MutableColumnPtr clone_resized(size_t size) const override;

    Field operator[](size_t n) const override;

    void get(size_t n, Field& res) const override { res = (*this)[n]; }

    void clear() override {
        data.clear();
        _reset_external_data();
    }

    bool get_bool(size_t n) const override {
        if constexpr (T == TYPE_BOOLEAN) {
            return bool(immutable_data()[n]);
        } else {
            throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                                   "Method get_int is not supported for " + get_name());
            return false;
        }
    }

    Int64 get_int(size_t n) const override {
        if constexpr (is_date_type(T) || T == TYPE_TIMESTAMPTZ) {
            throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                                   "Method get_int is not supported for " + get_name());
            return 0;
        } else {
            return Int64(immutable_data()[n]);
        }
    }

    // For example, during create column_const(1, uint8), will use NearestFieldType
    // to cast a uint8 to int64, so that the Field is int64, but the column is created
    // using data_type, so that T == uint8, NearestFieldType<T> == uint64.
    // After the field is created, it will be inserted into the column,
    // but its type is different from column's data type (int64 vs uint64), so that during column
    // insert method, should use NearestFieldType<T> to get the Field and get it actual
    // uint8 value and then insert into column.
    void insert(const Field& x) override {
        materialize_external_data();
        data.push_back(x.get<T>());
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const IColumn::Filter& filter) override;
    MutableColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    StringRef get_raw_data() const override {
        auto values = immutable_data();
        return StringRef(reinterpret_cast<const char*>(values.data()),
                         values.size() * sizeof(value_type));
    }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnVector<T>);
    }

    /** More efficient methods of manipulation - to manipulate with data directly. */
    Container& get_data() {
        materialize_external_data();
        return data;
    }

    const Container& get_data() const {
        materialize_external_data();
        return data;
    }

    ImmContainer immutable_data() const {
        if (_has_external_data()) {
            return {_external_data, _external_size};
        }
        return {data.data(), data.size()};
    }

    const value_type& get_element(size_t n) const { return immutable_data()[n]; }

    value_type& get_element(size_t n) {
        materialize_external_data();
        return data[n];
    }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        materialize_external_data();
        data[self_row] =
                assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs).immutable_data()[row];
    }

    // Optimized batch version using memcpy for continuous range
    void replace_column_data_range(const IColumn& src, size_t src_start, size_t count,
                                   size_t self_start) override {
        DCHECK(size() >= self_start + count);
        materialize_external_data();
        const auto& src_col = assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(src);
        DCHECK(src_col.size() >= src_start + count);
        auto src_values = src_col.immutable_data();
        memcpy(data.data() + self_start, src_values.data() + src_start, count * sizeof(value_type));
    }

    bool support_replace_column_data_range() const override { return true; }

    void replace_column_null_data(const uint8_t* __restrict null_map) override;

    bool support_replace_column_null_data() const override { return true; }

    void replace_float_special_values() override;

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8_t>& cmp_res,
                          uint8_t* __restrict filter) const override;

    void erase(size_t start, size_t length) override {
        materialize_external_data();
        if (start >= data.size() || length == 0) {
            return;
        }
        length = std::min(length, data.size() - start);
        size_t elements_to_move = data.size() - start - length;
        memmove(data.data() + start, data.data() + start + length,
                elements_to_move * sizeof(value_type));
        data.resize(data.size() - length);
    }
    size_t serialize_impl(char* pos, const size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    size_t serialize_size_at(size_t row) const override { return sizeof(value_type); }
    // when run function which need_replace_null_data_to_default, use the value far from 0 to avoid
    // raise errors for null cell.
    static value_type default_value() {
        if constexpr (is_date_type(T) || T == PrimitiveType::TYPE_TIMESTAMPTZ) {
            return PrimitiveTypeTraits<T>::CppType::DEFAULT_VALUE;
        } else {
            return value_type();
        }
    }

private:
    bool _has_external_data() const { return _external_owner != nullptr; }

    void _set_external_data(const value_type* external_data, size_t external_size,
                            std::shared_ptr<void> owner) {
        if (external_size == 0) {
            return;
        }
        DCHECK(external_data != nullptr);
        DCHECK(owner != nullptr);
        DCHECK(!_has_external_data());
        _external_owner = std::move(owner);
        _external_data = external_data;
        _external_size = external_size;
    }

    void _reset_external_data() const {
        _external_owner.reset();
        _external_data = nullptr;
        _external_size = 0;
    }

    void materialize_external_data() const {
        if (!_has_external_data()) {
            return;
        }
        // This is the boundary back to the traditional ColumnVector contract. The zero-copy path
        // keeps at most one page-backed span. Any mutable access or append that cannot reuse that
        // single page resource copies it into Doris-owned storage before continuing.
        const auto old_size = data.size();
        data.resize(old_size + _external_size);
        memcpy(data.data() + old_size, _external_data, _external_size * sizeof(value_type));
        _reset_external_data();
    }

protected:
    uint32_t _zlib_crc32_hash(uint32_t hash, size_t idx) const;
    uint32_t _crc32c_hash_value(uint32_t hash, const value_type& value) const;
    uint32_t _crc32c_hash(uint32_t hash, size_t idx) const;
    mutable Container data;
    mutable std::shared_ptr<void> _external_owner;
    mutable const value_type* _external_data = nullptr;
    mutable size_t _external_size = 0;
};

using ColumnUInt8 = ColumnVector<TYPE_BOOLEAN>;
using ColumnInt8 = ColumnVector<TYPE_TINYINT>;
using ColumnInt16 = ColumnVector<TYPE_SMALLINT>;
using ColumnInt32 = ColumnVector<TYPE_INT>;
using ColumnInt64 = ColumnVector<TYPE_BIGINT>;
using ColumnInt128 = ColumnVector<TYPE_LARGEINT>;
using ColumnBool = ColumnUInt8;
using ColumnDate = ColumnVector<TYPE_DATE>;
using ColumnDateTime = ColumnVector<TYPE_DATETIME>;
using ColumnDateV2 = ColumnVector<TYPE_DATEV2>;
using ColumnDateTimeV2 = ColumnVector<TYPE_DATETIMEV2>;
using ColumnFloat32 = ColumnVector<TYPE_FLOAT>;
using ColumnFloat64 = ColumnVector<TYPE_DOUBLE>;
using ColumnIPv4 = ColumnVector<TYPE_IPV4>;
using ColumnIPv6 = ColumnVector<TYPE_IPV6>;
using ColumnTimeV2 = ColumnVector<TYPE_TIMEV2>;
using ColumnTimeStampTz = ColumnVector<TYPE_TIMESTAMPTZ>;
using ColumnOffset32 = ColumnVector<TYPE_UINT32>;
using ColumnOffset64 = ColumnVector<TYPE_UINT64>;

} // namespace doris
