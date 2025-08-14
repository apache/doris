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
#include <string>
#include <type_traits>
#include <typeinfo>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "olap/uint24.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/common/unaligned.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"

class SipHash;

namespace doris::vectorized {
class Arena;
class ColumnSorter;
} // namespace doris::vectorized

namespace doris::vectorized {
#include "common/compile_check_begin.h"

/** Stuff for comparing numbers.
  * Integer values are compared as usual.
  * Floating-point numbers are compared this way that NaNs always end up at the end
  *  (if you don't do this, the sort would not work at all).
  */
template <typename T>
struct CompareHelper {
    static bool less(T a, T b, int /*nan_direction_hint*/) { return a < b; }
    static bool greater(T a, T b, int /*nan_direction_hint*/) { return a > b; }

    /** Compares two numbers. Returns a number less than zero, equal to zero, or greater than zero if a < b, a == b, a > b, respectively.
      * If one of the values is NaN, then
      * - if nan_direction_hint == -1 - NaN are considered less than all numbers;
      * - if nan_direction_hint == 1 - NaN are considered to be larger than all numbers;
      * Essentially: nan_direction_hint == -1 says that the comparison is for sorting in descending order.
      */
    static int compare(T a, T b, int /*nan_direction_hint*/) {
        return a > b ? 1 : (a < b ? -1 : 0);
    }
};

template <typename T>
struct FloatCompareHelper {
    static bool less(T a, T b, int nan_direction_hint) {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b) return false;
        if (isnan_a) return nan_direction_hint < 0;
        if (isnan_b) return nan_direction_hint > 0;

        return a < b;
    }

    static bool greater(T a, T b, int nan_direction_hint) {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b) return false;
        if (isnan_a) return nan_direction_hint > 0;
        if (isnan_b) return nan_direction_hint < 0;

        return a > b;
    }

    static int compare(T a, T b, int nan_direction_hint) {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);
        if (UNLIKELY(isnan_a || isnan_b)) {
            if (isnan_a && isnan_b) return 0;

            return isnan_a ? nan_direction_hint : -nan_direction_hint;
        }

        return (T(0) < (a - b)) - ((a - b) < T(0));
    }
};

template <>
struct CompareHelper<Float32> : public FloatCompareHelper<Float32> {};
template <>
struct CompareHelper<Float64> : public FloatCompareHelper<Float64> {};

/** A template for columns that use a simple array to store.
 */
template <PrimitiveType T>
class ColumnVector final : public COWHelper<IColumn, ColumnVector<T>> {
    static_assert(is_int_or_bool(T) || is_ip(T) || is_date_type(T) || is_float_or_double(T) ||
                  T == TYPE_TIME || T == TYPE_TIMEV2 || T == TYPE_UINT32 || T == TYPE_UINT64);

private:
    using Self = ColumnVector;
    friend class COWHelper<IColumn, Self>;

    struct less;
    struct greater;

public:
    using value_type = typename PrimitiveTypeTraits<T>::ColumnItemType;
    using Container = PaddedPODArray<value_type>;

    ColumnVector() = default;
    ColumnVector(const size_t n) : data(n) {}
    ColumnVector(const size_t n, const value_type x) : data(n, x) {}
    ColumnVector(const ColumnVector& src) : data(src.data.begin(), src.data.end()) {}

    /// Sugar constructor.
    ColumnVector(std::initializer_list<value_type> il) : data {il} {}

    size_t size() const override { return data.size(); }

    StringRef get_data_at(size_t n) const override {
        return {reinterpret_cast<const char*>(&data[n]), sizeof(data[n])};
    }

    void insert_from(const IColumn& src, size_t n) override {
        data.push_back(assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(src).get_data()[n]);
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        data.push_back(unaligned_load<value_type>(pos));
    }

    void insert_many_vals(value_type val, size_t n) {
        auto old_size = data.size();
        data.resize(old_size + n);
        std::fill(data.data() + old_size, data.data() + old_size + n, val);
    }

    void insert_many_from(const IColumn& src, size_t position, size_t length) override;

    void insert_range_of_integer(value_type begin, value_type end) {
        if constexpr (!is_float_or_double(T) && T != TYPE_TIME && T != TYPE_TIMEV2) {
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
        data.reserve(data.size() + num);
        constexpr size_t input_value_size = sizeof(uint24_t);

        for (int i = 0; i < num; i++) {
            uint24_t val = 0;
            memcpy((char*)(&val), data_ptr, input_value_size);
            data_ptr += input_value_size;

            VecDateTimeValue date;
            date.set_olap_date(val);
            data.push_back_without_reserve(unaligned_load<Int64>(reinterpret_cast<char*>(&date)));
        }
    }

    void insert_datetime_column(const char* data_ptr, size_t num) {
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

    void insert_many_raw_data(const char* data_ptr, size_t num) override {
        DCHECK(data_ptr);
        auto old_size = data.size();
        data.resize(old_size + num);
        memcpy(data.data() + old_size, data_ptr, num * sizeof(value_type));
    }

    void insert_default() override { data.push_back(default_value()); }

    void insert_many_defaults(size_t length) override {
        size_t old_size = data.size();
        data.resize(old_size + length);
        std::fill(data.data() + old_size, data.data() + old_size + length, default_value());
    }

    void pop_back(size_t n) override { data.resize_assume_reserved(data.size() - n); }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;

    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void deserialize_vec(StringRef* keys, const size_t num_rows) override;

    size_t get_max_row_byte_size() const override;

    void serialize_vec(StringRef* keys, size_t num_rows) const override;

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override {
        if (null_data) {
            for (size_t i = start; i < end; i++) {
                if (null_data[i] == 0) {
                    hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                      sizeof(value_type), hash);
                }
            }
        } else {
            for (size_t i = start; i < end; i++) {
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                  sizeof(value_type), hash);
            }
        }
    }

    void ALWAYS_INLINE update_crc_with_value_without_null(size_t idx, uint32_t& hash) const {
        if constexpr (is_date_or_datetime(T)) {
            char buf[64];
            const auto& date_val = (const VecDateTimeValue&)data[idx];
            auto len = date_val.to_buffer(buf);
            hash = HashUtil::zlib_crc_hash(buf, len, hash);

        } else {
            hash = HashUtil::zlib_crc_hash(&data[idx], sizeof(value_type), hash);
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
    void update_hash_with_value(size_t n, SipHash& hash) const override;

    void update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;

    size_t byte_size() const override { return data.size() * sizeof(data[0]); }

    size_t allocated_bytes() const override { return data.allocated_bytes(); }

    bool has_enough_capacity(const IColumn& src) const override {
        const auto& src_vec = assert_cast<const ColumnVector&>(src);
        return data.capacity() - data.size() > src_vec.data.size();
    }

    void insert_value(const value_type value) { data.push_back(value); }

    /// This method implemented in header because it could be possibly devirtualized.
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const override {
        return CompareHelper<value_type>::compare(
                data[n], assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs_).data[m],
                nan_direction_hint);
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override;

    void reserve(size_t n) override { data.reserve(n); }

    void resize(size_t n) override { data.resize(n); }

    std::string get_name() const override { return type_to_string(T); }

    MutableColumnPtr clone_resized(size_t size) const override;

    Field operator[](size_t n) const override;

    void get(size_t n, Field& res) const override { res = (*this)[n]; }

    void clear() override { data.clear(); }

    bool get_bool(size_t n) const override { return bool(data[n]); }

    Int64 get_int(size_t n) const override { return Int64(data[n]); }

    // For example, during create column_const(1, uint8), will use NearestFieldType
    // to cast a uint8 to int64, so that the Field is int64, but the column is created
    // using data_type, so that T == uint8, NearestFieldType<T> == uint64.
    // After the field is created, it will be inserted into the column,
    // but its type is different from column's data type (int64 vs uint64), so that during column
    // insert method, should use NearestFieldType<T> to get the Field and get it actual
    // uint8 value and then insert into column.
    void insert(const Field& x) override {
        data.push_back(
                doris::vectorized::get<typename PrimitiveTypeTraits<T>::NearestFieldType>(x));
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const IColumn::Filter& filter) override;

    MutableColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    StringRef get_raw_data() const override {
        return StringRef(reinterpret_cast<const char*>(data.data()), data.size());
    }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnVector<T>);
    }

    /** More efficient methods of manipulation - to manipulate with data directly. */
    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    const value_type& get_element(size_t n) const { return data[n]; }

    value_type& get_element(size_t n) { return data[n]; }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs).data[row];
    }

    void replace_column_null_data(const uint8_t* __restrict null_map) override;

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8_t>& cmp_res,
                          uint8_t* __restrict filter) const override;

    void erase(size_t start, size_t length) override {
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

protected:
    static value_type default_value() {
        if constexpr (T == PrimitiveType::TYPE_DATEV2 || T == PrimitiveType::TYPE_DATETIMEV2) {
            return PrimitiveTypeTraits<T>::CppType::FIRST_DAY.to_date_int_val();
        } else if constexpr (T == PrimitiveType::TYPE_DATE || T == PrimitiveType::TYPE_DATETIME) {
            return PrimitiveTypeTraits<T>::CppType::FIRST_DAY;
        } else {
            return value_type();
        }
    }

    Container data;
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
using ColumnTime = ColumnVector<TYPE_TIME>;
using ColumnTimeV2 = ColumnVector<TYPE_TIMEV2>;
using ColumnOffset32 = ColumnVector<TYPE_UINT32>;
using ColumnOffset64 = ColumnVector<TYPE_UINT64>;

} // namespace doris::vectorized
#include "common/compile_check_end.h"
