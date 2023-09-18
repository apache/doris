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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/ColumnDecimal.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <vector>

#include "gutil/integral_types.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_vector_helper.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/pod_array.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

class SipHash;

namespace doris {
namespace vectorized {
class Arena;
class ColumnSorter;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/// PaddedPODArray extended by Decimal scale
template <typename T>
class DecimalPaddedPODArray : public PaddedPODArray<T> {
public:
    using Base = PaddedPODArray<T>;
    using Base::operator[];

    DecimalPaddedPODArray(size_t size, UInt32 scale_) : Base(size), scale(scale_) {}

    DecimalPaddedPODArray(const DecimalPaddedPODArray& other)
            : Base(other.begin(), other.end()), scale(other.scale) {}

    DecimalPaddedPODArray(DecimalPaddedPODArray&& other) {
        this->swap(other);
        std::swap(scale, other.scale);
    }

    DecimalPaddedPODArray& operator=(DecimalPaddedPODArray&& other) {
        this->swap(other);
        std::swap(scale, other.scale);
        return *this;
    }

    UInt32 get_scale() const { return scale; }

private:
    UInt32 scale;
};

/// A ColumnVector for Decimals
template <typename T>
class ColumnDecimal final : public COWHelper<ColumnVectorHelper, ColumnDecimal<T>> {
    static_assert(IsDecimalNumber<T>);

private:
    using Self = ColumnDecimal;
    friend class COWHelper<ColumnVectorHelper, Self>;

public:
    using value_type = T;
    using Container = DecimalPaddedPODArray<T>;

private:
    ColumnDecimal(const size_t n, UInt32 scale_) : data(n, scale_), scale(scale_) {}

    ColumnDecimal(const ColumnDecimal& src) : data(src.data), scale(src.scale) {}

public:
    const char* get_family_name() const override { return TypeName<T>::get(); }

    bool is_numeric() const override { return false; }
    bool is_column_decimal() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }
    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    size_t size() const override { return data.size(); }
    size_t byte_size() const override { return data.size() * sizeof(data[0]); }
    size_t allocated_bytes() const override { return data.allocated_bytes(); }
    void reserve(size_t n) override { data.reserve(n); }
    void resize(size_t n) override { data.resize(n); }

    void insert_from(const IColumn& src, size_t n) override {
        data.push_back(assert_cast<const Self&>(src).get_data()[n]);
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        auto origin_size = size();
        auto new_size = indices_end - indices_begin;
        data.resize(origin_size + new_size);
        const T* src_data = reinterpret_cast<const T*>(src.get_raw_data().data);

        for (int i = 0; i < new_size; ++i) {
            data[origin_size + i] = src_data[indices_begin[i]];
        }
    }

    void insert_many_fix_len_data(const char* data_ptr, size_t num) override;

    void insert_many_raw_data(const char* pos, size_t num) override {
        size_t old_size = data.size();
        data.resize(old_size + num);
        memcpy(data.data() + old_size, pos, num * sizeof(T));
    }

    void insert_data(const char* pos, size_t /*length*/) override;
    void insert_default() override { data.push_back(T()); }
    void insert(const Field& x) override {
        data.push_back(doris::vectorized::get<NearestFieldType<T>>(x));
    }
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_many_defaults(size_t length) override {
        size_t old_size = data.size();
        data.resize(old_size + length);
        memset(data.data() + old_size, 0, length * sizeof(data[0]));
    }

    void pop_back(size_t n) override { data.resize_assume_reserved(data.size() - n); }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;

    size_t get_max_row_byte_size() const override;

    void serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                       size_t max_row_byte_size) const override;

    void serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                     const uint8_t* null_map) const override;

    void deserialize_vec(std::vector<StringRef>& keys, const size_t num_rows) override;

    void deserialize_vec_with_null_map(std::vector<StringRef>& keys, const size_t num_rows,
                                       const uint8_t* null_map) override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_hashes_with_value(std::vector<SipHash>& hashes,
                                  const uint8_t* __restrict null_data) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;
    void update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    int compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override;

    MutableColumnPtr clone_resized(size_t size) const override;

    Field operator[](size_t n) const override { return DecimalField(data[n], scale); }

    StringRef get_raw_data() const override {
        return StringRef(reinterpret_cast<const char*>(data.data()), data.size());
    }
    StringRef get_data_at(size_t n) const override {
        return StringRef(reinterpret_cast<const char*>(&data[n]), sizeof(data[n]));
    }
    void get(size_t n, Field& res) const override { res = (*this)[n]; }
    bool get_bool(size_t n) const override { return bool(data[n]); }
    Int64 get_int(size_t n) const override { return Int64(data[n].value * scale); }
    UInt64 get64(size_t n) const override;
    bool is_default_at(size_t n) const override { return data[n].value == 0; }

    void clear() override { data.clear(); }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;

    size_t filter(const IColumn::Filter& filter) override;

    ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const;

    void get_indices_of_non_default_rows(IColumn::Offsets64& indices, size_t from,
                                         size_t limit) const override {
        return this->template get_indices_of_non_default_rows_impl<Self>(indices, from, limit);
    }

    ColumnPtr index(const IColumn& indexes, size_t limit) const override;

    ColumnPtr replicate(const IColumn::Offsets& offsets) const override;

    void replicate(const uint32_t* indexs, size_t target_size, IColumn& column) const override;

    void get_extremes(Field& min, Field& max) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns,
                           const IColumn::Selector& selector) const override {
        return this->template scatter_impl<Self>(num_columns, selector);
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        this->template append_data_by_selector_impl<Self>(res, selector);
    }

    //    void gather(ColumnGathererStream & gatherer_stream) override;

    bool structure_equals(const IColumn& rhs) const override {
        if (auto rhs_concrete = typeid_cast<const ColumnDecimal<T>*>(&rhs))
            return scale == rhs_concrete->scale;
        return false;
    }

    void insert_value(const T value) { data.push_back(value); }
    Container& get_data() { return data; }
    const Container& get_data() const { return data; }
    const T& get_element(size_t n) const { return data[n]; }
    T& get_element(size_t n) { return data[n]; }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = assert_cast<const Self&>(rhs).data[row];
    }

    void replace_column_data_default(size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = T();
    }

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8>& cmp_res,
                          uint8* __restrict filter) const override;

    UInt32 get_scale() const { return scale; }

    T get_scale_multiplier() const;
    T get_whole_part(size_t n) const { return data[n] / get_scale_multiplier(); }
    T get_fractional_part(size_t n) const { return data[n] % get_scale_multiplier(); }

protected:
    Container data;
    UInt32 scale;

    template <typename U>
    void permutation(bool reverse, size_t limit, PaddedPODArray<U>& res) const {
        size_t s = data.size();
        res.resize(s);
        for (U i = 0; i < s; ++i) res[i] = i;

        auto sort_end = res.end();
        if (limit && limit < s) sort_end = res.begin() + limit;

        if (reverse)
            std::partial_sort(res.begin(), sort_end, res.end(),
                              [this](size_t a, size_t b) { return data[a] > data[b]; });
        else
            std::partial_sort(res.begin(), sort_end, res.end(),
                              [this](size_t a, size_t b) { return data[a] < data[b]; });
    }

    void ALWAYS_INLINE decimalv2_do_crc(size_t i, uint32_t& hash) const {
        const auto& dec_val = (const DecimalV2Value&)data[i];
        int64_t int_val = dec_val.int_value();
        int32_t frac_val = dec_val.frac_value();
        hash = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), hash);
        hash = HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), hash);
    };
};

template <typename>
class ColumnVector;

template <typename T, bool is_decimal = false>
struct ColumnVectorOrDecimalT {
    using Col = ColumnVector<T>;
};

template <typename T>
struct ColumnVectorOrDecimalT<T, true> {
    using Col = ColumnDecimal<T>;
};

template <typename T>
using ColumnVectorOrDecimal = typename ColumnVectorOrDecimalT<T, IsDecimalNumber<T>>::Col;

template <typename T>
template <typename Type>
ColumnPtr ColumnDecimal<T>::index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const {
    size_t size = indexes.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    auto res = this->create(limit, scale);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) res_data[i] = data[indexes[i]];

    return res;
}

} // namespace doris::vectorized
