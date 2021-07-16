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

#include <cmath>

#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_vector_helper.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"

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
    using Container = DecimalPaddedPODArray<T>;

private:
    ColumnDecimal(const size_t n, UInt32 scale_) : data(n, scale_), scale(scale_) {}

    ColumnDecimal(const ColumnDecimal& src) : data(src.data), scale(src.scale) {}

public:
    const char* get_family_name() const override { return TypeName<T>::get(); }

    bool is_numeric() const override { return false; }
    bool can_be_inside_nullable() const override { return true; }
    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    size_t size() const override { return data.size(); }
    size_t byte_size() const override { return data.size() * sizeof(data[0]); }
    size_t allocated_bytes() const override { return data.allocated_bytes(); }
    void protect() override { data.protect(); }
    void reserve(size_t n) override { data.reserve(n); }

    void insert_from(const IColumn& src, size_t n) override {
        data.push_back(static_cast<const Self&>(src).get_data()[n]);
    }
    void insert_data(const char* pos, size_t /*length*/) override;
    void insert_default() override { data.push_back(T()); }
    void insert(const Field& x) override {
        data.push_back(doris::vectorized::get<NearestFieldType<T>>(x));
    }
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void pop_back(size_t n) override { data.resize_assume_reserved(data.size() - n); }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    void update_hash_with_value(size_t n, SipHash& hash) const override;
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
    Int64 get_int(size_t n) const override { return Int64(data[n] * scale); }
    UInt64 get64(size_t n) const override;
    bool is_default_at(size_t n) const override { return data[n] == 0; }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const;

    ColumnPtr replicate(const IColumn::Offsets& offsets) const override;
    void get_extremes(Field& min, Field& max) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns,
                           const IColumn::Selector& selector) const override {
        return this->template scatter_impl<Self>(num_columns, selector);
    }

    //    void gather(ColumnGathererStream & gatherer_stream) override;

    bool structure_equals(const IColumn& rhs) const override {
        if (auto rhs_concrete = typeid_cast<const ColumnDecimal<T>*>(&rhs))
            return scale == rhs_concrete->scale;
        return false;
    }

    void insert(const T value) { data.push_back(value); }
    Container& get_data() { return data; }
    const Container& get_data() const { return data; }
    const T& get_element(size_t n) const { return data[n]; }
    T& get_element(size_t n) { return data[n]; }

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
};

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
