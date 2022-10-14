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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnComplex.h
// and modified by Doris

#pragma once

#include <vector>

#include "olap/hll.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

namespace doris::vectorized {

template <typename T>
class ColumnComplexType final : public COWHelper<IColumn, ColumnComplexType<T>> {
private:
    ColumnComplexType() {}
    ColumnComplexType(const size_t n) : data(n) {}
    friend class COWHelper<IColumn, ColumnComplexType<T>>;

public:
    using Self = ColumnComplexType;
    using value_type = T;
    using Container = std::vector<value_type>;

    bool is_numeric() const override { return false; }

    bool is_bitmap() const override { return std::is_same_v<T, BitmapValue>; }

    size_t size() const override { return data.size(); }

    StringRef get_data_at(size_t n) const override {
        return StringRef(reinterpret_cast<const char*>(&data[n]), sizeof(data[n]));
    }

    void insert_from(const IColumn& src, size_t n) override {
        data.push_back(assert_cast<const Self&>(src).get_data()[n]);
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        data.push_back(*reinterpret_cast<const T*>(pos));
    }

    void insert_binary_data(const char* pos, size_t length) {
        insert_default();
        T* pvalue = &get_element(size() - 1);
        if (!length) {
            *pvalue = *reinterpret_cast<const T*>(pos);
            return;
        }

        if constexpr (std::is_same_v<T, BitmapValue>) {
            pvalue->deserialize(pos);
        } else if constexpr (std::is_same_v<T, HyperLogLog>) {
            pvalue->deserialize(Slice(pos, length));
        } else {
            LOG(FATAL) << "Unexpected type in column complex";
        }
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        for (size_t i = 0; i < num; i++) {
            insert_binary_data(data_array + start_offset_array[i], len_array[i]);
        }
    }

    void insert_default() override { data.push_back(T()); }

    void insert_many_defaults(size_t length) override {
        size_t old_size = data.size();
        data.resize(old_size + length);
    }

    void clear() override { data.clear(); }

    // TODO: value_type is not a pod type, so we also need to
    // calculate the memory requested by value_type
    size_t byte_size() const override { return data.size() * sizeof(data[0]); }

    size_t allocated_bytes() const override { return byte_size(); }

    void protect() override {}

    void insert_value(T value) { data.emplace_back(std::move(value)); }

    [[noreturn]] void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                      IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not implemented";
    }

    void reserve(size_t n) override { data.reserve(n); }

    void resize(size_t n) override { data.resize(n); }

    const char* get_family_name() const override { return TypeName<T>::get(); }

    MutableColumnPtr clone_resized(size_t size) const override;

    [[noreturn]] void insert(const Field& x) override {
        LOG(FATAL) << "insert field not implemented";
    }

    [[noreturn]] Field operator[](size_t n) const override {
        LOG(FATAL) << "operator[] not implemented";
    }
    [[noreturn]] void get(size_t n, Field& res) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] UInt64 get64(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] Float64 get_float64(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] UInt64 get_uint(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        auto& col = assert_cast<const Self&>(src);
        auto& src_data = col.get_data();
        auto st = src_data.begin() + start;
        auto ed = st + length;
        data.insert(data.end(), st, ed);
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        const Self& src_vec = assert_cast<const Self&>(src);
        auto new_size = indices_end - indices_begin;

        for (int i = 0; i < new_size; ++i) {
            auto offset = *(indices_begin + i);
            if (offset == -1) {
                data.emplace_back(T {});
            } else {
                data.emplace_back(src_vec.get_element(offset));
            }
        }
    }

    void pop_back(size_t n) override { data.erase(data.end() - n, data.end()); }
    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const override {
        LOG(FATAL) << "serialize_value_into_arena not implemented";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) override {
        LOG(FATAL) << "deserialize_and_insert_from_arena not implemented";
    }

    // maybe we do not need to impl the function
    void update_hash_with_value(size_t n, SipHash& hash) const override {
        // TODO add hash function
    }

    virtual void update_hashes_with_value(
            std::vector<SipHash>& hashes,
            const uint8_t* __restrict null_data = nullptr) const override {
            // TODO add hash function
    };

    virtual void update_hashes_with_value(
            uint64_t* __restrict hashes,
            const uint8_t* __restrict null_data = nullptr) const override {
        // TODO add hash function
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const override {
        LOG(FATAL) << "compare_at not implemented";
    }

    void get_extremes(Field& min, Field& max) const override {
        LOG(FATAL) << "get_extremes not implemented";
    }

    bool can_be_inside_nullable() const override { return true; }

    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    StringRef get_raw_data() const override {
        return StringRef(reinterpret_cast<const char*>(data.data()), data.size());
    }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnComplexType<T>);
    }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    const T& get_element(size_t n) const { return data[n]; }

    T& get_element(size_t n) { return data[n]; }

    ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override;

    void replicate(const uint32_t* counts, size_t target_size, IColumn& column, size_t begin = 0,
                   int count_sz = -1) const override;

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not implemented";
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        this->template append_data_by_selector_impl<ColumnComplexType<T>>(res, selector);
    }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = assert_cast<const Self&>(rhs).data[row];
    }

    void replace_column_data_default(size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = T();
    }

private:
    Container data;
};

template <typename T>
MutableColumnPtr ColumnComplexType<T>::clone_resized(size_t size) const {
    auto res = this->create();

    if (size > 0) {
        auto& new_col = assert_cast<Self&>(*res);
        new_col.data = this->data;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnComplexType<T>::filter(const IColumn::Filter& filt,
                                       ssize_t result_size_hint) const {
    size_t size = data.size();
    if (size != filt.size()) {
        LOG(FATAL) << "Size of filter doesn't match size of column.";
    }

    if (data.size() == 0) return this->create();
    auto res = this->create();
    Container& res_data = res->get_data();

    if (result_size_hint) res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8* filt_pos = filt.data();
    const UInt8* filt_end = filt_pos + size;
    const T* data_pos = data.data();

    while (filt_pos < filt_end) {
        if (*filt_pos) res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnComplexType<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = data.size();

    limit = limit ? std::min(size, limit) : size;

    if (perm.size() < limit) {
        LOG(FATAL) << "Size of permutation is less than required.";
    }

    auto res = this->create(limit);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) {
        res_data[i] = data[perm[i]];
    }

    return res;
}

template <typename T>
ColumnPtr ColumnComplexType<T>::replicate(const IColumn::Offsets& offsets) const {
    size_t size = data.size();
    if (size != offsets.size()) {
        LOG(FATAL) << "Size of offsets doesn't match size of column.";
    }

    if (0 == size) return this->create();

    auto res = this->create();
    typename Self::Container& res_data = res->get_data();
    res_data.reserve(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i) {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j) {
            res_data.push_back(data[i]);
        }
    }

    return res;
}

template <typename T>
void ColumnComplexType<T>::replicate(const uint32_t* counts, size_t target_size, IColumn& column,
                                     size_t begin, int count_sz) const {
    size_t size = count_sz < 0 ? data.size() : count_sz;
    if (0 == size) return;

    auto& res = reinterpret_cast<ColumnComplexType<T>&>(column);
    typename Self::Container& res_data = res.get_data();
    res_data.reserve(target_size);

    size_t end = size + begin;
    for (size_t i = begin; i < end; ++i) {
        size_t size_to_replicate = counts[i];
        for (size_t j = 0; j < size_to_replicate; ++j) {
            res_data.push_back(data[i]);
        }
    }
}

using ColumnBitmap = ColumnComplexType<BitmapValue>;
using ColumnHLL = ColumnComplexType<HyperLogLog>;

template <typename T>
struct is_complex : std::false_type {};

template <>
struct is_complex<BitmapValue> : std::true_type {};
//DataTypeBitMap::FieldType = BitmapValue

template <>
struct is_complex<HyperLogLog> : std::true_type {};
//DataTypeHLL::FieldType = HyperLogLog

template <class T>
constexpr bool is_complex_v = is_complex<T>::value;

} // namespace doris::vectorized
