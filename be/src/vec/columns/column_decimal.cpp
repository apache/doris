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

#include "vec/columns/column_decimal.h"

#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/exception.h"
#include "vec/common/sip_hash.h"
#include "vec/common/unaligned.h"

template <typename T>
bool decimal_less(T x, T y, doris::vectorized::UInt32 x_scale, doris::vectorized::UInt32 y_scale);

namespace doris::vectorized {

template <typename T>
int ColumnDecimal<T>::compare_at(size_t n, size_t m, const IColumn& rhs_, int) const {
    auto& other = static_cast<const Self&>(rhs_);
    const T& a = data[n];
    const T& b = other.data[m];

    if (scale == other.scale) return a > b ? 1 : (a < b ? -1 : 0);
    return decimal_less<T>(b, a, other.scale, scale)
                   ? 1
                   : (decimal_less<T>(a, b, scale, other.scale) ? -1 : 0);
}

template <typename T>
StringRef ColumnDecimal<T>::serialize_value_into_arena(size_t n, Arena& arena,
                                                       char const*& begin) const {
    auto pos = arena.alloc_continue(sizeof(T), begin);
    memcpy(pos, &data[n], sizeof(T));
    return StringRef(pos, sizeof(T));
}

template <typename T>
const char* ColumnDecimal<T>::deserialize_and_insert_from_arena(const char* pos) {
    data.push_back(unaligned_load<T>(pos));
    return pos + sizeof(T);
}

template <typename T>
UInt64 ColumnDecimal<T>::get64(size_t n) const {
    if constexpr (sizeof(T) > sizeof(UInt64)) {
        LOG(FATAL) << "Method get64 is not supported for " << get_family_name();
    }
    return static_cast<typename T::NativeType>(data[n]);
}

template <typename T>
void ColumnDecimal<T>::update_hash_with_value(size_t n, SipHash& hash) const {
    hash.update(data[n]);
}

template <typename T>
void ColumnDecimal<T>::get_permutation(bool reverse, size_t limit, int,
                                       IColumn::Permutation& res) const {
#if 1 /// TODO: perf test
    if (data.size() <= std::numeric_limits<UInt32>::max()) {
        PaddedPODArray<UInt32> tmp_res;
        permutation(reverse, limit, tmp_res);

        res.resize(tmp_res.size());
        for (size_t i = 0; i < tmp_res.size(); ++i) res[i] = tmp_res[i];
        return;
    }
#endif

    permutation(reverse, limit, res);
}

template <typename T>
ColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = limit ? std::min(data.size(), limit) : data.size();
    if (perm.size() < size) {
        LOG(FATAL) << "Size of permutation is less than required.";
    }

    auto res = this->create(size, scale);
    typename Self::Container& res_data = res->get_data();

    for (size_t i = 0; i < size; ++i) res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
MutableColumnPtr ColumnDecimal<T>::clone_resized(size_t size) const {
    auto res = this->create(0, scale);

    if (size > 0) {
        auto& new_col = static_cast<Self&>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count) {
            void* tail = &new_col.data[count];
            memset(tail, 0, (size - count) * sizeof(T));
        }
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::insert_data(const char* src, size_t /*length*/) {
    T tmp;
    memcpy(&tmp, src, sizeof(T));
    data.emplace_back(tmp);
}

template <typename T>
void ColumnDecimal<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnDecimal& src_vec = assert_cast<const ColumnDecimal&>(src);

    if (start + length > src_vec.data.size()) {
        LOG(FATAL) << fmt::format(
                "Parameters start = {}, length = {} are out of bound in "
                "ColumnDecimal<T>::insert_range_from method (data.size() = {})",
                start, length, src_vec.data.size());
    }

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
}

template <typename T>
ColumnPtr ColumnDecimal<T>::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    size_t size = data.size();
    if (size != filt.size()) {
        LOG(FATAL) << "Size of filter doesn't match size of column.";
    }

    auto res = this->create(0, scale);
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
ColumnPtr ColumnDecimal<T>::replicate(const IColumn::Offsets& offsets) const {
    size_t size = data.size();
    if (size != offsets.size()) {
        LOG(FATAL) << "Size of offsets doesn't match size of column.";
    }

    auto res = this->create(0, scale);
    if (0 == size) return res;

    typename Self::Container& res_data = res->get_data();
    res_data.reserve(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i) {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j) res_data.push_back(data[i]);
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::get_extremes(Field& min, Field& max) const {
    if (data.size() == 0) {
        min = NearestFieldType<T>(0, scale);
        max = NearestFieldType<T>(0, scale);
        return;
    }

    T cur_min = data[0];
    T cur_max = data[0];

    for (const T& x : data) {
        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = NearestFieldType<T>(cur_min, scale);
    max = NearestFieldType<T>(cur_max, scale);
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128>;
} // namespace doris::vectorized
