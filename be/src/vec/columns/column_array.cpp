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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnArray.cpp
// and modified by Doris

#include "vec/columns/column_array.h"

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstring>
#include <vector>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "util/simd/bits.h"
#include "util/simd/vstring_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/typeid_cast.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"

class SipHash;

namespace doris::vectorized {

ColumnArray::ColumnArray(MutableColumnPtr&& nested_column, MutableColumnPtr&& offsets_column)
        : data(std::move(nested_column)), offsets(std::move(offsets_column)) {
    // TODO(lihangyu) : we need to check the nullable attribute of array's data column.
    // but currently ColumnMap<ColumnString, ColumnString> is used to store sparse data of variant type,
    // so I temporarily disable this check.
    // #ifndef BE_TEST
    //     // This is a known problem.
    //     // We often do not consider the nullable attribute of array's data column in beut.
    //     // Considering that beut is just a test, it will not be checked at present, but this problem needs to be considered in the future.
    //     if (!data->is_nullable() && check_nullable) {
    //         throw doris::Exception(ErrorCode::INTERNAL_ERROR,
    //                                "nested_column must be nullable, but got {}", data->get_name());
    //     }
    // #endif

    data = data->convert_to_full_column_if_const();
    offsets = offsets->convert_to_full_column_if_const();
    const auto* offsets_concrete = typeid_cast<const ColumnOffsets*>(offsets.get());

    if (!offsets_concrete) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "offsets_column must be a ColumnUInt64");
        __builtin_unreachable();
    }

    if (!offsets_concrete->empty() && data) {
        auto last_offset = offsets_concrete->get_data().back();

        /// This will also prevent possible overflow in offset.
        if (data->size() != last_offset) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "nested_column's size {}, is not consistent with offsets_column's {}",
                    data->size(), last_offset);
        }
    }

    /** NOTE
      * Arrays with constant value are possible and used in implementation of higher order functions (see FunctionReplicate).
      * But in most cases, arrays with constant value are unexpected and code will work wrong. Use with caution.
      */
}

ColumnArray::ColumnArray(MutableColumnPtr&& nested_column) : data(std::move(nested_column)) {
    data = data->convert_to_full_column_if_const();
    if (!data->empty()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Not empty data passed to ColumnArray, but no offsets passed");
        __builtin_unreachable();
    }
    offsets = ColumnOffsets::create();
}

void ColumnArray::shrink_padding_chars() {
    data->shrink_padding_chars();
}

std::string ColumnArray::get_name() const {
    return "Array(" + get_data().get_name() + ")";
}

MutableColumnPtr ColumnArray::clone_resized(size_t to_size) const {
    auto res = ColumnArray::create(get_data().clone_empty());

    if (to_size == 0) return res;
    size_t from_size = size();

    if (to_size <= from_size) {
        /// Just cut column.
        res->get_offsets().assign(get_offsets().begin(), get_offsets().begin() + to_size);
        res->get_data().insert_range_from(get_data(), 0, get_offsets()[to_size - 1]);
    } else {
        /// Copy column and append empty arrays for extra elements.
        Offset64 offset = 0;
        if (from_size > 0) {
            res->get_offsets().assign(get_offsets().begin(), get_offsets().end());
            res->get_data().insert_range_from(get_data(), 0, get_data().size());
            offset = get_offsets().back();
        }

        res->get_offsets().resize(to_size);
        for (size_t i = from_size; i < to_size; ++i) res->get_offsets()[i] = offset;
    }

    return res;
}

size_t ColumnArray::size() const {
    return get_offsets().size();
}

Field ColumnArray::operator[](size_t n) const {
    size_t offset = offset_at(n);
    size_t size = size_at(n);

    if (size > max_array_size_as_field)
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Array of size {} in row {}, is too large to be manipulated as "
                               "single field, maximum size {}",
                               size, n, max_array_size_as_field);

    Array res(size);

    for (size_t i = 0; i < size; ++i) res[i] = get_data()[offset + i];

    return Field::create_field<TYPE_ARRAY>(res);
}

void ColumnArray::get(size_t n, Field& res) const {
    size_t offset = offset_at(n);
    size_t size = size_at(n);

    if (size > max_array_size_as_field)
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Array of size {} in row {}, is too large to be manipulated as "
                               "single field, maximum size {}",
                               size, n, max_array_size_as_field);

    res = Field::create_field<TYPE_ARRAY>(Array(size));
    Array& res_arr = doris::vectorized::get<Array&>(res);

    for (size_t i = 0; i < size; ++i) get_data().get(offset + i, res_arr[i]);
}

bool ColumnArray::is_default_at(size_t n) const {
    const auto& offsets_data = get_offsets();
    return offsets_data[n] == offsets_data[static_cast<ssize_t>(n) - 1];
}

size_t ColumnArray::serialize_size_at(size_t row) const {
    size_t array_size = size_at(row);
    size_t offset = offset_at(row);

    size_t sz = 0;

    for (size_t i = 0; i < array_size; ++i) {
        sz += get_data().serialize_size_at(offset + i);
    }

    return sz + sizeof(size_t);
}

size_t ColumnArray::serialize_impl(char* pos, const size_t row) const {
    size_t array_size = size_at(row);
    size_t offset = offset_at(row);

    memcpy_fixed<size_t>(pos, (char*)&array_size);

    size_t sz = sizeof(array_size);

    for (size_t i = 0; i < array_size; ++i) {
        sz += get_data().serialize_impl(pos + sz, offset + i);
    }

    DCHECK_EQ(sz, serialize_size_at(row));
    return sz;
}

StringRef ColumnArray::serialize_value_into_arena(size_t n, Arena& arena,
                                                  char const*& begin) const {
    char* pos = arena.alloc_continue(serialize_size_at(n), begin);
    return {pos, serialize_impl(pos, n)};
}

template <bool positive>
struct ColumnArray::less {
    const ColumnArray& parent;
    const int nan_direction_hint;
    explicit less(const ColumnArray& parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        size_t lhs_size = parent.size_at(lhs);
        size_t rhs_size = parent.size_at(rhs);
        size_t min_size = std::min(lhs_size, rhs_size);
        int res = 0;
        for (size_t i = 0; i < min_size; ++i) {
            if (res = parent.get_data().compare_at(
                        parent.offset_at(lhs) + i, parent.offset_at(rhs) + i,
                        *parent.get_data_ptr().get(), nan_direction_hint);
                res) {
                // if res != 0 , here is something different ,just return
                break;
            }
        }
        if (res == 0) {
            // then we check size of array
            res = lhs_size < rhs_size ? -1 : (lhs_size == rhs_size ? 0 : 1);
        }

        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnArray::get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                  IColumn::Permutation& res) const {
    size_t s = size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }

    if (reverse) {
        pdqsort(res.begin(), res.end(), ColumnArray::less<false>(*this, nan_direction_hint));
    } else {
        pdqsort(res.begin(), res.end(), ColumnArray::less<true>(*this, nan_direction_hint));
    }
}

void ColumnArray::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                              IColumn::Permutation& perms, EqualRange& range,
                              bool last_column) const {
    sorter->sort_column(static_cast<const ColumnArray&>(*this), flags, perms, range, last_column);
}

int ColumnArray::compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const {
    // since column type is complex, we can't use this function
    const auto& rhs = assert_cast<const ColumnArray&, TypeCheckOnRelease::DISABLE>(rhs_);

    size_t lhs_size = size_at(n);
    size_t rhs_size = rhs.size_at(m);
    size_t min_size = std::min(lhs_size, rhs_size);
    for (size_t i = 0; i < min_size; ++i) {
        if (int res = get_data().compare_at(offset_at(n) + i, rhs.offset_at(m) + i, *rhs.data.get(),
                                            nan_direction_hint);
            res) {
            // if res != 0 , here is something different ,just return
            return res;
        }
    }

    // then we check size of array
    return lhs_size < rhs_size ? -1 : (lhs_size == rhs_size ? 0 : 1);
}

size_t ColumnArray::get_max_row_byte_size() const {
    size_t max_size = 0;
    size_t num_rows = size();
    auto sz = data->get_max_row_byte_size();
    for (size_t i = 0; i < num_rows; ++i) {
        max_size = std::max(max_size, size_at(i) * sz);
    }

    return sizeof(size_t) + max_size;
}

void ColumnArray::serialize_vec(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        // Used in hash_map_context.h, this address is allocated via Arena,
        // but passed through StringRef, so using const_cast is acceptable.
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

size_t ColumnArray::deserialize_impl(const char* pos) {
    size_t sz = 0;
    size_t array_size = unaligned_load<size_t>(pos);
    sz += sizeof(size_t);
    for (size_t j = 0; j < array_size; j++) {
        sz += get_data().deserialize_impl(pos + sz);
    }
    get_offsets().push_back(get_offsets().back() + array_size);
    return sz;
}

void ColumnArray::deserialize_vec(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

const char* ColumnArray::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

void ColumnArray::update_hash_with_value(size_t n, SipHash& hash) const {
    size_t array_size = size_at(n);
    size_t offset = offset_at(n);

    for (size_t i = 0; i < array_size; ++i) get_data().update_hash_with_value(offset + i, hash);
}

// for every array row calculate xxHash
void ColumnArray::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                           const uint8_t* __restrict null_data) const {
    auto& offsets_column = get_offsets();
    if (null_data) {
        for (size_t i = start; i < end; ++i) {
            if (null_data[i] == 0) {
                size_t elem_size = offsets_column[i] - offsets_column[i - 1];
                if (elem_size == 0) {
                    hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&elem_size),
                                                      sizeof(elem_size), hash);
                } else {
                    get_data().update_xxHash_with_value(offsets_column[i - 1], offsets_column[i],
                                                        hash, nullptr);
                }
            }
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            size_t elem_size = offsets_column[i] - offsets_column[i - 1];
            if (elem_size == 0) {
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&elem_size),
                                                  sizeof(elem_size), hash);
            } else {
                get_data().update_xxHash_with_value(offsets_column[i - 1], offsets_column[i], hash,
                                                    nullptr);
            }
        }
    }
}

// for every array row calculate crcHash
void ColumnArray::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                        const uint8_t* __restrict null_data) const {
    auto& offsets_column = get_offsets();
    if (null_data) {
        for (size_t i = start; i < end; ++i) {
            if (null_data[i] == 0) {
                size_t elem_size = offsets_column[i] - offsets_column[i - 1];
                if (elem_size == 0) {
                    hash = HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(&elem_size),
                                                   sizeof(elem_size), hash);
                } else {
                    get_data().update_crc_with_value(offsets_column[i - 1], offsets_column[i], hash,
                                                     nullptr);
                }
            }
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            size_t elem_size = offsets_column[i] - offsets_column[i - 1];
            if (elem_size == 0) {
                hash = HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(&elem_size),
                                               sizeof(elem_size), hash);
            } else {
                get_data().update_crc_with_value(offsets_column[i - 1], offsets_column[i], hash,
                                                 nullptr);
            }
        }
    }
}

void ColumnArray::update_hashes_with_value(uint64_t* __restrict hashes,
                                           const uint8_t* __restrict null_data) const {
    auto s = size();
    if (null_data) {
        for (size_t i = 0; i < s; ++i) {
            if (null_data[i] == 0) {
                update_xxHash_with_value(i, i + 1, hashes[i], nullptr);
            }
        }
    } else {
        for (size_t i = 0; i < s; ++i) {
            update_xxHash_with_value(i, i + 1, hashes[i], nullptr);
        }
    }
}

void ColumnArray::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type,
                                         uint32_t rows, uint32_t offset,
                                         const uint8_t* __restrict null_data) const {
    auto s = rows;
    DCHECK(s == size());

    if (null_data) {
        for (size_t i = 0; i < s; ++i) {
            // every row
            if (null_data[i] == 0) {
                update_crc_with_value(i, i + 1, hash[i], nullptr);
            }
        }
    } else {
        for (size_t i = 0; i < s; ++i) {
            update_crc_with_value(i, i + 1, hash[i], nullptr);
        }
    }
}

void ColumnArray::insert(const Field& x) {
    DCHECK_EQ(x.get_type(), PrimitiveType::TYPE_ARRAY);
    if (x.is_null()) {
        get_data().insert(Field::create_field<TYPE_NULL>(Null()));
        get_offsets().push_back(get_offsets().back() + 1);
    } else {
        const auto& array = doris::vectorized::get<const Array&>(x);
        size_t size = array.size();
        for (size_t i = 0; i < size; ++i) {
            get_data().insert(array[i]);
        }
        get_offsets().push_back(get_offsets().back() + size);
    }
}

void ColumnArray::insert_from(const IColumn& src_, size_t n) {
    DCHECK_LT(n, src_.size());
    const ColumnArray& src = assert_cast<const ColumnArray&>(src_);
    size_t size = src.size_at(n);
    size_t offset = src.offset_at(n);

    if ((!get_data().is_nullable() && src.get_data().is_nullable()) ||
        (get_data().is_nullable() && !src.get_data().is_nullable())) {
        // Note: we can't process the case of 'Array(Nullable(nest))' or 'Array(NotNullable(nest))'
        throw Exception(ErrorCode::INTERNAL_ERROR, "insert '{}' into '{}'", src.get_name(),
                        get_name());
    } else {
        get_data().insert_range_from(src.get_data(), offset, size);
    }
    get_offsets().push_back(get_offsets().back() + size);
}

void ColumnArray::insert_default() {
    /// NOTE 1: We can use back() even if the array is empty (due to zero -1th element in PODArray).
    /// NOTE 2: We cannot use reference in push_back, because reference get invalidated if array is reallocated.
    auto last_offset = get_offsets().back();
    get_offsets().push_back(last_offset);
}

void ColumnArray::pop_back(size_t n) {
    auto& offsets_data = get_offsets();
    DCHECK(n <= offsets_data.size()) << " n:" << n << " with offsets size: " << offsets_data.size();
    size_t nested_n = offsets_data.back() - offset_at(offsets_data.size() - n);
    if (nested_n) get_data().pop_back(nested_n);
    offsets_data.resize_assume_reserved(offsets_data.size() - n);
}

void ColumnArray::reserve(size_t n) {
    get_offsets().reserve(n);
    get_data().reserve(
            n); /// The average size of arrays is not taken into account here. Or it is considered to be no more than 1.
}

//please check you real need size in data column, because it's maybe need greater size when data is string column
void ColumnArray::resize(size_t n) {
    auto last_off = get_offsets().back();
    get_offsets().resize_fill(n, last_off);
    // make new size of data column
    get_data().resize(get_offsets().back());
}

size_t ColumnArray::byte_size() const {
    return get_data().byte_size() + get_offsets().size() * sizeof(get_offsets()[0]);
}

size_t ColumnArray::allocated_bytes() const {
    return get_data().allocated_bytes() + get_offsets().allocated_bytes();
}

bool ColumnArray::has_enough_capacity(const IColumn& src) const {
    const auto& src_concrete = assert_cast<const ColumnArray&>(src);
    return get_data().has_enough_capacity(src_concrete.get_data()) &&
           get_offsets_column().has_enough_capacity(src_concrete.get_offsets_column());
}

bool ColumnArray::has_equal_offsets(const ColumnArray& other) const {
    const Offsets64& offsets1 = get_offsets();
    const Offsets64& offsets2 = other.get_offsets();
    return offsets1.size() == offsets2.size() &&
           (offsets1.empty() ||
            0 == memcmp(offsets1.data(), offsets2.data(), sizeof(offsets1[0]) * offsets1.size()));
}

void ColumnArray::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) return;

    const ColumnArray& src_concrete = assert_cast<const ColumnArray&>(src);

    if (start + length > src_concrete.get_offsets().size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameter out of bound in ColumnArray::insert_range_from method. "
                               "[start({}) + length({}) > offsets.size({})]",
                               std::to_string(start), std::to_string(length),
                               std::to_string(src_concrete.get_offsets().size()));
    }

    size_t nested_offset = src_concrete.offset_at(start);
    size_t nested_length = src_concrete.get_offsets()[start + length - 1] - nested_offset;

    get_data().insert_range_from(src_concrete.get_data(), nested_offset, nested_length);

    auto& cur_offsets = get_offsets();
    const auto& src_offsets = src_concrete.get_offsets();

    if (start == 0 && cur_offsets.empty()) {
        cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
    } else {
        size_t old_size = cur_offsets.size();
        // -1 is ok, because PaddedPODArray pads zeros on the left.
        size_t prev_max_offset = cur_offsets.back();
        cur_offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            cur_offsets[old_size + i] = src_offsets[start + i] - nested_offset + prev_max_offset;
    }
}

void ColumnArray::insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                                    size_t length) {
    const ColumnArray& src_concrete = assert_cast<const ColumnArray&>(src);

    if (start + length > src_concrete.get_offsets().size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameter out of bound in ColumnArray::insert_range_from method. "
                               "[start({}) + length({}) > offsets.size({})]",
                               std::to_string(start), std::to_string(length),
                               std::to_string(src_concrete.get_offsets().size()));
    }

    size_t nested_offset = src_concrete.offset_at(start);
    size_t nested_length = src_concrete.get_offsets()[start + length - 1] - nested_offset;

    get_data().insert_range_from_ignore_overflow(src_concrete.get_data(), nested_offset,
                                                 nested_length);

    auto& cur_offsets = get_offsets();
    const auto& src_offsets = src_concrete.get_offsets();

    if (start == 0 && cur_offsets.empty()) {
        cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
    } else {
        size_t old_size = cur_offsets.size();
        // -1 is ok, because PaddedPODArray pads zeros on the left.
        size_t prev_max_offset = cur_offsets.back();
        cur_offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            cur_offsets[old_size + i] = src_offsets[start + i] - nested_offset + prev_max_offset;
    }
}

using Offset64 = IColumn::Offset64;
using Offsets64 = IColumn::Offsets64;
using Filter = IColumn::Filter;
using ColumnOffsets = ColumnArray::ColumnOffsets;

struct ColumnArrayDataOffsets {
    ColumnPtr data;
    ColumnOffsets::Ptr offsets;
};

template <PrimitiveType T>
ColumnArrayDataOffsets filter_number_return_new(const Filter& filt, ssize_t result_size_hint,
                                                const ColumnPtr& src_data,
                                                const ColumnOffsets* src_offsets) {
    if (src_offsets->empty()) {
        return ColumnArrayDataOffsets {.data = src_data->clone_empty(),
                                       .offsets = ColumnOffsets::create()};
    }

    auto dst_data = src_data->clone_empty();
    auto dst_offset = ColumnOffsets::create();

    auto& res_elems = assert_cast<ColumnVector<T>&>(*dst_data).get_data();
    auto& res_offsets = dst_offset->get_data();

    filter_arrays_impl<typename PrimitiveTypeTraits<T>::ColumnItemType, IColumn::Offset64>(
            assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*src_data).get_data(),
            src_offsets->get_data(), res_elems, res_offsets, filt, result_size_hint);

    return ColumnArrayDataOffsets {.data = std::move(dst_data), .offsets = std::move(dst_offset)};
}

template <PrimitiveType T>
size_t filter_number_inplace(const Filter& filter, IColumn& src_data, ColumnOffsets& src_offsets) {
    return filter_arrays_impl<typename PrimitiveTypeTraits<T>::ColumnItemType, Offset64>(
            assert_cast<ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(src_data).get_data(),
            src_offsets.get_data(), filter);
}

ColumnArrayDataOffsets filter_string_return_new(const Filter& filt, ssize_t result_size_hint,
                                                const ColumnPtr& src_data,
                                                const ColumnOffsets* src_offsets) {
    size_t col_size = src_offsets->size();
    column_match_filter_size(col_size, filt.size());

    if (col_size == 0) {
        return ColumnArrayDataOffsets {.data = src_data->clone_empty(),
                                       .offsets = ColumnOffsets::create()};
    }

    const auto& src_string = assert_cast<const ColumnString&>(*src_data);
    const ColumnString::Chars& src_chars = src_string.get_chars();
    const auto& src_string_offsets = src_string.get_offsets();
    const auto& offsets = src_offsets->get_data();

    auto dst_data = src_data->clone_empty();
    ColumnString::Chars& res_chars = assert_cast<ColumnString&>(*dst_data).get_chars();
    auto& res_string_offsets = assert_cast<ColumnString&>(*dst_data).get_offsets();
    auto dst_offset = ColumnOffsets::create();
    auto& res_offsets = dst_offset->get_data();

    if (result_size_hint < 0) {
        res_chars.reserve(src_chars.size());
        res_string_offsets.reserve(src_string_offsets.size());
        res_offsets.reserve(col_size);
    }

    Offset64 prev_src_offset = 0;
    IColumn::Offset prev_src_string_offset = 0;

    Offset64 prev_res_offset = 0;
    IColumn::Offset prev_res_string_offset = 0;

    for (size_t i = 0; i < col_size; ++i) {
        /// Number of rows in the array.
        size_t array_size = offsets[i] - prev_src_offset;

        if (filt[i]) {
            /// If the array is not empty - copy content.
            if (array_size) {
                size_t chars_to_copy = src_string_offsets[array_size + prev_src_offset - 1] -
                                       prev_src_string_offset;
                size_t res_chars_prev_size = res_chars.size();
                res_chars.resize(res_chars_prev_size + chars_to_copy);
                memcpy(&res_chars[res_chars_prev_size], &src_chars[prev_src_string_offset],
                       chars_to_copy);

                for (size_t j = 0; j < array_size; ++j) {
                    res_string_offsets.push_back(src_string_offsets[j + prev_src_offset] +
                                                 prev_res_string_offset - prev_src_string_offset);
                }

                prev_res_string_offset = res_string_offsets.back();
            }

            prev_res_offset += array_size;
            res_offsets.push_back(prev_res_offset);
        }

        if (array_size) {
            prev_src_offset += array_size;
            prev_src_string_offset = src_string_offsets[prev_src_offset - 1];
        }
    }

    return ColumnArrayDataOffsets {.data = std::move(dst_data), .offsets = std::move(dst_offset)};
}

ColumnArrayDataOffsets filter_generic_return_new(const Filter& filt, ssize_t result_size_hint,
                                                 const ColumnPtr& src_data,
                                                 const ColumnOffsets* src_offsets) {
    size_t size = src_offsets->size();
    column_match_filter_size(size, filt.size());

    if (size == 0) {
        return ColumnArrayDataOffsets {.data = src_data->clone_empty(),
                                       .offsets = ColumnOffsets::create()};
    }

    const auto& offsets = src_offsets->get_data();
    Filter nested_filt(offsets.back());
    ssize_t nested_result_size_hint = 0;
    for (size_t i = 0; i < size; ++i) {
        const auto offset_at = offsets[i - 1];
        const auto size_at = offsets[i] - offsets[i - 1];
        if (filt[i]) {
            memset(&nested_filt[offset_at], 1, size_at);
            nested_result_size_hint += size_at;
        } else {
            memset(&nested_filt[offset_at], 0, size_at);
        }
    }

    ColumnPtr dst_data = src_data->filter(nested_filt, nested_result_size_hint);
    auto dst_offsets = ColumnOffsets::create();
    auto& res_offsets = dst_offsets->get_data();

    if (result_size_hint) {
        res_offsets.reserve(result_size_hint > 0 ? result_size_hint : size);
    }

    size_t current_offset = 0;
    for (size_t i = 0; i < size; ++i) {
        if (filt[i]) {
            current_offset += offsets[i] - offsets[i - 1];
            res_offsets.push_back(current_offset);
        }
    }
    return ColumnArrayDataOffsets {.data = dst_data, .offsets = std::move(dst_offsets)};
}

size_t filter_generic_inplace(const Filter& filter, IColumn& src_data, ColumnOffsets& src_offsets) {
    const size_t size = src_offsets.size();
    column_match_filter_size(size, filter.size());

    if (size == 0) {
        return 0;
    }

    auto& offsets = src_offsets.get_data();

    Filter nested_filter(offsets.back());
    for (size_t i = 0; i < size; ++i) {
        const auto offset_at = offsets[i - 1];
        const auto size_at = offsets[i] - offsets[i - 1];
        if (filter[i]) {
            memset(&nested_filter[offset_at], 1, size_at);
        } else {
            memset(&nested_filter[offset_at], 0, size_at);
        }
    }

    src_data.filter(nested_filter);
    // Make a new offset to avoid inplace operation
    auto res_offset = ColumnOffsets::create();
    auto& res_offset_data = res_offset->get_data();
    res_offset_data.reserve(size);
    size_t current_offset = 0;
    for (size_t i = 0; i < size; ++i) {
        if (filter[i]) {
            current_offset += offsets[i] - offsets[i - 1];
            res_offset_data.push_back(current_offset);
        }
    }
    offsets.swap(res_offset_data);
    return offsets.size();
}

/// TODO: We need to consider reconstructing here.
// The distribution type here should not be enumerated in this way.
// We can consider adding a function to return type to the column.
ColumnArrayDataOffsets filter_return_new_dispatch(const Filter& filt, ssize_t result_size_hint,
                                                  const ColumnPtr& data,
                                                  const ColumnOffsets* offsets) {
    if (typeid_cast<const ColumnUInt8*>(data.get()))
        return filter_number_return_new<TYPE_BOOLEAN>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnInt8*>(data.get()))
        return filter_number_return_new<TYPE_TINYINT>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnInt16*>(data.get()))
        return filter_number_return_new<TYPE_SMALLINT>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnInt32*>(data.get()))
        return filter_number_return_new<TYPE_INT>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnInt64*>(data.get()))
        return filter_number_return_new<TYPE_BIGINT>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnFloat32*>(data.get()))
        return filter_number_return_new<TYPE_FLOAT>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnFloat64*>(data.get()))
        return filter_number_return_new<TYPE_DOUBLE>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnDate*>(data.get()))
        return filter_number_return_new<TYPE_DATE>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnDateV2*>(data.get()))
        return filter_number_return_new<TYPE_DATEV2>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnDateTime*>(data.get()))
        return filter_number_return_new<TYPE_DATETIME>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnDateTimeV2*>(data.get()))
        return filter_number_return_new<TYPE_DATETIMEV2>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnTimeV2*>(data.get()))
        return filter_number_return_new<TYPE_TIMEV2>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnTime*>(data.get()))
        return filter_number_return_new<TYPE_TIME>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnIPv4*>(data.get()))
        return filter_number_return_new<TYPE_IPV4>(filt, result_size_hint, data, offsets);
    if (typeid_cast<const ColumnString*>(data.get()))
        return filter_string_return_new(filt, result_size_hint, data, offsets);
    return filter_generic_return_new(filt, result_size_hint, data, offsets);
}

ColumnPtr ColumnArray::filter(const Filter& filt, ssize_t result_size_hint) const {
    // return empty
    if (this->empty()) {
        return ColumnArray::create(data);
    }

    if (const auto* nullable_data_column = check_and_get_column<ColumnNullable>(*data)) {
        auto res_null_map = ColumnUInt8::create();
        // filter null map
        filter_arrays_impl_only_data(nullable_data_column->get_null_map_data(), get_offsets(),
                                     res_null_map->get_data(), filt, result_size_hint);

        auto src_data = nullable_data_column->get_nested_column_ptr();
        const auto* src_offsets = assert_cast<const ColumnOffsets*>(offsets.get());
        auto array_of_nested =
                filter_return_new_dispatch(filt, result_size_hint, src_data, src_offsets);

        return ColumnArray::create(
                ColumnNullable::create(array_of_nested.data, std::move(res_null_map)),
                array_of_nested.offsets);
    } else {
        // filter offsets
        const auto* src_offsets = assert_cast<const ColumnOffsets*>(offsets.get());
        auto array_of_nested =
                filter_return_new_dispatch(filt, result_size_hint, data, src_offsets);
        return ColumnArray::create(array_of_nested.data, std::move(array_of_nested.offsets));
    }
}

// There is a strange thing here. It didn't implement the type of string?
// In order not to destroy the original logic, the original logic is still retained here.
size_t filter_inplace_dispatch(const Filter& filter, IColumn& src_data,
                               ColumnOffsets& src_offsets) {
    if (typeid_cast<ColumnUInt8*>(&src_data))
        return filter_number_inplace<TYPE_BOOLEAN>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnInt8*>(&src_data))
        return filter_number_inplace<TYPE_TINYINT>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnInt16*>(&src_data))
        return filter_number_inplace<TYPE_SMALLINT>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnInt32*>(&src_data))
        return filter_number_inplace<TYPE_INT>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnInt64*>(&src_data))
        return filter_number_inplace<TYPE_BIGINT>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnFloat32*>(&src_data))
        return filter_number_inplace<TYPE_FLOAT>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnFloat64*>(&src_data))
        return filter_number_inplace<TYPE_DOUBLE>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnDate*>(&src_data))
        return filter_number_inplace<TYPE_DATE>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnDateV2*>(&src_data))
        return filter_number_inplace<TYPE_DATEV2>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnDateTime*>(&src_data))
        return filter_number_inplace<TYPE_DATETIME>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnDateTimeV2*>(&src_data))
        return filter_number_inplace<TYPE_DATETIMEV2>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnTimeV2*>(&src_data))
        return filter_number_inplace<TYPE_TIMEV2>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnTime*>(&src_data))
        return filter_number_inplace<TYPE_TIME>(filter, src_data, src_offsets);
    if (typeid_cast<ColumnIPv4*>(&src_data))
        return filter_number_inplace<TYPE_IPV4>(filter, src_data, src_offsets);
    return filter_generic_inplace(filter, src_data, src_offsets);
}

size_t ColumnArray::filter(const Filter& filter) {
    if (this->empty()) {
        return 0;
    }

    if (auto* nullable_data_column = typeid_cast<ColumnNullable*>(data.get())) {
        const auto result_size = filter_arrays_impl_only_data(
                nullable_data_column->get_null_map_data(), get_offsets(), filter);

        auto& src_data = nullable_data_column->get_nested_column();
        auto& src_offsets = assert_cast<ColumnOffsets&>(*offsets);
        filter_inplace_dispatch(filter, src_data, src_offsets);
        return result_size;
    } else {
        auto& src_data = get_data();
        auto& src_offsets = assert_cast<ColumnOffsets&>(*offsets);

        return filter_inplace_dispatch(filter, src_data, src_offsets);
    }
}

void ColumnArray::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                      const uint32_t* indices_end) {
    for (const auto* x = indices_begin; x != indices_end; ++x) {
        ColumnArray::insert_from(src, *x);
    }
}

void ColumnArray::insert_many_from(const IColumn& src, size_t position, size_t length) {
    for (auto x = 0; x != length; ++x) {
        ColumnArray::insert_from(src, position);
    }
}

MutableColumnPtr ColumnArray::permute(const Permutation& perm, size_t limit) const {
    size_t size = offsets->size();
    if (limit == 0) {
        limit = size;
    } else {
        limit = std::min(size, limit);
    }
    if (perm.size() < limit) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of permutation is less than required.");
        __builtin_unreachable();
    }
    if (limit == 0) {
        return ColumnArray::create(data);
    }

    auto res = ColumnArray::create(data->clone_empty());
    auto& res_offsets = res->get_offsets();
    res_offsets.resize(limit);

    Permutation nested_perm;
    nested_perm.reserve(data->size());

    for (size_t i = 0; i < limit; ++i) {
        res_offsets[i] = res_offsets[i - 1] + size_at(perm[i]);
        for (size_t j = 0; j < size_at(perm[i]); ++j) {
            nested_perm.push_back(offset_at(perm[i]) + j);
        }
    }
    if (nested_perm.size() != 0) {
        res->data = data->permute(nested_perm, nested_perm.size());
    }
    return res;
}

void ColumnArray::erase(size_t start, size_t length) {
    if (start >= size() || length == 0) {
        return;
    }
    length = std::min(length, size() - start);

    const auto& offsets_data = get_offsets();
    auto data_start = offsets_data[start - 1];
    auto data_end = offsets_data[start + length - 1];
    auto data_length = data_end - data_start;
    data->erase(data_start, data_length);
    offsets->erase(start, length);

    for (auto i = start; i < size(); ++i) {
        get_offsets()[i] -= data_length;
    }
}

void ColumnArray::replace_float_special_values() {
    get_data().replace_float_special_values();
}

} // namespace doris::vectorized
