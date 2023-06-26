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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnNullable.cpp
// and modified by Doris

#include "vec/columns/column_nullable.h"

#include <string.h>

#include <algorithm>

#include "util/hash_util.hpp"
#include "util/simd/bits.h"
#include "vec/columns/column_const.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/nan_utils.h"
#include "vec/common/sip_hash.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

ColumnNullable::ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_)
        : nested_column(std::move(nested_column_)), null_map(std::move(null_map_)) {
    /// ColumnNullable cannot have constant nested column. But constant argument could be passed. Materialize it.
    nested_column = get_nested_column().convert_to_full_column_if_const();

    // after convert const column to full column, it may be a nullable column
    if (nested_column->is_nullable()) {
        assert_cast<ColumnNullable&>(*nested_column).apply_null_map((const ColumnUInt8&)*null_map);
        null_map = assert_cast<ColumnNullable&>(*nested_column).get_null_map_column_ptr();
        nested_column = assert_cast<ColumnNullable&>(*nested_column).get_nested_column_ptr();
    }

    if (!get_nested_column().can_be_inside_nullable()) {
        LOG(FATAL) << get_nested_column().get_name() << " cannot be inside Nullable column";
    }

    if (is_column_const(*null_map)) {
        LOG(FATAL) << "ColumnNullable cannot have constant null map";
    }
    _need_update_has_null = true;
}

MutableColumnPtr ColumnNullable::get_shrinked_column() {
    return ColumnNullable::create(get_nested_column_ptr()->get_shrinked_column(),
                                  get_null_map_column_ptr());
}

void ColumnNullable::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                              const uint8_t* __restrict null_data) const {
    if (!has_null()) {
        nested_column->update_xxHash_with_value(start, end, hash, nullptr);
    } else {
        auto* __restrict real_null_data =
                assert_cast<const ColumnUInt8&>(*null_map).get_data().data();
        for (int i = start; i < end; ++i) {
            if (real_null_data[i] != 0) {
                hash = HashUtil::xxHash64NullWithSeed(hash);
            }
        }
        nested_column->update_xxHash_with_value(start, end, hash, real_null_data);
    }
}

void ColumnNullable::update_crc_with_value(size_t start, size_t end, uint64_t& hash,
                                           const uint8_t* __restrict null_data) const {
    if (!has_null()) {
        nested_column->update_crc_with_value(start, end, hash, nullptr);
    } else {
        auto* __restrict real_null_data =
                assert_cast<const ColumnUInt8&>(*null_map).get_data().data();
        for (int i = start; i < end; ++i) {
            if (real_null_data[i] != 0) {
                hash = HashUtil::zlib_crc_hash_null(hash);
            }
        }
        nested_column->update_crc_with_value(start, end, hash, real_null_data);
    }
}

void ColumnNullable::update_hash_with_value(size_t n, SipHash& hash) const {
    if (is_null_at(n))
        hash.update(0);
    else
        get_nested_column().update_hash_with_value(n, hash);
}

void ColumnNullable::update_hashes_with_value(std::vector<SipHash>& hashes,
                                              const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto s = hashes.size();
    DCHECK(s == size());
    auto* __restrict real_null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data().data();
    if (!has_null()) {
        nested_column->update_hashes_with_value(hashes, nullptr);
    } else {
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) hashes[i].update(0);
        }
        nested_column->update_hashes_with_value(hashes, real_null_data);
    }
}

void ColumnNullable::update_crcs_with_value(std::vector<uint64_t>& hashes,
                                            doris::PrimitiveType type,
                                            const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto s = hashes.size();
    DCHECK(s == size());
    auto* __restrict real_null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data().data();
    if (!has_null()) {
        nested_column->update_crcs_with_value(hashes, type, nullptr);
    } else {
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) {
                hashes[i] = HashUtil::zlib_crc_hash_null(hashes[i]);
            }
        }
        nested_column->update_crcs_with_value(hashes, type, real_null_data);
    }
}

void ColumnNullable::update_hashes_with_value(uint64_t* __restrict hashes,
                                              const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto s = size();
    auto* __restrict real_null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data().data();
    if (!has_null()) {
        nested_column->update_hashes_with_value(hashes, nullptr);
    } else {
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) hashes[i] = HashUtil::xxHash64NullWithSeed(hashes[i]);
        }
        nested_column->update_hashes_with_value(hashes, real_null_data);
    }
}

MutableColumnPtr ColumnNullable::clone_resized(size_t new_size) const {
    MutableColumnPtr new_nested_col = get_nested_column().clone_resized(new_size);
    auto new_null_map = ColumnUInt8::create();

    if (new_size > 0) {
        new_null_map->get_data().resize(new_size);

        size_t count = std::min(size(), new_size);
        memcpy(new_null_map->get_data().data(), get_null_map_data().data(),
               count * sizeof(get_null_map_data()[0]));

        /// If resizing to bigger one, set all new values to NULLs.
        if (new_size > count) memset(&new_null_map->get_data()[count], 1, new_size - count);
    }

    return ColumnNullable::create(std::move(new_nested_col), std::move(new_null_map));
}

Field ColumnNullable::operator[](size_t n) const {
    return is_null_at(n) ? Null() : get_nested_column()[n];
}

void ColumnNullable::get(size_t n, Field& res) const {
    if (is_null_at(n))
        res = Null();
    else
        get_nested_column().get(n, res);
}

StringRef ColumnNullable::get_data_at(size_t n) const {
    if (is_null_at(n)) {
        return StringRef((const char*)nullptr, 0);
    }
    return get_nested_column().get_data_at(n);
}

void ColumnNullable::insert_data(const char* pos, size_t length) {
    if (pos == nullptr) {
        get_nested_column().insert_default();
        _get_null_map_data().push_back(1);
        _has_null = true;
    } else {
        get_nested_column().insert_data(pos, length);
        _get_null_map_data().push_back(0);
    }
}

void ColumnNullable::insert_many_strings(const StringRef* strings, size_t num) {
    auto& null_map_data = _get_null_map_data();
    for (size_t i = 0; i != num; ++i) {
        if (strings[i].data == nullptr) {
            nested_column->insert_default();
            null_map_data.push_back(1);
            _has_null = true;
        } else {
            nested_column->insert_data(strings[i].data, strings[i].size);
            null_map_data.push_back(0);
        }
    }
}

StringRef ColumnNullable::serialize_value_into_arena(size_t n, Arena& arena,
                                                     char const*& begin) const {
    const auto& arr = get_null_map_data();
    static constexpr auto s = sizeof(arr[0]);

    auto pos = arena.alloc_continue(s, begin);
    memcpy(pos, &arr[n], s);

    if (arr[n]) return StringRef(pos, s);

    auto nested_ref = get_nested_column().serialize_value_into_arena(n, arena, begin);

    /// serialize_value_into_arena may reallocate memory. Have to use ptr from nested_ref.data and move it back.
    return StringRef(nested_ref.data - s, nested_ref.size + s);
}

const char* ColumnNullable::deserialize_and_insert_from_arena(const char* pos) {
    UInt8 val = *reinterpret_cast<const UInt8*>(pos);
    pos += sizeof(val);

    _get_null_map_data().push_back(val);

    if (val == 0) {
        pos = get_nested_column().deserialize_and_insert_from_arena(pos);
    } else {
        get_nested_column().insert_default();
        _has_null = true;
    }

    return pos;
}

size_t ColumnNullable::get_max_row_byte_size() const {
    constexpr auto flag_size = sizeof(NullMap::value_type);
    return flag_size + get_nested_column().get_max_row_byte_size();
}

void ColumnNullable::serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                                   size_t max_row_byte_size) const {
    const auto& arr = get_null_map_data();
    static constexpr auto s = sizeof(arr[0]);
    for (size_t i = 0; i < num_rows; ++i) {
        auto* val = const_cast<char*>(keys[i].data + keys[i].size);
        *val = (arr[i] ? 1 : 0);
        keys[i].size += s;
    }

    get_nested_column().serialize_vec_with_null_map(keys, num_rows, arr.data(), max_row_byte_size);
}

void ColumnNullable::deserialize_vec(std::vector<StringRef>& keys, const size_t num_rows) {
    auto& arr = _get_null_map_data();
    const size_t old_size = arr.size();
    arr.resize(old_size + num_rows);

    _has_null = has_null();
    auto* null_map_data = &arr[old_size];
    for (size_t i = 0; i != num_rows; ++i) {
        UInt8 val = *reinterpret_cast<const UInt8*>(keys[i].data);
        null_map_data[i] = val;
        _has_null |= val;
        keys[i].data += sizeof(val);
        keys[i].size -= sizeof(val);
    }
    get_nested_column().deserialize_vec_with_null_map(keys, num_rows, arr.data());
}

void ColumnNullable::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnNullable& nullable_col = assert_cast<const ColumnNullable&>(src);
    _get_null_map_column().insert_range_from(*nullable_col.null_map, start, length);
    get_nested_column().insert_range_from(*nullable_col.nested_column, start, length);
    auto& src_null_map_data = nullable_col.get_null_map_data();
    _has_null = has_null();
    _has_null |= simd::contain_byte(src_null_map_data.data() + start, length, 1);
}

void ColumnNullable::insert_indices_from(const IColumn& src, const int* indices_begin,
                                         const int* indices_end) {
    const ColumnNullable& src_concrete = assert_cast_safe<const ColumnNullable&>(src);
    get_nested_column().insert_indices_from(src_concrete.get_nested_column(), indices_begin,
                                            indices_end);
    _get_null_map_column().insert_indices_from(src_concrete.get_null_map_column(), indices_begin,
                                               indices_end);
    _need_update_has_null = true;
}

void ColumnNullable::insert(const Field& x) {
    if (x.is_null()) {
        get_nested_column().insert_default();
        _get_null_map_data().push_back(1);
        _has_null = true;
    } else {
        get_nested_column().insert(x);
        _get_null_map_data().push_back(0);
    }
}

void ColumnNullable::insert_from(const IColumn& src, size_t n) {
    const ColumnNullable& src_concrete = assert_cast<const ColumnNullable&>(src);
    get_nested_column().insert_from(src_concrete.get_nested_column(), n);
    auto is_null = src_concrete.get_null_map_data()[n];
    _has_null |= is_null;
    _get_null_map_data().push_back(is_null);
}

void ColumnNullable::insert_from_not_nullable(const IColumn& src, size_t n) {
    get_nested_column().insert_from(src, n);
    _get_null_map_data().push_back(0);
}

void ColumnNullable::insert_range_from_not_nullable(const IColumn& src, size_t start,
                                                    size_t length) {
    get_nested_column().insert_range_from(src, start, length);
    _get_null_map_data().resize_fill(_get_null_map_data().size() + length, 0);
}

void ColumnNullable::insert_many_from_not_nullable(const IColumn& src, size_t position,
                                                   size_t length) {
    for (size_t i = 0; i < length; ++i) {
        insert_from_not_nullable(src, position);
    }
}

void ColumnNullable::pop_back(size_t n) {
    get_nested_column().pop_back(n);
    get_null_map_column().pop_back(n);
}

ColumnPtr ColumnNullable::filter(const Filter& filt, ssize_t result_size_hint) const {
    ColumnPtr filtered_data = get_nested_column().filter(filt, result_size_hint);
    ColumnPtr filtered_null_map = get_null_map_column().filter(filt, result_size_hint);
    return ColumnNullable::create(filtered_data, filtered_null_map);
}

size_t ColumnNullable::filter(const Filter& filter) {
    const auto data_result_size = get_nested_column().filter(filter);
    const auto map_result_size = get_null_map_column().filter(filter);
    CHECK_EQ(data_result_size, map_result_size);
    return data_result_size;
}

Status ColumnNullable::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    const ColumnNullable* nullable_col_ptr = reinterpret_cast<const ColumnNullable*>(col_ptr);
    ColumnPtr nest_col_ptr = nullable_col_ptr->nested_column;
    ColumnPtr null_map_ptr = nullable_col_ptr->null_map;
    RETURN_IF_ERROR(get_nested_column().filter_by_selector(
            sel, sel_size, const_cast<doris::vectorized::IColumn*>(nest_col_ptr.get())));
    RETURN_IF_ERROR(get_null_map_column().filter_by_selector(
            sel, sel_size, const_cast<doris::vectorized::IColumn*>(null_map_ptr.get())));
    return Status::OK();
}

ColumnPtr ColumnNullable::permute(const Permutation& perm, size_t limit) const {
    ColumnPtr permuted_data = get_nested_column().permute(perm, limit);
    ColumnPtr permuted_null_map = get_null_map_column().permute(perm, limit);
    return ColumnNullable::create(permuted_data, permuted_null_map);
}

int ColumnNullable::compare_at(size_t n, size_t m, const IColumn& rhs_,
                               int null_direction_hint) const {
    /// NULL values share the properties of NaN values.
    /// Here the last parameter of compare_at is called null_direction_hint
    /// instead of the usual nan_direction_hint and is used to implement
    /// the ordering specified by either NULLS FIRST or NULLS LAST in the
    /// ORDER BY construction.
    const ColumnNullable& nullable_rhs = assert_cast<const ColumnNullable&>(rhs_);

    if (is_null_at(n)) {
        return nullable_rhs.is_null_at(m) ? 0 : null_direction_hint;
    } else if (nullable_rhs.is_null_at(m)) {
        return -null_direction_hint;
    }

    return get_nested_column().compare_at(n, m, nullable_rhs.get_nested_column(),
                                          null_direction_hint);
}

void ColumnNullable::get_permutation(bool reverse, size_t limit, int null_direction_hint,
                                     Permutation& res) const {
    /// Cannot pass limit because of unknown amount of NULLs.
    get_nested_column().get_permutation(reverse, 0, null_direction_hint, res);

    if ((null_direction_hint > 0) != reverse) {
        /// Shift all NULL values to the end.

        size_t read_idx = 0;
        size_t write_idx = 0;
        size_t end_idx = res.size();

        if (!limit)
            limit = end_idx;
        else
            limit = std::min(end_idx, limit);

        while (read_idx < limit && !is_null_at(res[read_idx])) {
            ++read_idx;
            ++write_idx;
        }

        ++read_idx;

        /// Invariants:
        ///  write_idx < read_idx
        ///  write_idx points to NULL
        ///  read_idx will be incremented to position of next not-NULL
        ///  there are range of NULLs between write_idx and read_idx - 1,
        /// We are moving elements from end to begin of this range,
        ///  so range will "bubble" towards the end.
        /// Relative order of NULL elements could be changed,
        ///  but relative order of non-NULLs is preserved.

        while (read_idx < end_idx && write_idx < limit) {
            if (!is_null_at(res[read_idx])) {
                std::swap(res[read_idx], res[write_idx]);
                ++write_idx;
            }
            ++read_idx;
        }
    } else {
        /// Shift all NULL values to the beginning.

        ssize_t read_idx = res.size() - 1;
        ssize_t write_idx = res.size() - 1;

        while (read_idx >= 0 && !is_null_at(res[read_idx])) {
            --read_idx;
            --write_idx;
        }

        --read_idx;

        while (read_idx >= 0 && write_idx >= 0) {
            if (!is_null_at(res[read_idx])) {
                std::swap(res[read_idx], res[write_idx]);
                --write_idx;
            }
            --read_idx;
        }
    }
}
//
//void ColumnNullable::gather(ColumnGathererStream & gatherer)
//{
//    gatherer.gather(*this);
//}

void ColumnNullable::reserve(size_t n) {
    get_nested_column().reserve(n);
    _get_null_map_data().reserve(n);
}

void ColumnNullable::resize(size_t n) {
    auto& null_map_data = get_null_map_data();
    get_nested_column().resize(n);
    null_map_data.resize(n);
}

size_t ColumnNullable::byte_size() const {
    return get_nested_column().byte_size() + get_null_map_column().byte_size();
}

size_t ColumnNullable::allocated_bytes() const {
    return get_nested_column().allocated_bytes() + get_null_map_column().allocated_bytes();
}

void ColumnNullable::protect() {
    get_nested_column().protect();
    get_null_map_column().protect();
}

namespace {

/// The following function implements a slightly more general version
/// of get_extremes() than the implementation from ColumnVector.
/// It takes into account the possible presence of nullable values.
template <typename T>
void getExtremesFromNullableContent(const ColumnVector<T>& col, const NullMap& null_map, Field& min,
                                    Field& max) {
    const auto& data = col.get_data();
    size_t size = data.size();

    if (size == 0) {
        min = Null();
        max = Null();
        return;
    }

    bool has_not_null = false;
    bool has_not_nan = false;

    T cur_min = 0;
    T cur_max = 0;

    for (size_t i = 0; i < size; ++i) {
        const T x = data[i];

        if (null_map[i]) continue;

        if (!has_not_null) {
            cur_min = x;
            cur_max = x;
            has_not_null = true;
            has_not_nan = !is_nan(x);
            continue;
        }

        if (is_nan(x)) continue;

        if (!has_not_nan) {
            cur_min = x;
            cur_max = x;
            has_not_nan = true;
            continue;
        }

        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    if (has_not_null) {
        min = cur_min;
        max = cur_max;
    }
}

} // namespace

void ColumnNullable::get_extremes(Field& min, Field& max) const {
    min = Null();
    max = Null();

    const auto& null_map_data = get_null_map_data();

    if (const auto col_i8 = typeid_cast<const ColumnInt8*>(nested_column.get()))
        getExtremesFromNullableContent<Int8>(*col_i8, null_map_data, min, max);
    else if (const auto col_i16 = typeid_cast<const ColumnInt16*>(nested_column.get()))
        getExtremesFromNullableContent<Int16>(*col_i16, null_map_data, min, max);
    else if (const auto col_i32 = typeid_cast<const ColumnInt32*>(nested_column.get()))
        getExtremesFromNullableContent<Int32>(*col_i32, null_map_data, min, max);
    else if (const auto col_i64 = typeid_cast<const ColumnInt64*>(nested_column.get()))
        getExtremesFromNullableContent<Int64>(*col_i64, null_map_data, min, max);
    else if (const auto col_u8 = typeid_cast<const ColumnUInt8*>(nested_column.get()))
        getExtremesFromNullableContent<UInt8>(*col_u8, null_map_data, min, max);
    else if (const auto col_u16 = typeid_cast<const ColumnUInt16*>(nested_column.get()))
        getExtremesFromNullableContent<UInt16>(*col_u16, null_map_data, min, max);
    else if (const auto col_u32 = typeid_cast<const ColumnUInt32*>(nested_column.get()))
        getExtremesFromNullableContent<UInt32>(*col_u32, null_map_data, min, max);
    else if (const auto col_u64 = typeid_cast<const ColumnUInt64*>(nested_column.get()))
        getExtremesFromNullableContent<UInt64>(*col_u64, null_map_data, min, max);
    else if (const auto col_f32 = typeid_cast<const ColumnFloat32*>(nested_column.get()))
        getExtremesFromNullableContent<Float32>(*col_f32, null_map_data, min, max);
    else if (const auto col_f64 = typeid_cast<const ColumnFloat64*>(nested_column.get()))
        getExtremesFromNullableContent<Float64>(*col_f64, null_map_data, min, max);
}

ColumnPtr ColumnNullable::replicate(const Offsets& offsets) const {
    ColumnPtr replicated_data = get_nested_column().replicate(offsets);
    ColumnPtr replicated_null_map = get_null_map_column().replicate(offsets);
    return ColumnNullable::create(replicated_data, replicated_null_map);
}

void ColumnNullable::replicate(const uint32_t* counts, size_t target_size, IColumn& column) const {
    auto& res = reinterpret_cast<ColumnNullable&>(column);
    get_nested_column().replicate(counts, target_size, res.get_nested_column());
    get_null_map_column().replicate(counts, target_size, res.get_null_map_column());
}

template <bool negative>
void ColumnNullable::apply_null_map_impl(const ColumnUInt8& map) {
    NullMap& arr1 = get_null_map_data();
    const NullMap& arr2 = map.get_data();

    if (arr1.size() != arr2.size()) {
        LOG(FATAL) << "Inconsistent sizes of ColumnNullable objects";
    }

    for (size_t i = 0, size = arr1.size(); i < size; ++i) arr1[i] |= negative ^ arr2[i];
}

void ColumnNullable::apply_null_map(const ColumnUInt8& map) {
    apply_null_map_impl<false>(map);
}

void ColumnNullable::apply_negated_null_map(const ColumnUInt8& map) {
    apply_null_map_impl<true>(map);
}

void ColumnNullable::apply_null_map(const ColumnNullable& other) {
    apply_null_map(other.get_null_map_column());
}

void ColumnNullable::check_consistency() const {
    if (null_map->size() != get_nested_column().size()) {
        LOG(FATAL) << "Logical error: Sizes of nested column and null map of Nullable column are "
                      "not equal";
    }
}

void ColumnNullable::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                 IColumn::Permutation& perms, EqualRange& range,
                                 bool last_column) const {
    sorter->sort_column(static_cast<const ColumnNullable&>(*this), flags, perms, range,
                        last_column);
}

void ColumnNullable::_update_has_null() {
    const UInt8* null_pos = _get_null_map_data().data();
    _has_null = simd::contain_byte(null_pos, _get_null_map_data().size(), 1);
    _need_update_has_null = false;
}

bool ColumnNullable::has_null(size_t size) const {
    if (!_has_null && !_need_update_has_null) {
        return false;
    }
    const UInt8* null_pos = get_null_map_data().data();
    return simd::contain_byte(null_pos, size, 1);
}

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable) {
    if (is_column_nullable(*column) || !column->can_be_inside_nullable()) return column;

    if (is_column_const(*column))
        return ColumnConst::create(
                make_nullable(assert_cast<const ColumnConst&>(*column).get_data_column_ptr(),
                              is_nullable),
                column->size());

    return ColumnNullable::create(column, ColumnUInt8::create(column->size(), is_nullable ? 1 : 0));
}

ColumnPtr remove_nullable(const ColumnPtr& column) {
    if (is_column_nullable(*column)) {
        return reinterpret_cast<const ColumnNullable*>(column.get())->get_nested_column_ptr();
    }

    if (is_column_const(*column)) {
        auto& column_nested = assert_cast<const ColumnConst&>(*column).get_data_column_ptr();
        if (is_column_nullable(*column_nested)) {
            return ColumnConst::create(
                    assert_cast<const ColumnNullable&>(*column_nested).get_nested_column_ptr(),
                    column->size());
        }
    }

    return column;
}

ColumnPtr ColumnNullable::index(const IColumn& indexes, size_t limit) const {
    ColumnPtr indexed_data = get_nested_column().index(indexes, limit);
    ColumnPtr indexed_null_map = get_null_map_column().index(indexes, limit);
    return ColumnNullable::create(indexed_data, indexed_null_map);
}

void check_set_nullable(ColumnPtr& argument_column, ColumnVector<UInt8>::MutablePtr& null_map,
                        bool is_single) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_column)) {
        // Danger: Here must dispose the null map data first! Because
        // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
        // of column nullable mem of null map
        VectorizedUtils::update_null_map(null_map->get_data(), nullable->get_null_map_data(),
                                         is_single);
        argument_column = nullable->get_nested_column_ptr();
    }
}

} // namespace doris::vectorized
