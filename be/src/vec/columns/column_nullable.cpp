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

#include "vec/columns/column_const.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/nan_utils.h"
#include "vec/common/sip_hash.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

ColumnNullable::ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_)
        : nested_column(std::move(nested_column_)), null_map(std::move(null_map_)) {
    /// ColumnNullable cannot have constant nested column. But constant argument could be passed. Materialize it.
    nested_column = get_nested_column().convert_to_full_column_if_const();

    if (!get_nested_column().can_be_inside_nullable()) {
        LOG(FATAL) << get_nested_column().get_name() << " cannot be inside Nullable column";
    }

    if (is_column_const(*null_map)) {
        LOG(FATAL) << "ColumnNullable cannot have constant null map";
    }
}

void ColumnNullable::update_hash_with_value(size_t n, SipHash& hash) const {
    if (is_null_at(n))
        hash.update(0);
    else
        get_nested_column().update_hash_with_value(n, hash);
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
        get_null_map_data().push_back(1);
    } else {
        get_nested_column().insert_data(pos, length);
        get_null_map_data().push_back(0);
    }
}

StringRef ColumnNullable::serialize_value_into_arena(size_t n, Arena& arena,
                                                     char const*& begin) const {
    const auto& arr = get_null_map_data();
    static constexpr auto s = sizeof(arr[0]);

    auto pos = arena.alloc_continue(s, begin);
    // Value of `NULL` may be 1 or JOIN_NULL_HINT, we serialize both to 1.
    // Because we need same key for both `NULL` values while processing `group by`.
    UInt8* val = reinterpret_cast<UInt8*>(pos);
    *val = (arr[n] ? 1 : 0);

    if (arr[n]) return StringRef(pos, s);

    auto nested_ref = get_nested_column().serialize_value_into_arena(n, arena, begin);

    /// serialize_value_into_arena may reallocate memory. Have to use ptr from nested_ref.data and move it back.
    return StringRef(nested_ref.data - s, nested_ref.size + s);
}

    void ColumnNullable::insert_join_null_data() {
        get_nested_column().insert_default();
        get_null_map_data().push_back(JOIN_NULL_HINT);
    }

const char* ColumnNullable::deserialize_and_insert_from_arena(const char* pos) {
    UInt8 val = *reinterpret_cast<const UInt8*>(pos);
    pos += sizeof(val);

    get_null_map_data().push_back(val);

    if (val == 0)
        pos = get_nested_column().deserialize_and_insert_from_arena(pos);
    else
        get_nested_column().insert_default();

    return pos;
}

void ColumnNullable::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnNullable& nullable_col = assert_cast<const ColumnNullable&>(src);
    get_null_map_column().insert_range_from(*nullable_col.null_map, start, length);
    get_nested_column().insert_range_from(*nullable_col.nested_column, start, length);
}

void ColumnNullable::insert_indices_from(const IColumn& src, const int* indices_begin, const int* indices_end) {
    const ColumnNullable& src_concrete = assert_cast<const ColumnNullable&>(src);
    get_nested_column().insert_indices_from(src_concrete.get_nested_column(), indices_begin, indices_end);
    get_null_map_column().insert_indices_from(src_concrete.get_null_map_column(), indices_begin, indices_end);
}

void ColumnNullable::insert(const Field& x) {
    if (x.is_null()) {
        get_nested_column().insert_default();
        get_null_map_data().push_back(1);
    } else {
        get_nested_column().insert(x);
        get_null_map_data().push_back(0);
    }
}

void ColumnNullable::insert_from(const IColumn& src, size_t n) {
    const ColumnNullable& src_concrete = assert_cast<const ColumnNullable&>(src);
    get_nested_column().insert_from(src_concrete.get_nested_column(), n);
    get_null_map_data().push_back(src_concrete.get_null_map_data()[n]);
}

void ColumnNullable::insert_from_not_nullable(const IColumn& src, size_t n) {
    get_nested_column().insert_from(src, n);
    get_null_map_data().push_back(0);
}

void ColumnNullable::insert_range_from_not_nullable(const IColumn& src, size_t start,
                                                    size_t length) {
    get_nested_column().insert_range_from(src, start, length);
    get_null_map_data().resize_fill(get_null_map_data().size() + length, 0);
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

Status ColumnNullable::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    const ColumnNullable* nullable_col_ptr = reinterpret_cast<const ColumnNullable*>(col_ptr);
    ColumnPtr nest_col_ptr = nullable_col_ptr->nested_column;
    ColumnPtr null_map_ptr = nullable_col_ptr->null_map;
    RETURN_IF_ERROR(get_nested_column().filter_by_selector(sel, sel_size, const_cast<doris::vectorized::IColumn*>(nest_col_ptr.get())));
    RETURN_IF_ERROR(get_null_map_column().filter_by_selector(sel, sel_size, const_cast<doris::vectorized::IColumn*>(null_map_ptr.get())));
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
    get_null_map_data().reserve(n);
}

void ColumnNullable::resize(size_t n) {
    get_nested_column().resize(n);
    get_null_map_data().resize(n);
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

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable) {
    if (is_column_nullable(*column)) return column;

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
    return column;
}

} // namespace doris::vectorized
