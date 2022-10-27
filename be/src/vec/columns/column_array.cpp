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

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_common.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int NOT_IMPLEMENTED;
extern const int BAD_ARGUMENTS;
extern const int PARAMETER_OUT_OF_BOUND;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
extern const int TOO_LARGE_ARRAY_SIZE;
} // namespace ErrorCodes

/** Obtaining array as Field can be slow for large arrays and consume vast amount of memory.
  * Just don't allow to do it.
  * You can increase the limit if the following query:
  *  SELECT range(10000000)
  * will take less than 500ms on your machine.
  */
static constexpr size_t max_array_size_as_field = 1000000;

ColumnArray::ColumnArray(MutableColumnPtr&& nested_column, MutableColumnPtr&& offsets_column)
        : data(std::move(nested_column)), offsets(std::move(offsets_column)) {
    const ColumnOffsets* offsets_concrete = typeid_cast<const ColumnOffsets*>(offsets.get());

    if (!offsets_concrete) {
        LOG(FATAL) << "offsets_column must be a ColumnUInt64";
    }

    if (!offsets_concrete->empty() && nested_column) {
        auto last_offset = offsets_concrete->get_data().back();

        /// This will also prevent possible overflow in offset.
        if (nested_column->size() != last_offset) {
            LOG(FATAL) << "offsets_column has data inconsistent with nested_column";
        }
    }

    /** NOTE
      * Arrays with constant value are possible and used in implementation of higher order functions (see FunctionReplicate).
      * But in most cases, arrays with constant value are unexpected and code will work wrong. Use with caution.
      */
}

ColumnArray::ColumnArray(MutableColumnPtr&& nested_column) : data(std::move(nested_column)) {
    if (!data->empty()) {
        LOG(FATAL) << "Not empty data passed to ColumnArray, but no offsets passed";
    }

    offsets = ColumnOffsets::create();
}

MutableColumnPtr ColumnArray::get_shrinked_column() {
    return ColumnArray::create(data->get_shrinked_column(), offsets->assume_mutable());
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
        LOG(FATAL) << "Array of size " << size << " is too large to be manipulated as single field,"
                   << "maximum size " << max_array_size_as_field;

    Array res(size);

    for (size_t i = 0; i < size; ++i) res[i] = get_data()[offset + i];

    return res;
}

void ColumnArray::get(size_t n, Field& res) const {
    size_t offset = offset_at(n);
    size_t size = size_at(n);

    if (size > max_array_size_as_field)
        LOG(FATAL) << "Array of size " << size << " is too large to be manipulated as single field,"
                   << " maximum size " << max_array_size_as_field;

    res = Array(size);
    Array& res_arr = doris::vectorized::get<Array&>(res);

    for (size_t i = 0; i < size; ++i) get_data().get(offset + i, res_arr[i]);
}

StringRef ColumnArray::get_data_at(size_t n) const {
    /** Returns the range of memory that covers all elements of the array.
      * Works for arrays of fixed length values.
      * For arrays of strings and arrays of arrays, the resulting chunk of memory may not be one-to-one correspondence with the elements,
      *  since it contains only the data laid in succession, but not the offsets.
      */
    size_t offset_of_first_elem = offset_at(n);
    StringRef first;
    if (offset_of_first_elem < get_data().size()) {
        first = get_data().get_data_at(offset_of_first_elem);
    }

    size_t array_size = size_at(n);
    if (array_size == 0) {
        return StringRef(first.data, 0);
    }

    size_t offset_of_last_elem = offset_at(n + 1) - 1;
    StringRef last = get_data().get_data_at(offset_of_last_elem);

    return StringRef(first.data, last.data + last.size - first.data);
}

bool ColumnArray::is_default_at(size_t n) const {
    const auto& offsets_data = get_offsets();
    return offsets_data[n] == offsets_data[static_cast<ssize_t>(n) - 1];
}

void ColumnArray::insert_data(const char* pos, size_t length) {
    /** Similarly - only for arrays of fixed length values.
      */
    if (!data->is_fixed_and_contiguous())
        LOG(FATAL) << "Method insert_data is not supported for " << get_name();

    size_t field_size = data->size_of_value_if_fixed();

    size_t elems = 0;

    if (length) {
        const char* end = pos + length;
        for (; pos + field_size <= end; pos += field_size, ++elems)
            data->insert_data(pos, field_size);

        if (pos != end)
            LOG(FATAL) << "Incorrect length argument for method ColumnArray::insert_data";
    }

    get_offsets().push_back(get_offsets().back() + elems);
}

StringRef ColumnArray::serialize_value_into_arena(size_t n, Arena& arena,
                                                  char const*& begin) const {
    size_t array_size = size_at(n);
    size_t offset = offset_at(n);

    char* pos = arena.alloc_continue(sizeof(array_size), begin);
    memcpy(pos, &array_size, sizeof(array_size));

    StringRef res(pos, sizeof(array_size));

    for (size_t i = 0; i < array_size; ++i) {
        auto value_ref = get_data().serialize_value_into_arena(offset + i, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    return res;
}

const char* ColumnArray::deserialize_and_insert_from_arena(const char* pos) {
    size_t array_size = unaligned_load<size_t>(pos);
    pos += sizeof(array_size);

    for (size_t i = 0; i < array_size; ++i) pos = get_data().deserialize_and_insert_from_arena(pos);

    get_offsets().push_back(get_offsets().back() + array_size);
    return pos;
}

void ColumnArray::update_hash_with_value(size_t n, SipHash& hash) const {
    size_t array_size = size_at(n);
    size_t offset = offset_at(n);

    for (size_t i = 0; i < array_size; ++i) get_data().update_hash_with_value(offset + i, hash);
}

void ColumnArray::insert(const Field& x) {
    const Array& array = doris::vectorized::get<const Array&>(x);
    size_t size = array.size();
    for (size_t i = 0; i < size; ++i) get_data().insert(array[i]);
    get_offsets().push_back(get_offsets().back() + size);
}

void ColumnArray::insert_from(const IColumn& src_, size_t n) {
    const ColumnArray& src = assert_cast<const ColumnArray&>(src_);
    size_t size = src.size_at(n);
    size_t offset = src.offset_at(n);

    if (!get_data().is_nullable() && src.get_data().is_nullable()) {
        // Note: we can't process the case of 'Array(Nullable(nest))'
        DCHECK(false);
    } else if (get_data().is_nullable() && !src.get_data().is_nullable()) {
        // Note: here we should process the case of 'Array(NotNullable(nest))'
        reinterpret_cast<ColumnNullable*>(&get_data())
                ->insert_range_from_not_nullable(src.get_data(), offset, size);
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
    DCHECK(n <= offsets_data.size());
    size_t nested_n = offsets_data.back() - offset_at(offsets_data.size() - n);
    if (nested_n) get_data().pop_back(nested_n);
    offsets_data.resize_assume_reserved(offsets_data.size() - n);
}

void ColumnArray::reserve(size_t n) {
    get_offsets().reserve(n);
    get_data().reserve(
            n); /// The average size of arrays is not taken into account here. Or it is considered to be no more than 1.
}

size_t ColumnArray::byte_size() const {
    return get_data().byte_size() + get_offsets().size() * sizeof(get_offsets()[0]);
}

size_t ColumnArray::allocated_bytes() const {
    return get_data().allocated_bytes() + get_offsets().allocated_bytes();
}

void ColumnArray::protect() {
    get_data().protect();
    get_offsets().protect();
}

ColumnPtr ColumnArray::convert_to_full_column_if_const() const {
    /// It is possible to have an array with constant data and non-constant offsets.
    /// Example is the result of expression: replicate('hello', [1])
    return ColumnArray::create(data->convert_to_full_column_if_const(), offsets);
}

void ColumnArray::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) return;

    const ColumnArray& src_concrete = assert_cast<const ColumnArray&>(src);

    if (start + length > src_concrete.get_offsets().size())
        LOG(FATAL) << "Parameter out of bound in ColumnArray::insert_range_from method. [start("
                   << std::to_string(start) << ") + length(" << std::to_string(length)
                   << ") > offsets.size(" << std::to_string(src_concrete.get_offsets().size())
                   << ")]";

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

ColumnPtr ColumnArray::filter(const Filter& filt, ssize_t result_size_hint) const {
    if (typeid_cast<const ColumnUInt8*>(data.get()))
        return filter_number<UInt8>(filt, result_size_hint);
    if (typeid_cast<const ColumnUInt16*>(data.get()))
        return filter_number<UInt16>(filt, result_size_hint);
    if (typeid_cast<const ColumnUInt32*>(data.get()))
        return filter_number<UInt32>(filt, result_size_hint);
    if (typeid_cast<const ColumnUInt64*>(data.get()))
        return filter_number<UInt64>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt8*>(data.get()))
        return filter_number<Int8>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt16*>(data.get()))
        return filter_number<Int16>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt32*>(data.get()))
        return filter_number<Int32>(filt, result_size_hint);
    if (typeid_cast<const ColumnInt64*>(data.get()))
        return filter_number<Int64>(filt, result_size_hint);
    if (typeid_cast<const ColumnFloat32*>(data.get()))
        return filter_number<Float32>(filt, result_size_hint);
    if (typeid_cast<const ColumnFloat64*>(data.get()))
        return filter_number<Float64>(filt, result_size_hint);
    if (typeid_cast<const ColumnString*>(data.get())) return filter_string(filt, result_size_hint);
    //if (typeid_cast<const ColumnTuple *>(data.get()))      return filterTuple(filt, result_size_hint);
    if (typeid_cast<const ColumnNullable*>(data.get()))
        return filter_nullable(filt, result_size_hint);
    return filter_generic(filt, result_size_hint);
}

template <typename T>
ColumnPtr ColumnArray::filter_number(const Filter& filt, ssize_t result_size_hint) const {
    if (get_offsets().empty()) return ColumnArray::create(data);

    auto res = ColumnArray::create(data->clone_empty());

    auto& res_elems = assert_cast<ColumnVector<T>&>(res->get_data()).get_data();
    auto& res_offsets = res->get_offsets();

    filter_arrays_impl<T, Offset64>(assert_cast<const ColumnVector<T>&>(*data).get_data(),
                                    get_offsets(), res_elems, res_offsets, filt, result_size_hint);
    return res;
}

ColumnPtr ColumnArray::filter_string(const Filter& filt, ssize_t result_size_hint) const {
    size_t col_size = get_offsets().size();
    if (col_size != filt.size()) LOG(FATAL) << "Size of filter doesn't match size of column.";

    if (0 == col_size) return ColumnArray::create(data);

    auto res = ColumnArray::create(data->clone_empty());

    const ColumnString& src_string = typeid_cast<const ColumnString&>(*data);
    const ColumnString::Chars& src_chars = src_string.get_chars();
    const auto& src_string_offsets = src_string.get_offsets();
    const auto& src_offsets = get_offsets();

    ColumnString::Chars& res_chars = typeid_cast<ColumnString&>(res->get_data()).get_chars();
    auto& res_string_offsets = typeid_cast<ColumnString&>(res->get_data()).get_offsets();
    auto& res_offsets = res->get_offsets();

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
        size_t array_size = src_offsets[i] - prev_src_offset;

        if (filt[i]) {
            /// If the array is not empty - copy content.
            if (array_size) {
                size_t chars_to_copy = src_string_offsets[array_size + prev_src_offset - 1] -
                                       prev_src_string_offset;
                size_t res_chars_prev_size = res_chars.size();
                res_chars.resize(res_chars_prev_size + chars_to_copy);
                memcpy(&res_chars[res_chars_prev_size], &src_chars[prev_src_string_offset],
                       chars_to_copy);

                for (size_t j = 0; j < array_size; ++j)
                    res_string_offsets.push_back(src_string_offsets[j + prev_src_offset] +
                                                 prev_res_string_offset - prev_src_string_offset);

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

    return res;
}

ColumnPtr ColumnArray::filter_generic(const Filter& filt, ssize_t result_size_hint) const {
    size_t size = get_offsets().size();
    if (size != filt.size()) LOG(FATAL) << "Size of filter doesn't match size of column.";

    if (size == 0) return ColumnArray::create(data);

    Filter nested_filt(get_offsets().back());
    for (size_t i = 0; i < size; ++i) {
        if (filt[i])
            memset(&nested_filt[offset_at(i)], 1, size_at(i));
        else
            memset(&nested_filt[offset_at(i)], 0, size_at(i));
    }

    auto res = ColumnArray::create(data->clone_empty());

    ssize_t nested_result_size_hint = 0;
    if (result_size_hint < 0)
        nested_result_size_hint = result_size_hint;
    else if (result_size_hint && result_size_hint < 1000000000 &&
             data->size() < 1000000000) /// Avoid overflow.
        nested_result_size_hint = result_size_hint * data->size() / size;

    res->data = data->filter(nested_filt, nested_result_size_hint);

    auto& res_offsets = res->get_offsets();
    if (result_size_hint) res_offsets.reserve(result_size_hint > 0 ? result_size_hint : size);

    size_t current_offset = 0;
    for (size_t i = 0; i < size; ++i) {
        if (filt[i]) {
            current_offset += size_at(i);
            res_offsets.push_back(current_offset);
        }
    }

    return res;
}

ColumnPtr ColumnArray::filter_nullable(const Filter& filt, ssize_t result_size_hint) const {
    if (get_offsets().empty()) return ColumnArray::create(data);

    const ColumnNullable& nullable_elems = assert_cast<const ColumnNullable&>(*data);

    auto array_of_nested = ColumnArray::create(nullable_elems.get_nested_column_ptr(), offsets);
    auto filtered_array_of_nested_owner = array_of_nested->filter(filt, result_size_hint);
    const auto& filtered_array_of_nested =
            assert_cast<const ColumnArray&>(*filtered_array_of_nested_owner);
    const auto& filtered_offsets = filtered_array_of_nested.get_offsets_ptr();

    auto res_null_map = ColumnUInt8::create();

    filter_arrays_impl_only_data(nullable_elems.get_null_map_data(), get_offsets(),
                                 res_null_map->get_data(), filt, result_size_hint);

    return ColumnArray::create(ColumnNullable::create(filtered_array_of_nested.get_data_ptr(),
                                                      std::move(res_null_map)),
                               filtered_offsets);
}

void ColumnArray::insert_indices_from(const IColumn& src, const int* indices_begin,
                                      const int* indices_end) {
    for (auto x = indices_begin; x != indices_end; ++x) {
        if (*x == -1) {
            ColumnArray::insert_default();
        } else {
            ColumnArray::insert_from(src, *x);
        }
    }
}

Status ColumnArray::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    auto to = reinterpret_cast<vectorized::ColumnArray*>(col_ptr);
    auto& to_offsets = to->get_offsets();

    size_t element_size = 0;
    size_t max_offset = 0;
    for (size_t i = 0; i < sel_size; ++i) {
        element_size += size_at(sel[i]);
        max_offset = std::max(max_offset, offset_at(sel[i]));
    }
    if (max_offset > std::numeric_limits<uint16_t>::max()) {
        return Status::IOError("array elements too large than uint16_t::max");
    }

    to_offsets.reserve(to_offsets.size() + sel_size);
    auto nested_sel = std::make_unique<uint16_t[]>(element_size);
    size_t nested_sel_size = 0;
    for (size_t i = 0; i < sel_size; ++i) {
        auto row_off = offset_at(sel[i]);
        auto row_size = size_at(sel[i]);
        to_offsets.push_back(to_offsets.back() + row_size);
        for (auto j = 0; j < row_size; ++j) {
            nested_sel[nested_sel_size++] = row_off + j;
        }
    }

    if (nested_sel_size > 0) {
        return data->filter_by_selector(nested_sel.get(), nested_sel_size, &to->get_data());
    }
    return Status::OK();
}

ColumnPtr ColumnArray::replicate(const IColumn::Offsets& replicate_offsets) const {
    if (replicate_offsets.empty()) return clone_empty();

    // keep ColumnUInt8 for ColumnNullable::null_map
    if (typeid_cast<const ColumnUInt8*>(data.get()))
        return replicate_number<UInt8>(replicate_offsets);
    if (typeid_cast<const ColumnInt8*>(data.get()))
        return replicate_number<Int8>(replicate_offsets);
    if (typeid_cast<const ColumnInt16*>(data.get()))
        return replicate_number<Int16>(replicate_offsets);
    if (typeid_cast<const ColumnInt32*>(data.get()))
        return replicate_number<Int32>(replicate_offsets);
    if (typeid_cast<const ColumnInt64*>(data.get()))
        return replicate_number<Int64>(replicate_offsets);
    if (typeid_cast<const ColumnFloat32*>(data.get()))
        return replicate_number<Float32>(replicate_offsets);
    if (typeid_cast<const ColumnFloat64*>(data.get()))
        return replicate_number<Float64>(replicate_offsets);
    if (typeid_cast<const ColumnString*>(data.get())) return replicate_string(replicate_offsets);
    if (typeid_cast<const ColumnConst*>(data.get())) return replicate_const(replicate_offsets);
    if (typeid_cast<const ColumnNullable*>(data.get()))
        return replicate_nullable(replicate_offsets);
    return replicate_generic(replicate_offsets);
}

void ColumnArray::replicate(const uint32_t* counts, size_t target_size, IColumn& column,
                            size_t begin, int count_sz) const {
    size_t col_size = count_sz < 0 ? size() : count_sz;
    if (col_size == 0) {
        return;
    }

    IColumn::Offsets replicate_offsets(col_size);
    size_t cur_offset = 0;
    size_t end = begin + col_size;
    for (size_t i = begin; i < end; ++i) {
        cur_offset += counts[i];
        replicate_offsets[i - begin] = cur_offset;
    }
    if (cur_offset != target_size) {
        LOG(WARNING) << "ColumnArray replicate input target_size:" << target_size
                     << " not equal SUM(counts):" << cur_offset;
        return;
    }

    auto rep_res = replicate(replicate_offsets);
    if (!rep_res) {
        LOG(WARNING) << "ColumnArray replicate failed, replicate_offsets count="
                     << replicate_offsets.size() << ", max=" << replicate_offsets.back();
        return;
    }
    auto& rep_res_arr = typeid_cast<const ColumnArray&>(*rep_res);

    ColumnArray& res_arr = typeid_cast<ColumnArray&>(column);
    res_arr.data = rep_res_arr.get_data_ptr();
    res_arr.offsets = rep_res_arr.get_offsets_ptr();
}

template <typename T>
ColumnPtr ColumnArray::replicate_number(const IColumn::Offsets& replicate_offsets) const {
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        LOG(FATAL) << "Size of offsets doesn't match size of column.";

    MutableColumnPtr res = clone_empty();

    if (0 == col_size) return res;

    ColumnArray& res_arr = typeid_cast<ColumnArray&>(*res);

    const typename ColumnVector<T>::Container& src_data =
            typeid_cast<const ColumnVector<T>&>(*data).get_data();
    const auto& src_offsets = get_offsets();

    typename ColumnVector<T>::Container& res_data =
            typeid_cast<ColumnVector<T>&>(res_arr.get_data()).get_data();
    auto& res_offsets = res_arr.get_offsets();

    res_data.reserve(data->size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    IColumn::Offset prev_replicate_offset = 0;
    Offset64 prev_data_offset = 0;
    Offset64 current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i) {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_data_offset;

        for (size_t j = 0; j < size_to_replicate; ++j) {
            current_new_offset += value_size;
            res_offsets.push_back(current_new_offset);

            if (value_size) {
                res_data.resize(res_data.size() + value_size);
                memcpy(&res_data[res_data.size() - value_size], &src_data[prev_data_offset],
                       value_size * sizeof(T));
            }
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_data_offset = src_offsets[i];
    }

    return res;
}

ColumnPtr ColumnArray::replicate_string(const IColumn::Offsets& replicate_offsets) const {
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        LOG(FATAL) << "Size of offsets doesn't match size of column.";

    MutableColumnPtr res = clone_empty();

    if (0 == col_size) return res;

    ColumnArray& res_arr = assert_cast<ColumnArray&>(*res);

    const ColumnString& src_string = typeid_cast<const ColumnString&>(*data);
    const ColumnString::Chars& src_chars = src_string.get_chars();
    const auto& src_string_offsets = src_string.get_offsets();
    const auto& src_offsets = get_offsets();

    ColumnString::Chars& res_chars = typeid_cast<ColumnString&>(res_arr.get_data()).get_chars();
    auto& res_string_offsets = typeid_cast<ColumnString&>(res_arr.get_data()).get_offsets();
    auto& res_offsets = res_arr.get_offsets();

    res_chars.reserve(src_chars.size() / col_size * replicate_offsets.back());
    res_string_offsets.reserve(src_string_offsets.size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    IColumn::Offset prev_replicate_offset = 0;

    Offset64 prev_src_offset = 0;
    IColumn::Offset prev_src_string_offset = 0;

    Offset64 current_res_offset = 0;
    IColumn::Offset current_res_string_offset = 0;

    for (size_t i = 0; i < col_size; ++i) {
        /// How many times to replicate the array.
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        /// The number of strings in the array.
        size_t value_size = src_offsets[i] - prev_src_offset;
        /// Number of characters in strings of the array, including zero bytes.
        size_t sum_chars_size = src_string_offsets[prev_src_offset + value_size - 1] -
                                prev_src_string_offset; /// -1th index is Ok, see PaddedPODArray.

        for (size_t j = 0; j < size_to_replicate; ++j) {
            current_res_offset += value_size;
            res_offsets.push_back(current_res_offset);

            size_t prev_src_string_offset_local = prev_src_string_offset;
            for (size_t k = 0; k < value_size; ++k) {
                /// Size of single string.
                size_t chars_size =
                        src_string_offsets[k + prev_src_offset] - prev_src_string_offset_local;

                current_res_string_offset += chars_size;
                res_string_offsets.push_back(current_res_string_offset);

                prev_src_string_offset_local += chars_size;
            }

            if (sum_chars_size) {
                /// Copies the characters of the array of strings.
                res_chars.resize(res_chars.size() + sum_chars_size);
                memcpy_small_allow_read_write_overflow15(
                        &res_chars[res_chars.size() - sum_chars_size],
                        &src_chars[prev_src_string_offset], sum_chars_size);
            }
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_src_offset = src_offsets[i];
        prev_src_string_offset += sum_chars_size;
    }

    return res;
}

ColumnPtr ColumnArray::replicate_const(const IColumn::Offsets& replicate_offsets) const {
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        LOG(FATAL) << "Size of offsets doesn't match size of column.";

    if (0 == col_size) return clone_empty();

    const auto& src_offsets = get_offsets();

    auto res_column_offsets = ColumnOffsets::create();
    auto& res_offsets = res_column_offsets->get_data();
    res_offsets.reserve(replicate_offsets.back());

    IColumn::Offset prev_replicate_offset = 0;
    Offset64 prev_data_offset = 0;
    Offset64 current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i) {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_data_offset;

        for (size_t j = 0; j < size_to_replicate; ++j) {
            current_new_offset += value_size;
            res_offsets.push_back(current_new_offset);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_data_offset = src_offsets[i];
    }

    return ColumnArray::create(get_data().clone_resized(current_new_offset),
                               std::move(res_column_offsets));
}

ColumnPtr ColumnArray::replicate_generic(const IColumn::Offsets& replicate_offsets) const {
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        LOG(FATAL) << "Size of offsets doesn't match size of column.";

    MutableColumnPtr res = clone_empty();
    ColumnArray& res_concrete = assert_cast<ColumnArray&>(*res);

    if (0 == col_size) return res;

    Offset64 prev_offset = 0;
    for (size_t i = 0; i < col_size; ++i) {
        size_t size_to_replicate = replicate_offsets[i] - prev_offset;
        prev_offset = replicate_offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j) res_concrete.insert_from(*this, i);
    }

    return res;
}

ColumnPtr ColumnArray::replicate_nullable(const IColumn::Offsets& replicate_offsets) const {
    const ColumnNullable& nullable = assert_cast<const ColumnNullable&>(*data);

    /// Make temporary arrays for each components of Nullable. Then replicate them independently and collect back to result.
    /// NOTE Offsets are calculated twice and it is redundant.

    auto array_of_nested = ColumnArray(nullable.get_nested_column_ptr()->assume_mutable(),
                                       get_offsets_ptr()->assume_mutable())
                                   .replicate(replicate_offsets);
    auto array_of_null_map = ColumnArray(nullable.get_null_map_column_ptr()->assume_mutable(),
                                         get_offsets_ptr()->assume_mutable())
                                     .replicate(replicate_offsets);

    return ColumnArray::create(
            ColumnNullable::create(
                    assert_cast<const ColumnArray&>(*array_of_nested).get_data_ptr(),
                    assert_cast<const ColumnArray&>(*array_of_null_map).get_data_ptr()),
            assert_cast<const ColumnArray&>(*array_of_nested).get_offsets_ptr());
}

ColumnPtr ColumnArray::permute(const Permutation& perm, size_t limit) const {
    size_t size = offsets->size();
    if (limit == 0) {
        limit = size;
    } else {
        limit = std::min(size, limit);
    }
    if (perm.size() < limit) {
        LOG(FATAL) << "Size of permutation is less than required.";
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

} // namespace doris::vectorized
