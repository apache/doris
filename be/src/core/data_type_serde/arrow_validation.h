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

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/compiler_util.h"
#include "common/exception.h"
#include "util/unaligned.h"

namespace doris {
namespace arrow_validation_detail {

inline std::string arrow_type_name(const arrow::Array& array) {
    return array.type() ? array.type()->name() : "unknown";
}

inline void throw_invalid_arrow(std::string_view arrow_type, std::string_view message) {
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid Arrow {}: {}", arrow_type, message);
}

inline void throw_invalid_arrow(const arrow::Array& array, std::string_view message) {
    throw_invalid_arrow(arrow_type_name(array), message);
}

template <typename... Args>
inline void throw_invalid_arrow(std::string_view arrow_type, std::string_view format,
                                Args&&... args) {
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid Arrow {}: {}", arrow_type,
                    fmt::format(std::string(format), std::forward<Args>(args)...));
}

template <typename... Args>
inline void throw_invalid_arrow(const arrow::Array& array, std::string_view format,
                                Args&&... args) {
    throw_invalid_arrow(arrow_type_name(array), format, std::forward<Args>(args)...);
}

inline void check_arrow_length_and_offset(const arrow::Array& array) {
    if (UNLIKELY(array.length() < 0 || array.offset() < 0)) {
        throw_invalid_arrow(array, "negative length or offset: length={}, offset={}",
                            array.length(), array.offset());
    }
}

inline void check_arrow_no_offset(const arrow::Array& array) {
    check_arrow_length_and_offset(array);
    if (UNLIKELY(array.offset() != 0)) {
        throw_invalid_arrow(array, "non-zero array offset is not supported: offset={}",
                            array.offset());
    }
}

inline void check_add_overflow(size_t left, size_t right, const arrow::Array& array,
                               std::string_view item) {
    if (UNLIKELY(left > std::numeric_limits<size_t>::max() - right)) {
        throw_invalid_arrow(array, "{} size overflow", item);
    }
}

inline std::shared_ptr<arrow::Int32Array> get_int32_offsets_array(const arrow::Array& array) {
    auto offsets_array = static_cast<const arrow::ListArray&>(array).offsets();
    auto offsets = std::dynamic_pointer_cast<arrow::Int32Array>(offsets_array);
    if (UNLIKELY(!offsets)) {
        throw_invalid_arrow(array, "offsets array is not Int32Array");
    }
    return offsets;
}

inline std::shared_ptr<arrow::Int64Array> get_int64_offsets_array(
        const arrow::LargeListArray& array) {
    auto offsets_array = array.offsets();
    auto offsets = std::dynamic_pointer_cast<arrow::Int64Array>(offsets_array);
    if (UNLIKELY(!offsets)) {
        throw_invalid_arrow(array, "offsets array is not Int64Array");
    }
    return offsets;
}

} // namespace arrow_validation_detail

inline void check_arrow_no_offset(const arrow::Array& array) {
    arrow_validation_detail::check_arrow_no_offset(array);
}

// Validate the caller's requested read range before any Arrow buffer access.
// This rejects negative or out-of-array start/end values up front.
inline void check_arrow_array_range(const arrow::Array& array, int64_t start, int64_t end) {
    arrow_validation_detail::check_arrow_no_offset(array);
    if (UNLIKELY(start < 0 || end < start || end > array.length())) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "read range is invalid: start={}, end={}, length={}", start, end,
                array.length());
    }
}

// Validate buffers[0] when a validity bitmap exists. This must run before
// IsNull()/IsValid()/null_count(), because those APIs may scan the bitmap.
inline void check_arrow_validity_bitmap(const arrow::Array& array) {
    arrow_validation_detail::check_arrow_length_and_offset(array);
    const auto& buffers = array.data()->buffers;
    if (buffers.empty() || !buffers[0]) {
        if (UNLIKELY(array.data()->null_count > 0)) {
            arrow_validation_detail::throw_invalid_arrow(
                    array, "validity bitmap is missing but null_count={}",
                    array.data()->null_count.load());
        }
        return;
    }

    const size_t offset = static_cast<size_t>(array.offset());
    const size_t length = static_cast<size_t>(array.length());
    arrow_validation_detail::check_add_overflow(offset, length, array, "validity bitmap");
    const size_t count = offset + length;
    const size_t required = count / 8 + (count % 8 != 0 ? 1 : 0);
    const size_t available = static_cast<size_t>(buffers[0]->size());
    if (UNLIKELY(available < required)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "validity bitmap too small: {} bytes available, {} required", available,
                required);
    }
}

// Validate buffers[1] for fixed-width arrays before raw_values(), Value(), or
// direct buffer memcpy. elem_size is the physical byte width of one value.
inline void check_arrow_fixed_width_buffer(const arrow::Array& array, size_t elem_size) {
    check_arrow_validity_bitmap(array);
    const auto& buffers = array.data()->buffers;
    if (UNLIKELY(buffers.size() <= 1 || !buffers[1])) {
        if (array.length() == 0) {
            return;
        }
        arrow_validation_detail::throw_invalid_arrow(array, "data buffer is missing");
    }

    const size_t offset = static_cast<size_t>(array.offset());
    const size_t length = static_cast<size_t>(array.length());
    arrow_validation_detail::check_add_overflow(offset, length, array, "data buffer");
    const size_t count = offset + length;
    if (UNLIKELY(elem_size != 0 && count > std::numeric_limits<size_t>::max() / elem_size)) {
        arrow_validation_detail::throw_invalid_arrow(array, "data buffer size overflow");
    }
    const size_t required = elem_size * count;
    const size_t available = static_cast<size_t>(buffers[1]->size());
    if (UNLIKELY(available < required)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "data buffer too small: {} bytes available, {} required", available,
                required);
    }
}

// Validate the bit-packed boolean data bitmap before BooleanArray::Value().
inline void check_arrow_boolean_buffer(const arrow::Array& array) {
    check_arrow_validity_bitmap(array);
    const auto& buffers = array.data()->buffers;
    if (UNLIKELY(buffers.size() <= 1 || !buffers[1])) {
        if (array.length() == 0) {
            return;
        }
        arrow_validation_detail::throw_invalid_arrow(array, "data bitmap is missing");
    }

    const size_t offset = static_cast<size_t>(array.offset());
    const size_t length = static_cast<size_t>(array.length());
    arrow_validation_detail::check_add_overflow(offset, length, array, "data bitmap");
    const size_t count = offset + length;
    const size_t required = count / 8 + (count % 8 != 0 ? 1 : 0);
    const size_t available = static_cast<size_t>(buffers[1]->size());
    if (UNLIKELY(available < required)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "data bitmap too small: {} bytes available, {} required", available,
                required);
    }
}

// Validate one variable-width value range after reading offsets and before
// forming value_data() + offset.
inline void check_arrow_value_range(const arrow::Array& array, int64_t offset, int64_t length,
                                    size_t buffer_size) {
    if (UNLIKELY(offset < 0 || length < 0)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "value range has negative offset or length: offset={}, length={}", offset,
                length);
    }
    const size_t safe_offset = static_cast<size_t>(offset);
    const size_t safe_length = static_cast<size_t>(length);
    if (UNLIKELY(safe_offset > buffer_size || safe_length > buffer_size - safe_offset)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "value range exceeds data buffer: offset={}, length={}, buffer_size={}",
                offset, length, buffer_size);
    }
}

// Validate two variable-width value offsets before subtracting them. Keeping the endpoints
// separate avoids signed overflow on malformed int64 offsets.
inline int64_t check_arrow_value_offsets(const arrow::Array& array, int64_t start_offset,
                                         int64_t end_offset, size_t buffer_size) {
    if (UNLIKELY(start_offset < 0 || end_offset < start_offset)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "value offsets are invalid: start_offset={}, end_offset={}", start_offset,
                end_offset);
    }
    const int64_t length = end_offset - start_offset;
    check_arrow_value_range(array, start_offset, length, buffer_size);
    return length;
}

namespace arrow_validation_detail {

// Offsets buffers may come from external Arrow producers through Buffer::Wrap or FFI and are not
// guaranteed to be aligned to int32_t. Do not use Int32Array::Value() here because it performs a
// typed raw_values()[i] load and can trigger UBSan on misaligned buffers. Keep this validation path
// consistent with the array/map readers below, which load offsets through unaligned_load().
inline int32_t read_int32_offset(const arrow::Int32Array& offsets, int64_t index) {
    const auto* data = reinterpret_cast<const uint8_t*>(offsets.raw_values());
    return unaligned_load<int32_t>(data + index * sizeof(int32_t));
}

inline int64_t read_int64_offset(const arrow::Int64Array& offsets, int64_t index) {
    const auto* data = reinterpret_cast<const uint8_t*>(offsets.raw_values());
    return unaligned_load<int64_t>(data + index * sizeof(int64_t));
}

template <typename OffsetArray, typename ReadOffset>
inline int64_t check_arrow_offsets_range(const OffsetArray& offsets, size_t offset_size,
                                         ReadOffset&& read_offset, int64_t start, int64_t end) {
    check_arrow_array_range(offsets, 0, offsets.length());
    check_arrow_fixed_width_buffer(offsets, offset_size);
    if (UNLIKELY(start < 0 || end < start || end >= offsets.length())) {
        arrow_validation_detail::throw_invalid_arrow(
                offsets, "offsets read range is invalid: start={}, end={}, offsets_length={}",
                start, end, offsets.length());
    }

    int64_t previous_offset = read_offset(offsets, start);
    if (UNLIKELY(previous_offset < 0)) {
        arrow_validation_detail::throw_invalid_arrow(
                offsets, "offsets contain negative value: offset[{}]={}", start, previous_offset);
    }
    for (int64_t i = start + 1; i <= end; ++i) {
        const int64_t current_offset = read_offset(offsets, i);
        if (UNLIKELY(current_offset < previous_offset)) {
            arrow_validation_detail::throw_invalid_arrow(
                    offsets,
                    "offsets are not monotonically non-decreasing: offset[{}]={} < offset[{}]={}",
                    i, current_offset, i - 1, previous_offset);
        }
        previous_offset = current_offset;
    }
    return previous_offset;
}

inline int64_t check_arrow_offsets_range(const arrow::Int32Array& offsets, int64_t start,
                                         int64_t end) {
    return check_arrow_offsets_range(offsets, sizeof(int32_t), read_int32_offset, start, end);
}

inline int64_t check_arrow_offsets_range(const arrow::Int64Array& offsets, int64_t start,
                                         int64_t end) {
    return check_arrow_offsets_range(offsets, sizeof(int64_t), read_int64_offset, start, end);
}

} // namespace arrow_validation_detail

// Validate List offsets before reading offsets or recursing into values.
inline void check_arrow_list_offsets(const arrow::ListArray& array, int64_t start, int64_t end) {
    check_arrow_array_range(array, start, end);
    const auto offsets = arrow_validation_detail::get_int32_offsets_array(array);
    const int64_t last_offset =
            arrow_validation_detail::check_arrow_offsets_range(*offsets, start, end);
    const int64_t values_length = array.values() ? array.values()->length() : 0;
    if (UNLIKELY(last_offset > values_length)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "offsets exceed values length: last_offset={}, values_length={}",
                last_offset, values_length);
    }
}

// Validate LargeList offsets before reading offsets or recursing into values.
inline void check_arrow_large_list_offsets(const arrow::LargeListArray& array, int64_t start,
                                           int64_t end) {
    check_arrow_array_range(array, start, end);
    const auto offsets = arrow_validation_detail::get_int64_offsets_array(array);
    const int64_t last_offset =
            arrow_validation_detail::check_arrow_offsets_range(*offsets, start, end);
    const int64_t values_length = array.values() ? array.values()->length() : 0;
    if (UNLIKELY(last_offset > values_length)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "offsets exceed values length: last_offset={}, values_length={}",
                last_offset, values_length);
    }
}

inline int64_t checked_fixed_size_list_offset(const arrow::FixedSizeListArray& array,
                                              int64_t index) {
    const int64_t list_size = array.value_length();
    if (UNLIKELY(index < 0 || list_size < 0 ||
                 (list_size != 0 && index > std::numeric_limits<int64_t>::max() / list_size))) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "fixed-size list offset overflows: index={}, list_size={}", index,
                list_size);
    }
    return index * list_size;
}

// Validate FixedSizeList offset arithmetic and its child coverage before recursion.
inline void check_arrow_fixed_size_list_offsets(const arrow::FixedSizeListArray& array,
                                                int64_t start, int64_t end) {
    check_arrow_array_range(array, start, end);
    static_cast<void>(checked_fixed_size_list_offset(array, start));
    const int64_t end_offset = checked_fixed_size_list_offset(array, end);
    const auto& values = array.values();
    if (UNLIKELY(!values || end_offset > values->length())) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "fixed-size list values are too short: end_offset={}, values_length={}",
                end_offset, values ? values->length() : 0);
    }
}

// Validate Map offsets before reading offsets or recursing into keys/items.
inline void check_arrow_map_offsets(const arrow::MapArray& array, int64_t start, int64_t end) {
    check_arrow_array_range(array, start, end);
    const auto offsets = arrow_validation_detail::get_int32_offsets_array(array);
    const int64_t last_offset =
            arrow_validation_detail::check_arrow_offsets_range(*offsets, start, end);
    const int64_t keys_length = array.keys() ? array.keys()->length() : 0;
    if (UNLIKELY(last_offset > keys_length)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "offsets exceed keys length: last_offset={}, keys_length={}", last_offset,
                keys_length);
    }
    const int64_t items_length = array.items() ? array.items()->length() : 0;
    if (UNLIKELY(last_offset > items_length)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "offsets exceed items length: last_offset={}, items_length={}", last_offset,
                items_length);
    }
}

// Validate String/Binary offsets buffer before value_offset(), value_length(),
// raw_value_offsets(), or manual offset reads. Variable-width Arrow arrays need
// offset + length + 1 offset entries.
template <typename ArrowBinaryArray>
inline void check_arrow_binary_offsets_buffer(const ArrowBinaryArray& array) {
    check_arrow_validity_bitmap(array);
    const auto& buffers = array.data()->buffers;
    if (UNLIKELY(buffers.size() <= 1 || !buffers[1])) {
        arrow_validation_detail::throw_invalid_arrow(array, "offsets buffer is missing");
    }

    const size_t offset = static_cast<size_t>(array.offset());
    const size_t length = static_cast<size_t>(array.length());
    if (UNLIKELY(offset > std::numeric_limits<size_t>::max() - length)) {
        arrow_validation_detail::throw_invalid_arrow(array, "offsets entry count overflow");
    }
    const size_t count = offset + length;
    if (UNLIKELY(count == std::numeric_limits<size_t>::max())) {
        arrow_validation_detail::throw_invalid_arrow(array, "offsets entry count overflow");
    }
    const size_t count_plus_one = count + 1;
    if (UNLIKELY(count_plus_one > std::numeric_limits<size_t>::max() /
                                          sizeof(typename ArrowBinaryArray::offset_type))) {
        arrow_validation_detail::throw_invalid_arrow(array, "offsets buffer size overflow");
    }

    const size_t required = count_plus_one * sizeof(typename ArrowBinaryArray::offset_type);
    const size_t available = static_cast<size_t>(buffers[1]->size());
    if (UNLIKELY(available < required)) {
        arrow_validation_detail::throw_invalid_arrow(
                array, "offsets buffer too small: {} bytes available, {} required", available,
                required);
    }
}

} // namespace doris
