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

#include "format_v2/parquet/reader/native/fix_length_plain_decoder.h"

#include <limits>

#include "util/cpu_info.h"

namespace doris::format::parquet::native {

Status FixLengthPlainDecoder::decode_fixed_values(size_t num_values,
                                                  ParquetFixedValueConsumer& consumer) {
    const size_t byte_size = num_values * static_cast<size_t>(_type_length);
    if (UNLIKELY(_offset > _data->size || byte_size > _data->size - _offset)) {
        // Truncated PLAIN pages retain the public error keyword used by corrupt-file regression
        // checks, while the bounds check still prevents the native decoder from reading past data.
        return Status::IOError("Unexpected end of stream in Parquet plain decoder");
    }
    RETURN_IF_ERROR(consumer.consume(reinterpret_cast<const uint8_t*>(_data->data) + _offset,
                                     num_values, static_cast<size_t>(_type_length)));
    _offset += byte_size;
    return Status::OK();
}

Status FixLengthPlainDecoder::decode_selected_fixed_values(const ParquetSelection& selection,
                                                           ParquetFixedValueConsumer& consumer) {
    DORIS_CHECK(_type_length > 0);
    const size_t value_width = static_cast<size_t>(_type_length);
    if (UNLIKELY(selection.total_values > std::numeric_limits<size_t>::max() / value_width ||
                 selection.selected_values > std::numeric_limits<size_t>::max() / value_width)) {
        return Status::IOError("Parquet plain selection byte size overflows");
    }
    const size_t input_bytes = selection.total_values * value_width;
    if (UNLIKELY(_offset > _data->size || input_bytes > _data->size - _offset)) {
        return Status::IOError("Unexpected end of stream in Parquet plain selection decoder");
    }
    const auto* values = reinterpret_cast<const uint8_t*>(_data->data) + _offset;
    _offset += input_bytes;
    const size_t selected_bytes = selection.selected_values * value_width;
    constexpr size_t MIN_FRAGMENTED_RANGES = 8;
    constexpr size_t MAX_AVERAGE_RANGE_VALUES = 4;
    const bool fragmented =
            selection.ranges.size() >= MIN_FRAGMENTED_RANGES &&
            selection.selected_values <= selection.ranges.size() * MAX_AVERAGE_RANGE_VALUES;
    if (fragmented && selected_bytes <= static_cast<size_t>(CpuInfo::get_l2_cache_size())) {
        _selected_values.resize(selected_bytes);
        size_t output_offset = 0;
        for (const auto& range : selection.ranges) {
            DORIS_CHECK(range.first + range.count <= selection.total_values);
            const size_t range_bytes = range.count * value_width;
            memcpy(_selected_values.data() + output_offset, values + range.first * value_width,
                   range_bytes);
            output_offset += range_bytes;
        }
        DORIS_CHECK_EQ(output_offset, selected_bytes);
        // Tiny alternating runs make every consumer re-enter conversion and grow its destination
        // column per range. A cache-resident gather keeps the page access sequential and restores
        // one atomic conversion batch without retaining page-sized scratch for dense selections.
        return consumer.consume(_selected_values.data(), selection.selected_values, value_width);
    }
    _selected_values.clear();
    // PLAIN pages are random-access fixed-width spans. Keep those page bytes pinned while the
    // consumer gathers directly into the final column, otherwise sparse scans pay for a second
    // selected-width buffer and copy before materialization.
    return consumer.consume_selected(values, value_width, selection.ranges);
}

Status FixLengthPlainDecoder::skip_values(size_t num_values) {
    DORIS_CHECK(_type_length > 0);
    const size_t value_width = static_cast<size_t>(_type_length);
    if (UNLIKELY(_offset > _data->size || num_values > (_data->size - _offset) / value_width)) {
        return Status::IOError("Unexpected end of stream in Parquet plain decoder");
    }
    _offset += num_values * value_width;
    return Status::OK();
}

} // namespace doris::format::parquet::native
