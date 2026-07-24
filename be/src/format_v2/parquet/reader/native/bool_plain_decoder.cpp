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

#include "format_v2/parquet/reader/native/bool_plain_decoder.h"

#include <glog/logging.h>

#include <algorithm>
#include <array>

#include "core/column/column_vector.h"
#include "core/types.h"
#include "util/bit_util.h"

namespace doris::format::parquet::native {
Status BoolPlainDecoder::decode_fixed_values(size_t num_values,
                                             ParquetFixedValueConsumer& consumer) {
    std::array<uint8_t, UNPACKED_BUFFER_LEN> values;
    size_t decoded = 0;
    while (decoded < num_values) {
        const size_t batch_size = std::min(values.size(), num_values - decoded);
        for (size_t row = 0; row < batch_size; ++row) {
            bool value = false;
            if (UNLIKELY(!_decode_value(&value))) {
                return Status::IOError("Can't read enough booleans in plain decoder");
            }
            values[row] = static_cast<uint8_t>(value);
        }
        RETURN_IF_ERROR(consumer.consume(values.data(), batch_size, sizeof(uint8_t)));
        decoded += batch_size;
    }
    return Status::OK();
}

Status BoolPlainDecoder::decode_selected_fixed_values(const ParquetSelection& selection,
                                                      ParquetFixedValueConsumer& consumer) {
    selected_values_.resize(selection.selected_values);
    size_t range_index = 0;
    size_t output = 0;
    for (size_t row = 0; row < selection.total_values; ++row) {
        bool value = false;
        if (UNLIKELY(!_decode_value(&value))) {
            return Status::IOError("Can't read enough booleans in plain selection decoder");
        }
        while (range_index < selection.ranges.size() &&
               row >= selection.ranges[range_index].first + selection.ranges[range_index].count) {
            ++range_index;
        }
        if (range_index < selection.ranges.size() && row >= selection.ranges[range_index].first) {
            selected_values_[output++] = static_cast<uint8_t>(value);
        }
    }
    DORIS_CHECK_EQ(output, selection.selected_values);
    return consumer.consume(selected_values_.data(), output, sizeof(uint8_t));
}

Status BoolPlainDecoder::skip_values(size_t num_values) {
    int skip_cached =
            std::min(num_unpacked_values_ - unpacked_value_idx_, cast_set<int>(num_values));
    unpacked_value_idx_ += skip_cached;
    if (skip_cached == num_values) {
        return Status::OK();
    }
    int num_remaining = cast_set<int>(num_values - skip_cached);
    int num_to_skip = BitUtil::RoundDownToPowerOf2(num_remaining, 32);
    if (num_to_skip > 0) {
        // A failed bulk skip must not be reported as success for a truncated boolean page.
        if (!bool_values_.SkipBatch(1, num_to_skip)) {
            return Status::IOError("Can't skip enough booleans in plain decoder");
        }
    }
    num_remaining -= num_to_skip;
    if (num_remaining > 0) {
        DCHECK_LE(num_remaining, UNPACKED_BUFFER_LEN);
        num_unpacked_values_ =
                bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
        if (UNLIKELY(num_unpacked_values_ < num_remaining)) {
            return Status::IOError("Can't skip enough booleans in plain decoder");
        }
        unpacked_value_idx_ = num_remaining;
    }
    return Status::OK();
}

} // namespace doris::format::parquet::native
