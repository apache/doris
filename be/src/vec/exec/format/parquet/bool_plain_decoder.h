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

#include <stddef.h>
#include <stdint.h>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "util/bit_stream_utils.h"
#include "util/bit_stream_utils.inline.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris {
namespace vectorized {
class ColumnSelectVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
/// Decoder bit-packed boolean-encoded values.
/// Implementation from https://github.com/apache/impala/blob/master/be/src/exec/parquet/parquet-bool-decoder.h
class BoolPlainDecoder final : public Decoder {
public:
    BoolPlainDecoder() = default;
    ~BoolPlainDecoder() override = default;

    // Set the data to be decoded
    void set_data(Slice* data) override {
        bool_values_.Reset((const uint8_t*)data->data, data->size);
        num_unpacked_values_ = 0;
        unpacked_value_idx_ = 0;
        _offset = 0;
    }

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

    Status skip_values(size_t num_values) override;

protected:
    inline bool _decode_value(bool* value) {
        if (LIKELY(unpacked_value_idx_ < num_unpacked_values_)) {
            *value = unpacked_values_[unpacked_value_idx_++];
        } else {
            num_unpacked_values_ =
                    bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
            if (UNLIKELY(num_unpacked_values_ == 0)) {
                return false;
            }
            *value = unpacked_values_[0];
            unpacked_value_idx_ = 1;
        }
        return true;
    }

    /// A buffer to store unpacked values. Must be a multiple of 32 size to use the
    /// batch-oriented interface of BatchedBitReader. We use uint8_t instead of bool because
    /// bit unpacking is only supported for unsigned integers. The values are converted to
    /// bool when returned to the user.
    static const int UNPACKED_BUFFER_LEN = 128;
    uint8_t unpacked_values_[UNPACKED_BUFFER_LEN];

    /// The number of valid values in 'unpacked_values_'.
    int num_unpacked_values_ = 0;

    /// The next value to return from 'unpacked_values_'.
    int unpacked_value_idx_ = 0;

    /// Bit packed decoder, used if 'encoding_' is PLAIN.
    BatchedBitReader bool_values_;
};
} // namespace doris::vectorized
