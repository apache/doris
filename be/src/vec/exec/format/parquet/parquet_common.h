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

#include <cstdint>

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "util/bit_stream_utils.inline.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

using level_t = int16_t;

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    static Status get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                              std::unique_ptr<Decoder>& decoder);

    // The type with fix length
    void set_type_length(int32_t type_length) { _type_length = type_length; }

    // Set the data to be decoded
    virtual void set_data(Slice* data) {
        _data = data;
        _offset = 0;
    }

    // Write the decoded values batch to doris's column
    Status decode_values(ColumnPtr& doris_column, DataTypePtr& data_type, size_t num_values);

    virtual Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                 size_t num_values) = 0;

    virtual Status decode_values(Slice& slice, size_t num_values) = 0;

    virtual Status skip_values(size_t num_values) = 0;

protected:
    int32_t _type_length;
    Slice* _data = nullptr;
    uint32_t _offset = 0;
};

class PlainDecoder final : public Decoder {
public:
    PlainDecoder(tparquet::Type::type physical_type) : _physical_type(physical_type) {};
    ~PlainDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         size_t num_values) override;

    Status decode_values(Slice& slice, size_t num_values) override;

    Status skip_values(size_t num_values) override;

protected:
    Status _decode_short_int(MutableColumnPtr& doris_column, size_t num_values, size_t real_length);

    template <typename Numeric>
    Status _decode_numeric(MutableColumnPtr& doris_column, size_t num_values) {
        size_t to_read_bytes = _type_length * num_values;
        if (UNLIKELY(_offset + to_read_bytes > _data->size)) {
            return Status::IOError("Out-of-bounds access in parquet data decoder");
        }
        auto& column_data = static_cast<ColumnVector<Numeric>&>(*doris_column).get_data();
        const auto* raw_data = reinterpret_cast<const Numeric*>(_data->data + _offset);
        column_data.insert(raw_data, raw_data + num_values);
        _offset += to_read_bytes;
        return Status::OK();
    }

    tparquet::Type::type _physical_type;
};

class ByteArrayPlainDecoder final : public Decoder {
public:
    ByteArrayPlainDecoder() = default;
    ~ByteArrayPlainDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         size_t num_values) override;

    Status decode_values(Slice& slice, size_t num_values) override;

    Status skip_values(size_t num_values) override;
};

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
                         size_t num_values) override;

    Status decode_values(Slice& slice, size_t num_values) override;

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
