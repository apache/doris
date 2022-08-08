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
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"

namespace doris::vectorized {

using level_t = int16_t;

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    static Status getDecoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                             std::unique_ptr<Decoder>& decoder);

    static MutableColumnPtr getMutableColumnPtr(ColumnPtr& doris_column);

    // The type with fix length
    void set_type_length(int32_t type_length) { _type_length = type_length; }

    // Set the data to be decoded
    void set_data(Slice* data) {
        _data = data;
        _offset = 0;
    }

    // Write the decoded values batch to doris's column
    virtual Status decode_values(ColumnPtr& doris_column, size_t num_values) = 0;

    virtual Status decode_values(Slice& slice, size_t num_values) = 0;

    virtual Status skip_values(size_t num_values) = 0;

protected:
    int32_t _type_length;
    Slice* _data = nullptr;
    uint32_t _offset = 0;
};

template <typename T>
class PlainDecoder final : public Decoder {
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    Status decode_values(ColumnPtr& doris_column, size_t num_values) override {
        size_t to_read_bytes = TYPE_LENGTH * num_values;
        if (UNLIKELY(_offset + to_read_bytes > _data->size)) {
            return Status::IOError("Out-of-bounds access in parquet data decoder");
        }
        auto data_column = getMutableColumnPtr(doris_column);
        auto& column_data = static_cast<ColumnVector<T>&>(*data_column).get_data();
        const auto* raw_data = reinterpret_cast<const T*>(_data->data + _offset);
        column_data.insert(raw_data, raw_data + num_values);
        _offset += to_read_bytes;
        return Status::OK();
    }

    Status decode_values(Slice& slice, size_t num_values) override {
        size_t to_read_bytes = TYPE_LENGTH * num_values;
        if (UNLIKELY(_offset + to_read_bytes > _data->size)) {
            return Status::IOError("Out-of-bounds access in parquet data decoder");
        }
        if (UNLIKELY(to_read_bytes > slice.size)) {
            return Status::IOError(
                    "Slice does not have enough space to write out the decoding data");
        }
        memcpy(slice.data, _data->data + _offset, to_read_bytes);
        _offset += to_read_bytes;
        return Status::OK();
    }

    Status skip_values(size_t num_values) override {
        _offset += TYPE_LENGTH * num_values;
        if (UNLIKELY(_offset > _data->size)) {
            return Status::IOError("Out-of-bounds access in parquet data decoder");
        }
        return Status::OK();
    }

protected:
    enum { TYPE_LENGTH = sizeof(T) };
};

} // namespace doris::vectorized
