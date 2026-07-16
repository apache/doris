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

#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <vector>

#include "common/status.h"
#include "core/custom_allocator.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/types.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace doris::format::parquet::native {
class Decoder : public ParquetDecodeSource {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    static Status get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                              std::unique_ptr<Decoder>& decoder);

    // The type with fix length
    void set_type_length(int32_t type_length) { _type_length = type_length; }

    // Set the data to be decoded
    virtual Status set_data(Slice* data) {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        return Status::NotSupported("Fixed values are not supported by this Parquet decoder");
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        return Status::NotSupported("Binary values are not supported by this Parquet decoder");
    }

    Status skip_values(size_t num_values) override = 0;

    virtual Status set_dict(DorisUniqueBufferPtr<uint8_t>& dict, int32_t length,
                            size_t num_values) {
        return Status::NotSupported("set_dict is not supported");
    }

protected:
    int32_t _type_length;
    Slice* _data = nullptr;
    uint32_t _offset = 0;
};

class BaseDictDecoder : public Decoder {
public:
    BaseDictDecoder() = default;
    ~BaseDictDecoder() override = default;

    // Set the data to be decoded
    Status set_data(Slice* data) override {
        if (UNLIKELY(data == nullptr || data->size == 0)) {
            return Status::Corruption("Parquet dictionary index stream is empty");
        }
        _data = data;
        _offset = 0;
        uint8_t bit_width = *data->data;
        _index_batch_decoder = std::make_unique<RleBatchDecoder<uint32_t>>(
                reinterpret_cast<uint8_t*>(data->data) + 1, static_cast<int>(data->size) - 1,
                bit_width);
        return Status::OK();
    }

    bool has_dictionary() const override { return true; }
    uint64_t dictionary_generation() const override { return _dictionary_generation; }

    Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) override {
        DORIS_CHECK(indices != nullptr);
        indices->resize(num_values);
        const auto decoded =
                _index_batch_decoder->GetBatch(indices->data(), cast_set<uint32_t>(num_values));
        if (UNLIKELY(decoded != num_values)) {
            return Status::IOError("Can't read enough Parquet dictionary indices");
        }
        const size_t num_dictionary_values = dictionary_size();
        for (size_t row = 0; row < num_values; ++row) {
            if (UNLIKELY((*indices)[row] >= num_dictionary_values)) {
                return Status::Corruption(
                        "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                        (*indices)[row], row, num_dictionary_values);
            }
        }
        return Status::OK();
    }

    Status decode_selected_dictionary_indices(const ParquetSelection& selection,
                                              std::vector<uint32_t>* indices) override {
        DORIS_CHECK(indices != nullptr);
        _skip_indices.resize(selection.total_values);
        const auto decoded = _index_batch_decoder->GetBatch(
                _skip_indices.data(), cast_set<uint32_t>(selection.total_values));
        if (UNLIKELY(decoded != selection.total_values)) {
            return Status::IOError("Can't read enough Parquet dictionary indices");
        }
        const size_t num_dictionary_values = dictionary_size();
        for (size_t row = 0; row < selection.total_values; ++row) {
            if (UNLIKELY(_skip_indices[row] >= num_dictionary_values)) {
                return Status::Corruption(
                        "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                        _skip_indices[row], row, num_dictionary_values);
            }
        }
        indices->resize(selection.selected_values);
        size_t output = 0;
        for (const auto& range : selection.ranges) {
            memcpy(indices->data() + output, _skip_indices.data() + range.first,
                   range.count * sizeof(uint32_t));
            output += range.count;
        }
        DORIS_CHECK_EQ(output, selection.selected_values);
        return Status::OK();
    }

protected:
    Status skip_values(size_t num_values) override {
        _skip_indices.resize(num_values);
        const auto skipped = _index_batch_decoder->GetBatch(_skip_indices.data(),
                                                            cast_set<uint32_t>(num_values));
        if (UNLIKELY(skipped != num_values)) {
            return Status::IOError("Can't skip enough Parquet dictionary indices");
        }
        const size_t num_dictionary_values = dictionary_size();
        for (size_t row = 0; row < num_values; ++row) {
            if (UNLIKELY(_skip_indices[row] >= num_dictionary_values)) {
                return Status::Corruption(
                        "Parquet dictionary index {} at skipped row {} exceeds dictionary size {}",
                        _skip_indices[row], row, num_dictionary_values);
            }
        }
        return Status::OK();
    }

    // For dictionary encoding
    DorisUniqueBufferPtr<uint8_t> _dict;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder;
    std::vector<uint32_t> _skip_indices;
    uint64_t _dictionary_generation = 0;
};

} // namespace doris::format::parquet::native
