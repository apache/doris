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

#include "format/parquet/decoder.h"

#include <cctz/time_zone.h>
#include <gen_cpp/parquet_types.h>

#include "common/cast_set.h"
#include "format/parquet/bool_plain_decoder.h"
#include "format/parquet/bool_rle_decoder.h"
#include "format/parquet/byte_array_dict_decoder.h"
#include "format/parquet/byte_array_plain_decoder.h"
#include "format/parquet/byte_stream_split_decoder.h"
#include "format/parquet/delta_bit_pack_decoder.h"
#include "format/parquet/fix_length_dict_decoder.hpp"
#include "format/parquet/fix_length_plain_decoder.h"
#include "util/rle_encoding.h"

namespace doris {

BaseDictDecoder::BaseDictDecoder() = default;

BaseDictDecoder::~BaseDictDecoder() = default;

Status BaseDictDecoder::set_data(Slice* data) {
    _data = data;
    _offset = 0;
    uint8_t bit_width = *data->data;
    _index_batch_decoder = std::make_unique<RleBatchDecoder<uint32_t>>(
            reinterpret_cast<uint8_t*>(data->data) + 1, static_cast<int>(data->size) - 1,
            bit_width);
    return Status::OK();
}

Status BaseDictDecoder::skip_values(size_t num_values) {
    _indexes.resize(num_values);
    _index_batch_decoder->GetBatch(_indexes.data(), cast_set<uint32_t>(num_values));
    return Status::OK();
}

Status Decoder::get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                            std::unique_ptr<Decoder>& decoder) {
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
        switch (type) {
        case tparquet::Type::BOOLEAN:
            decoder.reset(new BoolPlainDecoder());
            break;
        case tparquet::Type::BYTE_ARRAY:
            decoder.reset(new ByteArrayPlainDecoder());
            break;
        case tparquet::Type::INT32:
        case tparquet::Type::INT64:
        case tparquet::Type::INT96:
        case tparquet::Type::FLOAT:
        case tparquet::Type::DOUBLE:
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new FixLengthPlainDecoder());
            break;
        default:
            return Status::InternalError("Unsupported type {}(encoding={}) in parquet decoder",
                                         tparquet::to_string(type), tparquet::to_string(encoding));
        }
        break;
    case tparquet::Encoding::RLE_DICTIONARY:
        switch (type) {
        case tparquet::Type::BOOLEAN:
            return Status::InternalError("Bool type can't has dictionary page");
        case tparquet::Type::BYTE_ARRAY:
            decoder.reset(new ByteArrayDictDecoder());
            break;
        case tparquet::Type::INT32:
            decoder.reset(new FixLengthDictDecoder<tparquet::Type::INT32>());
            break;
        case tparquet::Type::INT64:
            decoder.reset(new FixLengthDictDecoder<tparquet::Type::INT64>());
            break;
        case tparquet::Type::INT96:
            decoder.reset(new FixLengthDictDecoder<tparquet::Type::INT96>());
            break;
        case tparquet::Type::FLOAT:
            decoder.reset(new FixLengthDictDecoder<tparquet::Type::FLOAT>());
            break;
        case tparquet::Type::DOUBLE:
            decoder.reset(new FixLengthDictDecoder<tparquet::Type::DOUBLE>());
            break;
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new FixLengthDictDecoder<tparquet::Type::FIXED_LEN_BYTE_ARRAY>());
            break;
        default:
            return Status::InternalError("Unsupported type {}(encoding={}) in parquet decoder",
                                         tparquet::to_string(type), tparquet::to_string(encoding));
        }
        break;
    case tparquet::Encoding::RLE:
        switch (type) {
        case tparquet::Type::BOOLEAN:
            decoder.reset(new BoolRLEDecoder());
            break;
        default:
            return Status::InternalError("Unsupported type {}(encoding={}) in parquet decoder",
                                         tparquet::to_string(type), tparquet::to_string(encoding));
        }
        break;
    case tparquet::Encoding::DELTA_BINARY_PACKED:
        // Supports only INT32 and INT64.
        switch (type) {
        case tparquet::Type::INT32:
            decoder.reset(new DeltaBitPackDecoder<int32_t>());
            break;
        case tparquet::Type::INT64:
            decoder.reset(new DeltaBitPackDecoder<int64_t>());
            break;
        default:
            return Status::InternalError("DELTA_BINARY_PACKED only supports INT32 and INT64");
        }
        break;
    case tparquet::Encoding::DELTA_BYTE_ARRAY:
        switch (type) {
        case tparquet::Type::BYTE_ARRAY:
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new DeltaByteArrayDecoder());
            break;
        default:
            return Status::InternalError(
                    "DELTA_BYTE_ARRAY only supports BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY.");
        }
        break;
    case tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
        switch (type) {
        case tparquet::Type::BYTE_ARRAY:
            decoder.reset(new DeltaLengthByteArrayDecoder());
            break;
        default:
            return Status::InternalError("DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY.");
        }
        break;
    case tparquet::Encoding::BYTE_STREAM_SPLIT:
        switch (type) {
        case tparquet::Type::INT32:
        case tparquet::Type::INT64:
        case tparquet::Type::INT96:
        case tparquet::Type::FLOAT:
        case tparquet::Type::DOUBLE:
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new ByteStreamSplitDecoder());
            break;
        default:
            return Status::InternalError("Unsupported type {}(encoding={}) in parquet decoder",
                                         tparquet::to_string(type), tparquet::to_string(encoding));
        }
        break;
    default:
        return Status::InternalError("Unsupported encoding {}(type={}) in parquet decoder",
                                     tparquet::to_string(encoding), tparquet::to_string(type));
    }
    return Status::OK();
}

} // namespace doris
