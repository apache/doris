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

#include "format_v2/parquet/reader/native/decoder.h"

#include <cctz/time_zone.h>
#include <gen_cpp/parquet_types.h>

#include "format_v2/parquet/reader/native/bool_plain_decoder.h"
#include "format_v2/parquet/reader/native/bool_rle_decoder.h"
#include "format_v2/parquet/reader/native/byte_array_dict_decoder.h"
#include "format_v2/parquet/reader/native/byte_array_plain_decoder.h"
#include "format_v2/parquet/reader/native/byte_stream_split_decoder.h"
#include "format_v2/parquet/reader/native/delta_bit_pack_decoder.h"
#include "format_v2/parquet/reader/native/fix_length_dict_decoder.hpp"
#include "format_v2/parquet/reader/native/fix_length_plain_decoder.h"

namespace doris::format::parquet::native {
namespace {
Status unsupported_type(tparquet::Type::type type, tparquet::Encoding::type encoding) {
    return Status::InternalError("Unsupported type {}(encoding={}) in parquet decoder",
                                 tparquet::to_string(type), tparquet::to_string(encoding));
}

Status create_plain_decoder(tparquet::Type::type type, std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::BOOLEAN:
        decoder = std::make_unique<BoolPlainDecoder>();
        return Status::OK();
    case tparquet::Type::BYTE_ARRAY:
        decoder = std::make_unique<ByteArrayPlainDecoder>();
        return Status::OK();
    case tparquet::Type::INT32:
    case tparquet::Type::INT64:
    case tparquet::Type::INT96:
    case tparquet::Type::FLOAT:
    case tparquet::Type::DOUBLE:
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        decoder = std::make_unique<FixLengthPlainDecoder>();
        return Status::OK();
    default:
        return unsupported_type(type, tparquet::Encoding::PLAIN);
    }
}

Status create_dictionary_decoder(tparquet::Type::type type, std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::BOOLEAN:
        return Status::InternalError("Boolean type cannot have a dictionary page");
    case tparquet::Type::BYTE_ARRAY:
        decoder = std::make_unique<ByteArrayDictDecoder>();
        return Status::OK();
    case tparquet::Type::INT32:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::INT32>>();
        return Status::OK();
    case tparquet::Type::INT64:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::INT64>>();
        return Status::OK();
    case tparquet::Type::INT96:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::INT96>>();
        return Status::OK();
    case tparquet::Type::FLOAT:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::FLOAT>>();
        return Status::OK();
    case tparquet::Type::DOUBLE:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::DOUBLE>>();
        return Status::OK();
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::FIXED_LEN_BYTE_ARRAY>>();
        return Status::OK();
    default:
        return unsupported_type(type, tparquet::Encoding::RLE_DICTIONARY);
    }
}

Status create_delta_binary_decoder(tparquet::Type::type type, std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::INT32:
        decoder = std::make_unique<DeltaBitPackDecoder<int32_t>>();
        return Status::OK();
    case tparquet::Type::INT64:
        decoder = std::make_unique<DeltaBitPackDecoder<int64_t>>();
        return Status::OK();
    default:
        return Status::InternalError("DELTA_BINARY_PACKED only supports INT32 and INT64");
    }
}

Status create_byte_stream_split_decoder(tparquet::Type::type type,
                                        std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::INT32:
    case tparquet::Type::INT64:
    case tparquet::Type::INT96:
    case tparquet::Type::FLOAT:
    case tparquet::Type::DOUBLE:
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        decoder = std::make_unique<ByteStreamSplitDecoder>();
        return Status::OK();
    default:
        return unsupported_type(type, tparquet::Encoding::BYTE_STREAM_SPLIT);
    }
}
} // namespace

Status Decoder::get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                            std::unique_ptr<Decoder>& decoder) {
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
        return create_plain_decoder(type, decoder);
    case tparquet::Encoding::RLE_DICTIONARY:
        return create_dictionary_decoder(type, decoder);
    case tparquet::Encoding::RLE:
        if (type != tparquet::Type::BOOLEAN) {
            return unsupported_type(type, encoding);
        }
        decoder = std::make_unique<BoolRLEDecoder>();
        return Status::OK();
    case tparquet::Encoding::DELTA_BINARY_PACKED:
        return create_delta_binary_decoder(type, decoder);
    case tparquet::Encoding::DELTA_BYTE_ARRAY:
        if (type != tparquet::Type::BYTE_ARRAY && type != tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return Status::InternalError(
                    "DELTA_BYTE_ARRAY only supports BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY.");
        }
        decoder = std::make_unique<DeltaByteArrayDecoder>();
        return Status::OK();
    case tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
        if (type != tparquet::Type::BYTE_ARRAY) {
            return Status::InternalError("DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY.");
        }
        decoder = std::make_unique<DeltaLengthByteArrayDecoder>();
        return Status::OK();
    case tparquet::Encoding::BYTE_STREAM_SPLIT:
        return create_byte_stream_split_decoder(type, decoder);
    default:
        return Status::InternalError("Unsupported encoding {}(type={}) in parquet decoder",
                                     tparquet::to_string(encoding), tparquet::to_string(type));
    }
}

} // namespace doris::format::parquet::native
