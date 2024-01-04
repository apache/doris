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

#include "vec/exec/format/parquet/decoder.h"

#include <cctz/time_zone.h>
#include <gen_cpp/parquet_types.h>

#include "vec/exec/format/parquet/bool_plain_decoder.h"
#include "vec/exec/format/parquet/bool_rle_decoder.h"
#include "vec/exec/format/parquet/byte_array_dict_decoder.h"
#include "vec/exec/format/parquet/byte_array_plain_decoder.h"
#include "vec/exec/format/parquet/delta_bit_pack_decoder.h"
#include "vec/exec/format/parquet/fix_length_dict_decoder.hpp"
#include "vec/exec/format/parquet/fix_length_plain_decoder.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris::vectorized {

const cctz::time_zone DecodeParams::utc0 = cctz::utc_time_zone();

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
            [[fallthrough]];
        case tparquet::Type::INT64:
            [[fallthrough]];
        case tparquet::Type::INT96:
            [[fallthrough]];
        case tparquet::Type::FLOAT:
            [[fallthrough]];
        case tparquet::Type::DOUBLE:
            [[fallthrough]];
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new FixLengthPlainDecoder(type));
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
            decoder.reset(new FixLengthDictDecoder<Int32>(type));
            break;
        case tparquet::Type::INT64:
            decoder.reset(new FixLengthDictDecoder<Int64>(type));
            break;
        case tparquet::Type::INT96:
            decoder.reset(new FixLengthDictDecoder<ParquetInt96>(type));
            break;
        case tparquet::Type::FLOAT:
            decoder.reset(new FixLengthDictDecoder<Float32>(type));
            break;
        case tparquet::Type::DOUBLE:
            decoder.reset(new FixLengthDictDecoder<Float64>(type));
            break;
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new FixLengthDictDecoder<char*>(type));
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
            decoder.reset(new DeltaBitPackDecoder<Int32>(type));
            break;
        case tparquet::Type::INT64:
            decoder.reset(new DeltaBitPackDecoder<Int64>(type));
            break;
        default:
            return Status::InternalError("DELTA_BINARY_PACKED only supports INT32 and INT64");
        }
        break;
    case tparquet::Encoding::DELTA_BYTE_ARRAY:
        switch (type) {
        case tparquet::Type::BYTE_ARRAY:
            decoder.reset(new DeltaByteArrayDecoder(type));
            break;
        default:
            return Status::InternalError("DELTA_BYTE_ARRAY only supports BYTE_ARRAY.");
        }
        break;
    case tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
        switch (type) {
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new DeltaLengthByteArrayDecoder(type));
            break;
        default:
            return Status::InternalError(
                    "DELTA_LENGTH_BYTE_ARRAY only supports FIXED_LEN_BYTE_ARRAY.");
        }
        break;
    default:
        return Status::InternalError("Unsupported encoding {}(type={}) in parquet decoder",
                                     tparquet::to_string(encoding), tparquet::to_string(type));
    }
    return Status::OK();
}

void Decoder::init(FieldSchema* field_schema, cctz::time_zone* ctz) {
    _field_schema = field_schema;
    if (_decode_params == nullptr) {
        _decode_params.reset(new DecodeParams());
    }
    if (ctz != nullptr) {
        _decode_params->ctz = ctz;
    }
    const auto& schema = field_schema->parquet_schema;
    if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
        const auto& timestamp_info = schema.logicalType.TIMESTAMP;
        if (!timestamp_info.isAdjustedToUTC) {
            // should set timezone to utc+0
            _decode_params->ctz = const_cast<cctz::time_zone*>(&_decode_params->utc0);
        }
        const auto& time_unit = timestamp_info.unit;
        if (time_unit.__isset.MILLIS) {
            _decode_params->second_mask = 1000;
            _decode_params->scale_to_nano_factor = 1000000;
        } else if (time_unit.__isset.MICROS) {
            _decode_params->second_mask = 1000000;
            _decode_params->scale_to_nano_factor = 1000;
        } else if (time_unit.__isset.NANOS) {
            _decode_params->second_mask = 1000000000;
            _decode_params->scale_to_nano_factor = 1;
        }
    } else if (schema.__isset.converted_type) {
        const auto& converted_type = schema.converted_type;
        if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
            _decode_params->second_mask = 1000;
            _decode_params->scale_to_nano_factor = 1000000;
        } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
            _decode_params->second_mask = 1000000;
            _decode_params->scale_to_nano_factor = 1000;
        }
    }

    if (_decode_params->ctz) {
        VecDateTimeValue t;
        t.from_unixtime(0, *_decode_params->ctz);
        _decode_params->offset_days = t.day() == 31 ? -1 : 0; // If 1969-12-31, then returns -1.
    }
}
} // namespace doris::vectorized
