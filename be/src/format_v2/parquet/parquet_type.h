// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <parquet/types.h>

#include <string>

#include "core/data_type/data_type.h"
#include "core/data_type_serde/decoded_column_view.h"

namespace parquet {
class ColumnDescriptor;
} // namespace parquet

namespace doris::format::parquet {

// ============================================================================
// ============================================================================

enum class ParquetExtraTypeInfo {
    NONE,               // no special encoding; read by physical type
    DECIMAL_INT32,      // decimal stored as a 4-byte big-endian int
    DECIMAL_INT64,      // decimal stored as an 8-byte big-endian int
    DECIMAL_BYTE_ARRAY, // decimal stored as a variable/fixed-length big-endian byte array
    UNIT_MS,            // time unit is milliseconds
    UNIT_MICROS,        // time unit is microseconds
    UNIT_NS,            // time unit is nanoseconds
    IMPALA_TIMESTAMP,   // Impala-compatible timestamp encoded as INT96
    FLOAT16,            // half-precision float (FIXED_LEN_BYTE_ARRAY(2) -> Float32)
};

enum class ParquetTimeUnit {
    UNKNOWN,
    MILLIS,
    MICROS,
    NANOS,
};

// ============================================================================
// ============================================================================
struct ParquetTypeDescriptor {
    DataTypePtr doris_type;
    // Physical fallback used only to keep file schema construction alive when the logical type is
    // unsupported. Column reader creation still rejects unsupported_reason before decoding.
    DataTypePtr physical_doris_type;
    ParquetExtraTypeInfo extra_type_info = ParquetExtraTypeInfo::NONE;
    ParquetTimeUnit time_unit = ParquetTimeUnit::UNKNOWN;
    ::parquet::Type::type physical_type = ::parquet::Type::UNDEFINED;
    ::parquet::ConvertedType::type converted_type = ::parquet::ConvertedType::UNDEFINED;
    int integer_bit_width = -1;                // bit width for INT_8/16/32/64
    int decimal_precision = -1;                // precision for DECIMAL(p,s)
    int decimal_scale = -1;                    // scale for DECIMAL(p,s)
    int fixed_length = -1;                     // fixed length for FIXED_LEN_BYTE_ARRAY
    bool is_unsigned_integer = false;          // whether the integer is unsigned (UINT_8/16/32/64)
    bool is_decimal = false;                   // whether this is a decimal type
    bool is_timestamp = false;                 // whether this is a timestamp type
    bool timestamp_is_adjusted_to_utc = false; // whether the timestamp is UTC-normalized
    bool is_string_like = false;               // binary type that is neither decimal nor FLOAT16
    bool supports_record_reader = true;        // whether Arrow RecordReader can read this type
    std::string unsupported_reason; // non-empty when this Parquet logical type is unsupported
};

std::string parquet_column_name(const ::parquet::ColumnDescriptor* column);

ParquetTypeDescriptor resolve_parquet_type(const ::parquet::ColumnDescriptor* column);

bool supports_record_reader(const ParquetTypeDescriptor& type_descriptor);

DecodedValueKind decoded_value_kind(const ParquetTypeDescriptor& type_descriptor);

} // namespace doris::format::parquet
