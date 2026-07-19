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

#include "format_v2/parquet/parquet_type.h"

namespace doris::format::parquet {

DecodedValueKind decoded_value_kind(const ParquetTypeDescriptor& type_descriptor) {
    switch (type_descriptor.physical_type) {
    case tparquet::Type::BOOLEAN:
        return DecodedValueKind::BOOL;
    case tparquet::Type::INT32:
        if (type_descriptor.is_unsigned_integer && type_descriptor.integer_bit_width == 32) {
            return DecodedValueKind::UINT32;
        }
        return DecodedValueKind::INT32;
    case tparquet::Type::INT64:
        if (type_descriptor.is_unsigned_integer && type_descriptor.integer_bit_width == 64) {
            return DecodedValueKind::UINT64;
        }
        return DecodedValueKind::INT64;
    case tparquet::Type::INT96:
        return DecodedValueKind::INT96;
    case tparquet::Type::FLOAT:
        return DecodedValueKind::FLOAT;
    case tparquet::Type::DOUBLE:
        return DecodedValueKind::DOUBLE;
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return DecodedValueKind::FIXED_BINARY;
    case tparquet::Type::BYTE_ARRAY:
    default:
        return DecodedValueKind::BINARY;
    }
}

} // namespace doris::format::parquet
