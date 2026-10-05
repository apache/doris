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

#include <gtest/gtest.h>

namespace doris::format::parquet {

TEST(ParquetTypeTest, DecodedValueKindUsesNativePhysicalTypes) {
    ParquetTypeDescriptor descriptor;

    const std::pair<tparquet::Type::type, DecodedValueKind> cases[] = {
            {tparquet::Type::BOOLEAN, DecodedValueKind::BOOL},
            {tparquet::Type::INT32, DecodedValueKind::INT32},
            {tparquet::Type::INT64, DecodedValueKind::INT64},
            {tparquet::Type::INT96, DecodedValueKind::INT96},
            {tparquet::Type::FLOAT, DecodedValueKind::FLOAT},
            {tparquet::Type::DOUBLE, DecodedValueKind::DOUBLE},
            {tparquet::Type::BYTE_ARRAY, DecodedValueKind::BINARY},
            {tparquet::Type::FIXED_LEN_BYTE_ARRAY, DecodedValueKind::FIXED_BINARY},
    };

    for (const auto& [physical_type, expected] : cases) {
        descriptor.physical_type = physical_type;
        descriptor.is_unsigned_integer = false;
        descriptor.integer_bit_width = -1;
        EXPECT_EQ(decoded_value_kind(descriptor), expected);
    }
}

TEST(ParquetTypeTest, DecodedValueKindPreservesUnsignedWidth) {
    ParquetTypeDescriptor descriptor;
    descriptor.is_unsigned_integer = true;

    descriptor.physical_type = tparquet::Type::INT32;
    descriptor.integer_bit_width = 32;
    EXPECT_EQ(decoded_value_kind(descriptor), DecodedValueKind::UINT32);

    descriptor.physical_type = tparquet::Type::INT64;
    descriptor.integer_bit_width = 64;
    EXPECT_EQ(decoded_value_kind(descriptor), DecodedValueKind::UINT64);

    // Narrow unsigned logical integers still use the signed physical decoder; conversion
    // happens after decoding so the on-disk bit width remains the single source of truth.
    descriptor.physical_type = tparquet::Type::INT32;
    descriptor.integer_bit_width = 16;
    EXPECT_EQ(decoded_value_kind(descriptor), DecodedValueKind::INT32);
}

} // namespace doris::format::parquet
