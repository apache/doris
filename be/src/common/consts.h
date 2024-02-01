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

#include <string>

namespace doris {
namespace BeConsts {
const std::string CSV = "csv";
const std::string CSV_WITH_NAMES = "csv_with_names";
const std::string CSV_WITH_NAMES_AND_TYPES = "csv_with_names_and_types";
const std::string BLOCK_TEMP_COLUMN_PREFIX = "__TEMP__";
const std::string BLOCK_TEMP_COLUMN_SCANNER_FILTERED = "__TEMP__scanner_filtered";
const std::string ROWID_COL = "__DORIS_ROWID_COL__";
const std::string ROW_STORE_COL = "__DORIS_ROW_STORE_COL__";
const std::string DYNAMIC_COLUMN_NAME = "__DORIS_DYNAMIC_COL__";

/// The maximum precision representable by a 4-byte decimal (Decimal4Value)
constexpr int MAX_DECIMAL32_PRECISION = 9;
/// The maximum precision representable by a 8-byte decimal (Decimal8Value)
constexpr int MAX_DECIMAL64_PRECISION = 18;
/// The maximum precision representable by a 16-byte decimal
constexpr int MAX_DECIMAL128_PRECISION = 38;
/// The maximum precision representable by a 32-byte decimal
constexpr int MAX_DECIMAL256_PRECISION = 76;

/// Must be kept in sync with FE's max precision/scale.
static constexpr int MAX_DECIMALV2_PRECISION = 27;
static constexpr int MAX_DECIMALV2_SCALE = 9;

static constexpr int MAX_DECIMALV3_PRECISION = MAX_DECIMAL256_PRECISION;
static constexpr int MAX_DECIMALV3_SCALE = MAX_DECIMALV3_PRECISION;
} // namespace BeConsts
} // namespace doris
