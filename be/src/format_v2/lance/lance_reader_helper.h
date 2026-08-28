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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "gen_cpp/PlanNodes_types.h"

struct LanceBatch;
struct LanceDataset;
struct LanceScanner;

namespace arrow {
class Schema;
} // namespace arrow

namespace doris::format::lance {

inline constexpr std::string_view LANCE_DISTANCE_COLUMN = "_distance";
inline constexpr std::string_view LANCE_SCORE_COLUMN = "_score";
inline constexpr std::string_view LANCE_ROW_ID_COLUMN = "_rowid";
inline constexpr const char* LANCE_READER_PROFILE = "LanceReader";

struct LanceDatasetDeleter {
    void operator()(LanceDataset* dataset) const;
};

struct LanceScannerDeleter {
    void operator()(LanceScanner* scanner) const;
};

struct LanceBatchDeleter {
    void operator()(LanceBatch* batch) const;
};

size_t lance_vector_element_width(TVectorElementType::type type);

// Validate and convert the fragment and index-segment identifiers carried by the FE into the
// unsigned and packed representations expected by lance-c.
Status parse_fragment_ids(const TLanceFileDesc& lance_params, std::vector<uint64_t>* fragment_ids);
Status parse_index_segment_uuids(const TLanceFileDesc& lance_params,
                                 std::vector<uint8_t>* segment_uuids, size_t* segment_count);

// Convert every top-level field without discarding unsupported columns. Malformed schemas still
// return an error and leave both output vectors unchanged. DataTypeNothing is the local sentinel
// for a valid Arrow field whose logical type Doris does not support.
Status convert_arrow_schema_to_doris(const std::shared_ptr<arrow::Schema>& arrow_schema,
                                     std::vector<std::string>* column_names,
                                     std::vector<DataTypePtr>* column_types);

// The FE sends storage options in Lance's own vocabulary. Preserve the key-value sequence exactly
// while validating that every value can cross the C-string boundary into lance-c.
Status build_lance_storage_options(const TFileScanRangeParams* scan_params,
                                   std::vector<std::string>* options);

// Copy and release lance-c's thread-local error message before returning a Doris status.
Status lance_error(std::string_view operation);

} // namespace doris::format::lance
