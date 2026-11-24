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
#include <memory>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "format_v2/table_reader.h"

namespace arrow {
class Array;
}

namespace doris::format::lance {

// Inspect source metadata only; neither operation resolves per-row physical locations or reads
// payloads. Local datasets, registered bases and external objects are unsupported for BLOB reads.
bool lance_blob_storage_is_local(std::string_view uri);
Status lance_blob_base_paths_have_local_storage(std::string_view json, bool* has_local_storage);

// Build logical FILE references directly from scan descriptors. Unknown external lengths remain
// NULL until FL_GET_BINARY resolves a handle in the pinned snapshot. FILE.uri is display-only;
// blob_source_id and blob_row_id identify the content within the current query.
Status build_lance_blob_descriptors(const std::shared_ptr<arrow::Array>& descriptors,
                                    const std::vector<uint64_t>& row_ids,
                                    const ColumnDefinition& column, uint32_t source_id,
                                    MutableColumnPtr* output);

} // namespace doris::format::lance
