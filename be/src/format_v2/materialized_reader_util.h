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

#include <memory>

#include "common/status.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "runtime/runtime_profile.h"

namespace doris {
class Block;

namespace io {
struct IOContext;
} // namespace io

namespace format {
struct FileScanRequest;

// Shared helpers for FileReader implementations that deserialize or build already materialized
// Doris columns and then hand those columns to TableReader for final mapping.
ColumnPtr make_column_nullable_if_needed(ColumnPtr column, const DataTypePtr& target_type);

// Optional profile counters for text-like readers. Native/JSON do not expose per-reader filter
// counters today, so they call apply_materialized_reader_filters() without this struct.
struct MaterializedReaderFilterProfile {
    RuntimeProfile::Counter* delete_conjunct_filter_time = nullptr;
    RuntimeProfile::Counter* conjunct_filter_time = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_delete_conjunct = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr;
};

// Applies file-local filters in the same order used by FileScannerV2 readers:
// 1. delete_conjuncts remove rows that should not be visible to the scan output;
// 2. conjuncts apply ordinary file-local predicates.
//
// Only ordinary conjunct filtering contributes to IOContext::predicate_filtered_rows. This matches
// the previous JSON/Text/CSV behavior and keeps scanner accounting separate from delete filtering.
// When `profile` is provided, the helper also updates text-reader timer and row counters so CSV
// and Hive text keep their existing observability after sharing this implementation.
Status apply_materialized_reader_filters(const FileScanRequest* request, io::IOContext* io_ctx,
                                         Block* file_block, size_t* rows,
                                         const MaterializedReaderFilterProfile* profile = nullptr);

} // namespace format
} // namespace doris
