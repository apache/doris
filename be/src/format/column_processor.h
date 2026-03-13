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
class SlotDescriptor;

/// Column categories for table format reading.
///
/// Each column requested by the query is classified into one of these categories.
/// The category determines how the column's value is obtained:
///   - REGULAR:       Read directly from the data file (Parquet/ORC).
///   - PARTITION_KEY:  Filled from partition metadata (e.g. Hive path partitions).
///   - MISSING:        Column exists in table schema but not in the file (schema evolution),
///                     filled with default value or null.
///   - SYNTHESIZED:    Never in the data file; fully computed at runtime
///                     (e.g. Doris V2 __DORIS_ICEBERG_ROWID_COL__).
///   - GENERATED:      May or may not exist in the data file. If present but null,
///                     the value is backfilled at runtime (e.g. Iceberg V3 _row_id).
///   - INTERNAL:       Read from the data file but consumed internally by the TableReader
///                     and removed from the output block (e.g. Hive ACID system columns).
enum class ColumnCategory {
    REGULAR,
    PARTITION_KEY,
    MISSING,
    SYNTHESIZED,
    GENERATED,
    INTERNAL,
};

/// Describes a column requested by the query, along with its category.
struct ColumnDescriptor {
    std::string name;
    const SlotDescriptor* slot_desc = nullptr;
    bool is_file_slot = false;
    ColumnCategory category = ColumnCategory::REGULAR;
};

} // namespace doris
