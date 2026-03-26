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

#include "exprs/vexpr_fwd.h"

namespace doris {
class SlotDescriptor;

/// Column categories for table format reading.
///
/// Each column requested by the query is classified into one of these categories.
/// The category determines how the column's value is obtained:
///   - REGULAR:       Read directly from the data file (Parquet/ORC).
///                    If the column is absent from a file (schema evolution),
///                    its default_expr is used to produce a default value.
///   - PARTITION_KEY:  Filled from partition metadata (e.g. Hive path partitions).
///   - SYNTHESIZED:    Never in the data file; fully computed at runtime
///                     (e.g. Doris V2 __DORIS_ICEBERG_ROWID_COL__).
///   - GENERATED:      May or may not exist in the data file. If present but null,
///                     the value is backfilled at runtime (e.g. Iceberg V3 _row_id).
enum class ColumnCategory {
    REGULAR,
    PARTITION_KEY,
    SYNTHESIZED,
    GENERATED,
};

/// Describes a column requested by the query, along with its category.
struct ColumnDescriptor {
    std::string name;
    const SlotDescriptor* slot_desc = nullptr;
    ColumnCategory category = ColumnCategory::REGULAR;
    /// Default value expression when this column is missing from the data file.
    /// nullptr means fill with NULL. Built once per table scan in FileScanner.
    VExprContextSPtr default_expr;
};

} // namespace doris
