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

#include <vector>

namespace doris {
class Status;
}

namespace doris::format {
struct ColumnDefinition;
struct LocalColumnIndex;
enum class TableColumnMappingMode;

// Mark unannotated Parquet groups wherever the matched table column declares a logical Variant.
// Overrides carry file-local paths, including ARRAY/MAP structural wrappers.
Status annotate_parquet_variants(const std::vector<ColumnDefinition>& table_schema,
                                 std::vector<ColumnDefinition>* file_schema,
                                 TableColumnMappingMode mode,
                                 std::vector<LocalColumnIndex>* overrides);
} // namespace doris::format
