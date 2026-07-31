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
#include <vector>

#include "common/status.h"
#include "format_v2/column_data.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format {

const schema::external::TSchema* find_history_schema(const TFileScanRangeParams* params,
                                                     int64_t schema_id);

bool can_map_by_history_schema(const TFileScanRangeParams* params, int64_t split_schema_id);

// Annotate a file-local schema with the field ids and name mappings from the historical table
// schema that describes the current split. TableReader has already annotated projected table
// columns from current_schema_id; this function performs the symmetric annotation for the file
// schema so TableColumnMapper can match evolved Hudi/Paimon files by field id.
Status annotate_file_schema_from_history(const TFileScanRangeParams* params,
                                         int64_t split_schema_id,
                                         std::vector<ColumnDefinition>* file_schema);

} // namespace doris::format
