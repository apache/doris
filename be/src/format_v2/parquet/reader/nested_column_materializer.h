// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"

namespace doris::format::parquet {

// ============================================================================
// ============================================================================

ColumnArray* array_column_from_output(MutableColumnPtr& column);

ColumnMap* map_column_from_output(MutableColumnPtr& column);

ColumnStruct* struct_column_from_output(MutableColumnPtr& column);

NullMap* null_map_from_nullable_output(MutableColumnPtr& column);

// offsets[i] = offsets[i-1] + entry_counts[i].
void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts);

void append_parent_nulls(NullMap* dst, const NullMap& src);

} // namespace doris::format::parquet
