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
#include "core/column/column.h"
#include "format/new_parquet/selection_vector.h"
#include "format/reader/file_reader.h"

namespace doris {
class Block;
} // namespace doris

namespace doris::parquet {

IColumn::Filter selection_to_filter(const SelectionVector& selection, uint16_t selected_rows,
                                    int64_t batch_rows);

Status execute_reader_expression_map(const reader::FileScanRequest& request, Block* file_block,
                                     const std::vector<reader::ColumnId>& target_columns);

Status execute_batch_filters(const reader::FileScanRequest& request, int64_t batch_rows,
                             Block* file_block, SelectionVector* selection,
                             uint16_t* selected_rows);

} // namespace doris::parquet
