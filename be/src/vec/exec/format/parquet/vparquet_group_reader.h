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
#include <common/status.h>

#include "exprs/expr_context.h"
#include "io/file_reader.h"
#include "vec/core/block.h"
#include "vparquet_column_reader.h"
#include "vparquet_file_metadata.h"
#include "vparquet_reader.h"

namespace doris::vectorized {
class ParquetReadColumn;
class ParquetColumnReader;
struct RowRange;

class RowGroupReader {
public:
    RowGroupReader(doris::FileReader* file_reader,
                   const std::vector<ParquetReadColumn>& read_columns, const int32_t _row_group_id,
                   tparquet::RowGroup& row_group, cctz::time_zone* ctz);
    ~RowGroupReader();
    Status init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges);
    Status next_batch(Block* block, size_t batch_size, bool* _batch_eof);

private:
    Status _init_column_readers(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges);

private:
    doris::FileReader* _file_reader;
    std::unordered_map<int32_t, std::unique_ptr<ParquetColumnReader>> _column_readers;
    const std::vector<ParquetReadColumn>& _read_columns;
    const int32_t _row_group_id;
    tparquet::RowGroup& _row_group_meta;
    int64_t _read_rows = 0;
    int64_t _total_rows;
    cctz::time_zone* _ctz;
};
} // namespace doris::vectorized
