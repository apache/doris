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
#include <string>

#include "format_v2/column_data.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace doris::parquet {

class GlobalRowIdColumnReader final : public ParquetColumnReader {
public:
    GlobalRowIdColumnReader(format::GlobalRowIdContext context, int64_t row_group_first_row,
                            ParquetColumnReaderProfile profile = {});

    int file_column_id() const override;
    int parquet_leaf_column_id() const override;
    const DataTypePtr& type() const override;
    const std::string& name() const override;

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    void append_row_id(uint32_t row_id, MutableColumnPtr& column) const;

    format::GlobalRowIdContext _context;
    int64_t _row_group_first_row = 0;
    int64_t _next_row_position = 0;
};

} // namespace doris::parquet
