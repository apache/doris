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

#include "format_v2/parquet/reader/row_position_column_reader.h"

#include <memory>

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "format_v2/parquet/parquet_column_schema.h"

namespace doris::parquet {

RowPositionColumnReader::RowPositionColumnReader(int64_t row_group_first_row)
        : ParquetColumnReader(
                  ParquetColumnSchema {
                          .name = ParquetColumnReaderFactory::ROW_POSITION_COLUMN_NAME},
                  std::make_shared<DataTypeInt64>()),
          _row_group_first_row(row_group_first_row) {}

int RowPositionColumnReader::file_column_id() const {
    return ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID;
}

int RowPositionColumnReader::parquet_leaf_column_id() const {
    return -1;
}

const DataTypePtr& RowPositionColumnReader::type() const {
    return _type;
}

const std::string& RowPositionColumnReader::name() const {
    return _name;
}

Status RowPositionColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet row position read result pointer");
    }
    if (rows < 0) {
        return Status::InvalidArgument("Invalid parquet row position read rows {}", rows);
    }
    auto* vector_column = assert_cast<ColumnInt64*>(column.get());
    auto& data = vector_column->get_data();
    const auto old_size = data.size();
    data.resize(old_size + rows);
    for (int64_t row = 0; row < rows; ++row) {
        data[old_size + row] = _row_group_first_row + _next_row_position + row;
    }
    _next_row_position += rows;
    *rows_read = rows;
    return Status::OK();
}

Status RowPositionColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    _next_row_position += rows;
    return Status::OK();
}

} // namespace doris::parquet
