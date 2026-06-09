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

#include "format_v2/parquet/reader/global_rowid_column_reader.h"

#include <memory>

#include "common/cast_set.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "storage/utils.h"

namespace doris::parquet {

GlobalRowIdColumnReader::GlobalRowIdColumnReader(format::GlobalRowIdContext context,
                                                 int64_t row_group_first_row,
                                                 ParquetColumnReaderProfile profile)
        : ParquetColumnReader(ParquetColumnSchema {.name = BeConsts::GLOBAL_ROWID_COL},
                              std::make_shared<DataTypeString>(), profile),
          _context(context),
          _row_group_first_row(row_group_first_row) {}

int GlobalRowIdColumnReader::file_column_id() const {
    return format::GLOBAL_ROWID_COLUMN_ID;
}

int GlobalRowIdColumnReader::parquet_leaf_column_id() const {
    return -1;
}

const DataTypePtr& GlobalRowIdColumnReader::type() const {
    return _type;
}

const std::string& GlobalRowIdColumnReader::name() const {
    return _name;
}

Status GlobalRowIdColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet global rowid read result pointer");
    }
    if (rows < 0) {
        return Status::InvalidArgument("Invalid parquet global rowid read rows {}", rows);
    }
    for (int64_t row = 0; row < rows; ++row) {
        append_row_id(cast_set<uint32_t>(_row_group_first_row + _next_row_position + row), column);
    }
    _next_row_position += rows;
    *rows_read = rows;
    return Status::OK();
}

Status GlobalRowIdColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    _next_row_position += rows;
    return Status::OK();
}

void GlobalRowIdColumnReader::append_row_id(uint32_t row_id, MutableColumnPtr& column) const {
    auto* string_column = assert_cast<ColumnString*>(column.get());
    GlobalRowLoacationV2 location(_context.version, _context.backend_id, _context.file_id, row_id);
    string_column->insert_data(reinterpret_cast<const char*>(&location),
                               sizeof(GlobalRowLoacationV2));
}

} // namespace doris::parquet
