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

#include "vparquet_group_reader.h"

#include "schema_desc.h"
#include "vparquet_column_reader.h"

namespace doris::vectorized {

RowGroupReader::RowGroupReader(doris::FileReader* file_reader,
                               const std::vector<ParquetReadColumn>& read_columns,
                               const int32_t row_group_id, const tparquet::RowGroup& row_group,
                               cctz::time_zone* ctz)
        : _file_reader(file_reader),
          _read_columns(read_columns),
          _row_group_id(row_group_id),
          _row_group_meta(row_group),
          _ctz(ctz) {}

RowGroupReader::~RowGroupReader() {
    _column_readers.clear();
}

Status RowGroupReader::init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
                            std::unordered_map<int, tparquet::OffsetIndex>& col_offsets) {
    VLOG_DEBUG << "Row group id: " << _row_group_id;
    RETURN_IF_ERROR(_init_column_readers(schema, row_ranges, col_offsets));
    return Status::OK();
}

Status RowGroupReader::_init_column_readers(
        const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
        std::unordered_map<int, tparquet::OffsetIndex>& col_offsets) {
    for (auto& read_col : _read_columns) {
        auto field = const_cast<FieldSchema*>(schema.get_column(read_col._file_slot_name));
        std::unique_ptr<ParquetColumnReader> reader;
        RETURN_IF_ERROR(ParquetColumnReader::create(_file_reader, field, read_col, _row_group_meta,
                                                    row_ranges, _ctz, reader));
        auto col_iter = col_offsets.find(read_col._parquet_col_id);
        if (col_iter != col_offsets.end()) {
            tparquet::OffsetIndex oi = col_iter->second;
            reader->add_offset_index(&oi);
        }
        if (reader == nullptr) {
            VLOG_DEBUG << "Init row group reader failed";
            return Status::Corruption("Init row group reader failed");
        }
        _column_readers[read_col._file_slot_name] = std::move(reader);
    }
    return Status::OK();
}

Status RowGroupReader::next_batch(Block* block, size_t batch_size, bool* _batch_eof) {
    size_t batch_read_rows = 0;
    bool has_eof = false;
    int col_idx = 0;
    for (auto& read_col : _read_columns) {
        auto& column_with_type_and_name = block->get_by_name(read_col._file_slot_name);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        size_t col_read_rows = 0;
        bool col_eof = false;
        while (!col_eof && col_read_rows < batch_size) {
            size_t loop_rows = 0;
            RETURN_IF_ERROR(_column_readers[read_col._file_slot_name]->read_column_data(
                    column_ptr, column_type, batch_size - col_read_rows, &loop_rows, &col_eof));
            col_read_rows += loop_rows;
        }
        if (col_idx > 0 && (has_eof ^ col_eof)) {
            return Status::Corruption("The number of rows are not equal among parquet columns");
        }
        if (batch_read_rows > 0 && batch_read_rows != col_read_rows) {
            return Status::Corruption("Can't read the same number of rows among parquet columns");
        }
        batch_read_rows = col_read_rows;
        has_eof = col_eof;
        col_idx++;
    }
    _read_rows += batch_read_rows;
    *_batch_eof = has_eof;
    // use data fill utils read column data to column ptr
    return Status::OK();
}

} // namespace doris::vectorized
