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

#include "vparquet_reader.h"

#include "parquet_thrift_util.h"
#include "vparquet_page_index.h"

namespace doris::vectorized {
doris::vectorized::ParquetReader::ParquetReader(doris::FileReader* file_reader, int64_t batch_size,
                                                int32_t num_of_columns_from_file,
                                                int64_t range_start_offset, int64_t range_size) {
    //        : _batch_size(batch_size), _num_of_columns_from_file(num_of_columns_from_file) {
    _file_reader = file_reader;
    _total_groups = 0;
    //    _current_group = 0;
    //        _statistics = std::make_shared<Statistics>();
}

ParquetReader::~ParquetReader() {
    close();
}

void ParquetReader::close() {
    if (_file_reader != nullptr) {
        _file_reader->close();
        delete _file_reader;
        _file_reader = nullptr;
    }
}

Status ParquetReader::init_reader(const TupleDescriptor* tuple_desc,
                                  const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                  const std::vector<ExprContext*>& conjunct_ctxs,
                                  const std::string& timezone) {
    _file_reader->open();
    RETURN_IF_ERROR(parse_thrift_footer(_file_reader, _file_metadata));
    auto metadata = _file_metadata->to_thrift_metadata();

    _total_groups = metadata.row_groups.size();
    if (_total_groups == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }
    auto schemaDescriptor = _file_metadata->schema();
    for (int i = 0; i < _file_metadata->num_columns(); ++i) {
        LOG(WARNING) << schemaDescriptor.debug_string();
        //        // Get the Column Reader for the boolean column
        //        if (schemaDescriptor->(i)->max_definition_level() > 1) {
        //            _map_column.emplace(schemaDescriptor->(i)->path()->ToDotVector()[0], i);
        //        } else {
        //            _map_column.emplace(schemaDescriptor->(i)->name(), i);
        //        }
    }
    LOG(WARNING) << "";
    RETURN_IF_ERROR(_column_indices(tuple_slot_descs));
    _init_row_group_reader();
    return Status::OK();
}

int64_t ParquetReader::_get_row_group_start_offset(const tparquet::RowGroup& row_group) {
    if (row_group.__isset.file_offset) {
        return row_group.file_offset;
    }
    return row_group.columns[0].meta_data.data_page_offset;
}

Status ParquetReader::_column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs) {
    DCHECK(_num_of_columns_from_file <= tuple_slot_descs.size());
    _include_column_ids.clear();
    for (int i = 0; i < _num_of_columns_from_file; i++) {
        auto slot_desc = tuple_slot_descs.at(i);
        // Get the Column Reader for the boolean column
        auto iter = _map_column.find(slot_desc->col_name());
        if (iter != _map_column.end()) {
            _include_column_ids.emplace_back(iter->second);
        } else {
            std::stringstream str_error;
            str_error << "Invalid Column Name:" << slot_desc->col_name();
            LOG(WARNING) << str_error.str();
            return Status::InvalidArgument(str_error.str());
        }
    }
    return Status::OK();
}

Status ParquetReader::read_next_batch(Block* block) {
    // todo: build block and push scanner queue
    _fill_block_data();
    block = _batch;
    return Status();
}

void ParquetReader::_fill_block_data() {
    // read column chunk
    _init_page_index();
    Status st = _process_page_index();
    if (st.ok()) {
        // process filter page
        return;
    }
}

void ParquetReader::_init_row_group_reader() {}

void ParquetReader::_init_page_index(const tparquet::ColumnChunk& chunk) {
    PageIndex pageIndex;

    parse_column_index(chunk, &pageIndex._column_index);
    parse_offset_index(chunk, &pageIndex._offset_index);
}

Status ParquetReader::_process_page_index() {
    return Status();
}
} // namespace doris::vectorized