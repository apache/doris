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

namespace doris::vectorized {
ParquetReader::ParquetReader(FileReader* file_reader, int32_t num_of_columns_from_file,
                             size_t batch_size, int64_t range_start_offset, int64_t range_size)
        : _num_of_columns_from_file(num_of_columns_from_file),
          _batch_size(batch_size),
          _range_start_offset(range_start_offset),
          _range_size(range_size) {
    _file_reader = file_reader;
    _total_groups = 0;
    //    _current_group = 0;
    //        _statistics = std::make_shared<Statistics>();
}

ParquetReader::~ParquetReader() {
    close();
}

void ParquetReader::close() {
    for (auto& conjuncts : _slot_conjuncts) {
        conjuncts.second.clear();
    }
    _slot_conjuncts.clear();
    if (_file_reader != nullptr) {
        _file_reader->close();
        delete _file_reader;
        _file_reader = nullptr;
    }
}

Status ParquetReader::init_reader(const TupleDescriptor* tuple_desc,
                                  const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                  std::vector<ExprContext*>& conjunct_ctxs,
                                  const std::string& timezone) {
    _file_reader->open();
    _conjunct_ctxs.reset(&conjunct_ctxs);
    RETURN_IF_ERROR(parse_thrift_footer(_file_reader, _file_metadata));
    auto metadata = _file_metadata->to_thrift_metadata();

    _total_groups = _file_metadata->num_row_groups();
    if (_total_groups == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }
    auto schema_desc = _file_metadata->schema();
    for (int i = 0; i < _file_metadata->num_columns(); ++i) {
        LOG(WARNING) << schema_desc.debug_string();
        // Get the Column Reader for the boolean column
        _map_column.emplace(schema_desc.get_column(i)->name, i);
    }
    LOG(WARNING) << "";
    RETURN_IF_ERROR(_init_read_columns(tuple_slot_descs));
    RETURN_IF_ERROR(
            _init_row_group_reader(tuple_desc, _range_start_offset, _range_size, conjunct_ctxs));
    return Status::OK();
}

Status ParquetReader::_init_read_columns(const std::vector<SlotDescriptor*>& tuple_slot_descs) {
    DCHECK(_num_of_columns_from_file <= tuple_slot_descs.size());
    _include_column_ids.clear();
    for (int i = 0; i < _num_of_columns_from_file; i++) {
        auto slot_desc = tuple_slot_descs.at(i);
        // Get the Column Reader for the boolean column
        auto iter = _map_column.find(slot_desc->col_name());
        auto parquet_col_id = iter->second;
        if (iter != _map_column.end()) {
            _include_column_ids.emplace_back(parquet_col_id);
        } else {
            std::stringstream str_error;
            str_error << "Invalid Column Name:" << slot_desc->col_name();
            LOG(WARNING) << str_error.str();
            return Status::InvalidArgument(str_error.str());
        }
        ParquetReadColumn column;
        column.slot_desc = slot_desc;
        column.parquet_column_id = parquet_col_id;
        auto physical_type = _file_metadata->schema().get_column(parquet_col_id)->physical_type;
        column.parquet_type = physical_type;
        _read_columns.emplace_back(column);
    }
    return Status::OK();
}

Status ParquetReader::read_next_batch(Block* block, int64_t current_range_offset) {
    if (!_row_group_reader->has_next_batch() || _current_group < _total_groups) {
        RETURN_IF_ERROR(_row_group_reader->get_next_row_group(&_current_group));
    }
    auto metadata = _file_metadata->to_thrift_metadata();
    auto column_chunks = metadata.row_groups[_current_group].columns;
    if (_has_page_index(column_chunks)) {
        Status st = _process_page_index(column_chunks);
        if (st.ok()) {
            // todo: process filter page
            return Status::OK();
        } else {
            // todo: record profile
            LOG(WARNING) << "Process page index failed";
        }
    }
    Status st = _row_group_reader->next_batch(block, _batch_size, _batch_eof);
    if (st.ok()) {
        current_range_offset += _batch_size;
    }
    if (_batch_eof) {
        return Status::EndOfFile("No row group need read");
    }
    return Status::OK();
}

Status ParquetReader::_init_row_group_reader(const TupleDescriptor* tuple_desc,
                                             int64_t range_start_offset, int64_t range_size,
                                             const std::vector<ExprContext*>& conjunct_ctxs) {
    // todo: extract as create()
    _init_conjuncts(tuple_desc, conjunct_ctxs);
    _row_group_reader.reset(new RowGroupReader(_file_reader, _file_metadata, _read_columns,
                                               _map_column, _slot_conjuncts));
    RETURN_IF_ERROR(_row_group_reader->init(tuple_desc, range_start_offset, range_size));
    return Status::OK();
}

void ParquetReader::_init_conjuncts(const TupleDescriptor* tuple_desc,
                                    const std::vector<ExprContext*>& conjunct_ctxs) {
    if (tuple_desc->slots().empty()) {
        return;
    }
    std::unordered_set<int> parquet_col_ids(_include_column_ids.begin(), _include_column_ids.end());
    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        auto col_iter = _map_column.find(tuple_desc->slots()[i]->col_name());
        if (col_iter == _map_column.end()) {
            continue;
        }
        int parquet_col_id = col_iter->second;
        if (parquet_col_ids.end() == parquet_col_ids.find(parquet_col_id)) {
            continue;
        }
        for (int conj_idx = 0; conj_idx < conjunct_ctxs.size(); conj_idx++) {
            Expr* conjunct = conjunct_ctxs[conj_idx]->root();
            if (conjunct->get_num_children() == 0) {
                continue;
            }
            Expr* raw_slot = conjunct->get_child(0);
            if (TExprNodeType::SLOT_REF != raw_slot->node_type()) {
                continue;
            }
            SlotRef* slot_ref = (SlotRef*)raw_slot;
            SlotId conjunct_slot_id = slot_ref->slot_id();
            if (conjunct_slot_id == tuple_desc->slots()[i]->id()) {
                // Get conjuncts by conjunct_slot_id
                auto iter = _slot_conjuncts.find(conjunct_slot_id);
                if (_slot_conjuncts.end() == iter) {
                    std::vector<ExprContext*> conjuncts;
                    conjuncts.emplace_back(conjunct_ctxs[conj_idx]);
                    _slot_conjuncts.emplace(std::make_pair(conjunct_slot_id, conjuncts));
                } else {
                    std::vector<ExprContext*> conjuncts = iter->second;
                    conjuncts.emplace_back(conjunct_ctxs[conj_idx]);
                }
            }
        }
    }
}

bool ParquetReader::_has_page_index(std::vector<tparquet::ColumnChunk> columns) {
    _page_index.reset(new PageIndex());
    return _page_index->check_and_get_page_index_ranges(columns);
}

Status ParquetReader::_process_page_index(std::vector<tparquet::ColumnChunk> columns) {
    int64_t buffer_size = _page_index->_column_index_size + _page_index->_offset_index_size;
    uint8_t buff[buffer_size];
    for (auto col_id : _include_column_ids) {
        auto chunk = columns[col_id];
        RETURN_IF_ERROR(_page_index->parse_column_index(chunk, buff));
        _page_index->get_row_range_for_page();
        for (auto conjunct : *_conjunct_ctxs) {
            // todo: use page index filter min/max val
            conjunct->clear_error_msg();
        }
        RETURN_IF_ERROR(_page_index->parse_offset_index(chunk, buff, buffer_size));
        // todo: calculate row range
    }
    return Status::OK();
}
} // namespace doris::vectorized