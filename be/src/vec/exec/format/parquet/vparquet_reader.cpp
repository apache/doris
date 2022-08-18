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
                             size_t batch_size, int64_t range_start_offset, int64_t range_size,
                             cctz::time_zone* ctz)
        : _num_of_columns_from_file(num_of_columns_from_file),
          _batch_size(batch_size),
          _range_start_offset(range_start_offset),
          _range_size(range_size),
          _ctz(ctz) {
    _file_reader = file_reader;
    _total_groups = 0;
    _current_row_group_id = 0;
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
    _t_metadata.reset(&_file_metadata->to_thrift_metadata());
    _total_groups = _file_metadata->num_row_groups();
    if (_total_groups == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }
    auto schema_desc = _file_metadata->schema();
    for (int i = 0; i < _file_metadata->num_columns(); ++i) {
        VLOG_DEBUG << schema_desc.debug_string();
        // Get the Column Reader for the boolean column
        _map_column.emplace(schema_desc.get_column(i)->name, i);
    }
    RETURN_IF_ERROR(_init_read_columns(tuple_slot_descs));
    RETURN_IF_ERROR(
            _init_row_group_readers(tuple_desc, _range_start_offset, _range_size, conjunct_ctxs));
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
            VLOG_DEBUG << str_error.str();
            return Status::InvalidArgument(str_error.str());
        }
        ParquetReadColumn column(slot_desc);
        _read_columns.emplace_back(column);
        VLOG_DEBUG << "slot_desc " << slot_desc->debug_string();
    }
    return Status::OK();
}

Status ParquetReader::read_next_batch(Block* block, bool* eof) {
    DCHECK(_total_groups == _row_group_readers.size());
    if (_total_groups == 0) {
        *eof = true;
    }
    bool _batch_eof = false;
    auto row_group_reader = _row_group_readers[_current_row_group_id];
    RETURN_IF_ERROR(row_group_reader->next_batch(block, _batch_size, &_batch_eof));
    if (_batch_eof) {
        _current_row_group_id++;
        if (_current_row_group_id > _total_groups) {
            *eof = true;
        }
    }
    return Status::OK();
}

Status ParquetReader::_init_row_group_readers(const TupleDescriptor* tuple_desc,
                                              int64_t range_start_offset, int64_t range_size,
                                              const std::vector<ExprContext*>& conjunct_ctxs) {
    std::vector<int32_t> read_row_groups;
    RETURN_IF_ERROR(_filter_row_groups(&read_row_groups));
    _init_conjuncts(tuple_desc, conjunct_ctxs);
    for (auto row_group_id : read_row_groups) {
        VLOG_DEBUG << "_has_page_index";
        auto row_group = _t_metadata->row_groups[row_group_id];
        auto column_chunks = row_group.columns;
        std::vector<RowRange> skipped_row_ranges;
        if (_has_page_index(column_chunks)) {
            VLOG_DEBUG << "_process_page_index";
            RETURN_IF_ERROR(_process_page_index(row_group, skipped_row_ranges));
        }
        std::shared_ptr<RowGroupReader> row_group_reader;
        row_group_reader.reset(
                new RowGroupReader(_file_reader, _read_columns, row_group_id, row_group, _ctz));
        // todo: can filter row with candidate ranges rather than skipped ranges
        RETURN_IF_ERROR(row_group_reader->init(_file_metadata->schema(), skipped_row_ranges));
        _row_group_readers.emplace_back(row_group_reader);
    }
    VLOG_DEBUG << "_init_row_group_reader finished";
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

Status ParquetReader::_filter_row_groups(std::vector<int32_t>* read_row_group_ids) {
    if (_total_groups == 0 || _file_metadata->num_rows() == 0 || _range_size < 0) {
        return Status::EndOfFile("No row group need read");
    }
    int32_t row_group_idx = -1;
    while (row_group_idx < _total_groups) {
        row_group_idx++;
        const tparquet::RowGroup& row_group = _t_metadata->row_groups[row_group_idx];
        if (_is_misaligned_range_group(row_group)) {
            continue;
        }
        bool filter_group = false;
        RETURN_IF_ERROR(_process_row_group_filter(row_group, &filter_group));
        if (!filter_group) {
            read_row_group_ids->emplace_back(row_group_idx);
            break;
        }
    }
    return Status::OK();
}

bool ParquetReader::_is_misaligned_range_group(const tparquet::RowGroup& row_group) {
    int64_t start_offset = _get_column_start_offset(row_group.columns[0].meta_data);

    auto last_column = row_group.columns[row_group.columns.size() - 1].meta_data;
    int64_t end_offset = _get_column_start_offset(last_column) + last_column.total_compressed_size;

    int64_t row_group_mid = start_offset + (end_offset - start_offset) / 2;
    if (!(row_group_mid >= _range_start_offset &&
          row_group_mid < _range_start_offset + _range_size)) {
        return true;
    }
    return false;
}

bool ParquetReader::_has_page_index(std::vector<tparquet::ColumnChunk> columns) {
    _page_index.reset(new PageIndex());
    return _page_index->check_and_get_page_index_ranges(columns);
}

Status ParquetReader::_process_page_index(tparquet::RowGroup& row_group,
                                          std::vector<RowRange>& skipped_row_ranges) {
    int64_t buffer_size = _page_index->_column_index_size + _page_index->_offset_index_size;
    for (auto col_id : _include_column_ids) {
        uint8_t buff[buffer_size];
        auto chunk = row_group.columns[col_id];
        tparquet::ColumnIndex column_index;
        RETURN_IF_ERROR(_page_index->parse_column_index(chunk, buff, &column_index));
        VLOG_DEBUG << "_column_index_size : " << _page_index->_column_index_size;
        VLOG_DEBUG << "_page_index 0  max_values : " << column_index.max_values[0];
        const int num_of_page = column_index.null_pages.size();
        if (num_of_page <= 1) {
            break;
        }
        auto conjunct_iter = _slot_conjuncts.find(col_id);
        if (_slot_conjuncts.end() == conjunct_iter) {
            continue;
        }
        auto conjuncts = conjunct_iter->second;
        std::vector<int> candidate_page_range;
        _page_index->collect_skipped_page_range(conjuncts, candidate_page_range);
        tparquet::OffsetIndex offset_index;
        RETURN_IF_ERROR(_page_index->parse_offset_index(chunk, buff, buffer_size, &offset_index));
        VLOG_DEBUG << "page_locations size : " << offset_index.page_locations.size();
        for (int page_id : candidate_page_range) {
            RowRange skipped_row_range;
            _page_index->create_skipped_row_range(offset_index, row_group.num_rows, page_id,
                                                  &skipped_row_range);
            skipped_row_ranges.emplace_back(skipped_row_range);
        }
    }
    return Status::OK();
}

Status ParquetReader::_process_row_group_filter(const tparquet::RowGroup& row_group,
                                                bool* filter_group) {
    _process_column_stat_filter(row_group.columns, filter_group);
    _init_chunk_dicts();
    RETURN_IF_ERROR(_process_dict_filter(filter_group));
    _init_bloom_filter();
    RETURN_IF_ERROR(_process_bloom_filter(filter_group));
    return Status::OK();
}

Status ParquetReader::_process_column_stat_filter(const std::vector<tparquet::ColumnChunk>& columns,
                                                  bool* filter_group) {
    // It will not filter if head_group_offset equals tail_group_offset
    std::unordered_set<int> _parquet_column_ids(_include_column_ids.begin(),
                                                _include_column_ids.end());
    for (SlotId slot_id = 0; slot_id < _tuple_desc->slots().size(); slot_id++) {
        auto slot_iter = _slot_conjuncts.find(slot_id);
        if (slot_iter == _slot_conjuncts.end()) {
            continue;
        }
        const std::string& col_name = _tuple_desc->slots()[slot_id]->col_name();
        auto col_iter = _map_column.find(col_name);
        if (col_iter == _map_column.end()) {
            continue;
        }
        int parquet_col_id = col_iter->second;
        if (_parquet_column_ids.end() == _parquet_column_ids.find(parquet_col_id)) {
            // Column not exist in parquet file
            continue;
        }
        auto statistic = columns[parquet_col_id].meta_data.statistics;
        if (!statistic.__isset.max || !statistic.__isset.min) {
            continue;
        }
        // Min-max of statistic is plain-encoded value
        *filter_group = _determine_filter_min_max(slot_iter->second, statistic.min, statistic.max);
        if (*filter_group) {
            break;
        }
    }
    return Status::OK();
}

void ParquetReader::_init_chunk_dicts() {}

Status ParquetReader::_process_dict_filter(bool* filter_group) {
    return Status();
}

void ParquetReader::_init_bloom_filter() {}

Status ParquetReader::_process_bloom_filter(bool* filter_group) {
    RETURN_IF_ERROR(_file_reader->seek(0));
    return Status();
}

int64_t ParquetReader::_get_column_start_offset(const tparquet::ColumnMetaData& column) {
    if (column.__isset.dictionary_page_offset) {
        DCHECK_LT(column.dictionary_page_offset, column.data_page_offset);
        return column.dictionary_page_offset;
    }
    return column.data_page_offset;
}
} // namespace doris::vectorized