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
    // todo: use context instead of these structures
    _row_group_readers.clear();
    _read_row_groups.clear();
    _candidate_row_ranges.clear();
    _slot_conjuncts.clear();
    _file_reader->close();
    _col_offsets.clear();
    delete _file_reader;
}

Status ParquetReader::init_reader(const TupleDescriptor* tuple_desc,
                                  const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                  std::vector<ExprContext*>& conjunct_ctxs,
                                  const std::string& timezone) {
    _file_reader->open();
    _tuple_desc = tuple_desc;
    if (_tuple_desc->slots().size() == 0) {
        return Status::EndOfFile("No Parquet column need load");
    }
    RETURN_IF_ERROR(parse_thrift_footer(_file_reader, _file_metadata));
    _t_metadata = &_file_metadata->to_thrift_metadata();
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
    RETURN_IF_ERROR(_init_row_group_readers(conjunct_ctxs));
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
            ParquetReadColumn column(parquet_col_id, slot_desc);
            _read_columns.emplace_back(column);
        } else {
            std::stringstream str_error;
            str_error << "Invalid Column Name:" << slot_desc->col_name();
            VLOG_DEBUG << str_error.str();
            return Status::InvalidArgument(str_error.str());
        }
    }
    return Status::OK();
}

std::unordered_map<std::string, TypeDescriptor> ParquetReader::get_name_to_type() {
    std::unordered_map<std::string, TypeDescriptor> map;
    auto schema_desc = _file_metadata->schema();
    std::unordered_set<std::string> column_names;
    schema_desc.get_column_names(&column_names);
    for (auto name : column_names) {
        auto field = schema_desc.get_column(name);
        map.emplace(name, field->type);
    }
    return map;
}

Status ParquetReader::get_next_block(Block* block, bool* eof) {
    int32_t num_of_readers = _row_group_readers.size();
    DCHECK(num_of_readers <= _read_row_groups.size());
    if (_read_row_groups.empty()) {
        *eof = true;
        return Status::OK();
    }
    bool _batch_eof = false;
    RETURN_IF_ERROR(_current_group_reader->next_batch(block, _batch_size, &_batch_eof));
    if (_batch_eof) {
        if (!_next_row_group_reader()) {
            *eof = true;
        }
    }
    VLOG_DEBUG << "ParquetReader::get_next_block: " << block->rows();
    return Status::OK();
}

bool ParquetReader::_next_row_group_reader() {
    if (_row_group_readers.empty()) {
        return false;
    }
    _current_group_reader = _row_group_readers.front();
    _row_group_readers.pop_front();
    return true;
}

Status ParquetReader::_init_row_group_readers(const std::vector<ExprContext*>& conjunct_ctxs) {
    _init_conjuncts(conjunct_ctxs);
    RETURN_IF_ERROR(_filter_row_groups());
    for (auto row_group_id : _read_row_groups) {
        auto& row_group = _t_metadata->row_groups[row_group_id];
        std::shared_ptr<RowGroupReader> row_group_reader;
        row_group_reader.reset(
                new RowGroupReader(_file_reader, _read_columns, row_group_id, row_group, _ctz));
        RETURN_IF_ERROR(_process_page_index(row_group));
        RETURN_IF_ERROR(row_group_reader->init(_file_metadata->schema(), _candidate_row_ranges,
                                               _col_offsets));
        _row_group_readers.emplace_back(row_group_reader);
    }
    if (!_next_row_group_reader()) {
        return Status::EndOfFile("No next reader");
    }
    return Status::OK();
}

void ParquetReader::_init_conjuncts(const std::vector<ExprContext*>& conjunct_ctxs) {
    if (_tuple_desc->slots().empty()) {
        return;
    }
    std::unordered_set<int> parquet_col_ids(_include_column_ids.begin(), _include_column_ids.end());
    for (int i = 0; i < _tuple_desc->slots().size(); i++) {
        auto col_iter = _map_column.find(_tuple_desc->slots()[i]->col_name());
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
            if (conjunct_slot_id == _tuple_desc->slots()[i]->id()) {
                // Get conjuncts by conjunct_slot_id
                auto iter = _slot_conjuncts.find(parquet_col_id);
                if (_slot_conjuncts.end() == iter) {
                    std::vector<ExprContext*> conjuncts;
                    conjuncts.emplace_back(conjunct_ctxs[conj_idx]);
                    _slot_conjuncts.emplace(std::make_pair(parquet_col_id, conjuncts));
                } else {
                    std::vector<ExprContext*> conjuncts = iter->second;
                    conjuncts.emplace_back(conjunct_ctxs[conj_idx]);
                }
            }
        }
    }
}

Status ParquetReader::_filter_row_groups() {
    if (_total_groups == 0 || _file_metadata->num_rows() == 0 || _range_size < 0) {
        return Status::EndOfFile("No row group need read");
    }
    for (int32_t row_group_idx = 0; row_group_idx < _total_groups; row_group_idx++) {
        const tparquet::RowGroup& row_group = _t_metadata->row_groups[row_group_idx];
        if (_is_misaligned_range_group(row_group)) {
            continue;
        }
        bool filter_group = false;
        RETURN_IF_ERROR(_process_row_group_filter(row_group, &filter_group));
        if (!filter_group) {
            _read_row_groups.emplace_back(row_group_idx);
        }
    }
    return Status::OK();
}

bool ParquetReader::_is_misaligned_range_group(const tparquet::RowGroup& row_group) {
    int64_t start_offset = _get_column_start_offset(row_group.columns[0].meta_data);

    auto& last_column = row_group.columns[row_group.columns.size() - 1].meta_data;
    int64_t end_offset = _get_column_start_offset(last_column) + last_column.total_compressed_size;

    int64_t row_group_mid = start_offset + (end_offset - start_offset) / 2;
    if (!(row_group_mid >= _range_start_offset &&
          row_group_mid < _range_start_offset + _range_size)) {
        return true;
    }
    return false;
}

bool ParquetReader::_has_page_index(std::vector<tparquet::ColumnChunk>& columns) {
    _page_index.reset(new PageIndex());
    return _page_index->check_and_get_page_index_ranges(columns);
}

Status ParquetReader::_process_page_index(tparquet::RowGroup& row_group) {
    if (!_has_page_index(row_group.columns)) {
        return Status::OK();
    }
    int64_t buffer_size = _page_index->_column_index_size + _page_index->_offset_index_size;
    uint8_t buff[buffer_size];
    int64_t bytes_read = 0;
    RETURN_IF_ERROR(
            _file_reader->readat(_page_index->_column_index_start, buffer_size, &bytes_read, buff));

    std::vector<RowRange> skipped_row_ranges;
    for (auto& read_col : _read_columns) {
        auto conjunct_iter = _slot_conjuncts.find(read_col._parquet_col_id);
        if (_slot_conjuncts.end() == conjunct_iter) {
            continue;
        }
        auto& chunk = row_group.columns[read_col._parquet_col_id];
        tparquet::ColumnIndex column_index;
        RETURN_IF_ERROR(_page_index->parse_column_index(chunk, buff, &column_index));
        const int num_of_pages = column_index.null_pages.size();
        if (num_of_pages <= 0) {
            break;
        }
        auto& conjuncts = conjunct_iter->second;
        std::vector<int> skipped_page_range;
        _page_index->collect_skipped_page_range(&column_index, conjuncts, skipped_page_range);
        if (skipped_page_range.empty()) {
            return Status::OK();
        }
        tparquet::OffsetIndex offset_index;
        RETURN_IF_ERROR(_page_index->parse_offset_index(chunk, buff, buffer_size, &offset_index));
        for (int page_id : skipped_page_range) {
            RowRange skipped_row_range;
            _page_index->create_skipped_row_range(offset_index, row_group.num_rows, page_id,
                                                  &skipped_row_range);
            // use the union row range
            skipped_row_ranges.emplace_back(skipped_row_range);
        }
        _col_offsets.emplace(read_col._parquet_col_id, offset_index);
    }
    if (skipped_row_ranges.empty()) {
        return Status::OK();
    }

    std::sort(skipped_row_ranges.begin(), skipped_row_ranges.end(),
              [](const RowRange& lhs, const RowRange& rhs) {
                  return std::tie(lhs.first_row, lhs.last_row) <
                         std::tie(rhs.first_row, rhs.last_row);
              });
    int skip_end = -1;
    for (auto& skip_range : skipped_row_ranges) {
        VLOG_DEBUG << skip_range.first_row << " " << skip_range.last_row << " | ";
        if (skip_end + 1 >= skip_range.first_row) {
            if (skip_end < skip_range.last_row) {
                skip_end = skip_range.last_row;
            }
        } else {
            // read row with candidate ranges rather than skipped ranges
            _candidate_row_ranges.push_back({skip_end + 1, skip_range.first_row - 1});
            skip_end = skip_range.last_row;
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
        const std::string& col_name = _tuple_desc->slots()[slot_id]->col_name();
        auto col_iter = _map_column.find(col_name);
        if (col_iter == _map_column.end()) {
            continue;
        }
        int parquet_col_id = col_iter->second;
        auto slot_iter = _slot_conjuncts.find(parquet_col_id);
        if (slot_iter == _slot_conjuncts.end()) {
            continue;
        }
        if (_parquet_column_ids.end() == _parquet_column_ids.find(parquet_col_id)) {
            // Column not exist in parquet file
            continue;
        }
        auto& statistic = columns[parquet_col_id].meta_data.statistics;
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
