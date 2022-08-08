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

#include "parquet_pred_cmp.h"
#include "schema_desc.h"
#include "vparquet_column_reader.h"

namespace doris::vectorized {

RowGroupReader::RowGroupReader(doris::FileReader* file_reader,
                               const std::shared_ptr<FileMetaData>& file_metadata,
                               const std::vector<ParquetReadColumn>& read_columns,
                               const std::map<std::string, int>& map_column,
                               const std::vector<ExprContext*>& conjunct_ctxs)
        : _file_reader(file_reader),
          _file_metadata(file_metadata),
          _read_columns(read_columns),
          _map_column(map_column),
          _conjunct_ctxs(conjunct_ctxs),
          _current_row_group(-1) {}

RowGroupReader::~RowGroupReader() {
    for (auto& column_reader : _column_readers) {
        auto reader = column_reader.second;
        reader->close();
        delete reader;
        reader = nullptr;
    }
    _column_readers.clear();
}

Status RowGroupReader::init(const TupleDescriptor* tuple_desc, int64_t split_start_offset,
                            int64_t split_size) {
    _tuple_desc = tuple_desc;
    _split_start_offset = split_start_offset;
    _split_size = split_size;
    _init_conjuncts(tuple_desc, _conjunct_ctxs);
    RETURN_IF_ERROR(_init_column_readers());
    return Status::OK();
}

void RowGroupReader::_init_conjuncts(const TupleDescriptor* tuple_desc,
                                     const std::vector<ExprContext*>& conjunct_ctxs) {
    if (tuple_desc->slots().empty()) {
        return;
    }
    for (auto& read_col : _read_columns) {
        _parquet_column_ids.emplace(read_col.parquet_column_id);
    }

    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        auto col_iter = _map_column.find(tuple_desc->slots()[i]->col_name());
        if (col_iter == _map_column.end()) {
            continue;
        }
        int parquet_col_id = col_iter->second;
        if (_parquet_column_ids.end() == _parquet_column_ids.find(parquet_col_id)) {
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

Status RowGroupReader::_init_column_readers() {
    for (auto& read_col : _read_columns) {
        SlotDescriptor* slot_desc = read_col.slot_desc;
        FieldDescriptor schema = _file_metadata->schema();
        TypeDescriptor col_type = slot_desc->type();
        const auto& field = schema.get_column(slot_desc->col_name());
        const tparquet::RowGroup row_group =
                _file_metadata->to_thrift_metadata().row_groups[_current_row_group];
        ParquetColumnReader* reader = nullptr;
        RETURN_IF_ERROR(ParquetColumnReader::create(_file_reader, MAX_PARQUET_BLOCK_SIZE, field,
                                                    read_col, slot_desc->type(), row_group,
                                                    reader));
        if (reader == nullptr) {
            return Status::Corruption("Init row group reader failed");
        }
        _column_readers[slot_desc->id()] = reader;
    }
    return Status::OK();
}

Status RowGroupReader::fill_columns_data(Block* block, const int32_t group_id) {
    // get ColumnWithTypeAndName from src_block
    for (auto& read_col : _read_columns) {
        const tparquet::RowGroup row_group =
                _file_metadata->to_thrift_metadata().row_groups[_current_row_group];
        auto& column_with_type_and_name = block->get_by_name(read_col.slot_desc->col_name());
        RETURN_IF_ERROR(_column_readers[read_col.slot_desc->id()]->read_column_data(
                row_group, &column_with_type_and_name.column));
        VLOG_DEBUG << column_with_type_and_name.name;
    }
    // use data fill utils read column data to column ptr
    return Status::OK();
}

Status RowGroupReader::get_next_row_group(const int32_t* group_id) {
    int32_t total_group = _file_metadata->num_row_groups();
    if (total_group == 0 || _file_metadata->num_rows() == 0 || _split_size < 0) {
        return Status::EndOfFile("No row group need read");
    }
    while (_current_row_group < total_group) {
        _current_row_group++;
        const tparquet::RowGroup& row_group =
                _file_metadata->to_thrift_metadata().row_groups[_current_row_group];
        if (!_is_misaligned_range_group(row_group)) {
            continue;
        }
        bool filter_group = false;
        RETURN_IF_ERROR(_process_row_group_filter(row_group, _conjunct_ctxs, &filter_group));
        if (!filter_group) {
            group_id = &_current_row_group;
            break;
        }
    }
    return Status::OK();
}

bool RowGroupReader::_is_misaligned_range_group(const tparquet::RowGroup& row_group) {
    int64_t start_offset = _get_column_start_offset(row_group.columns[0].meta_data);

    auto last_column = row_group.columns[row_group.columns.size() - 1].meta_data;
    int64_t end_offset = _get_column_start_offset(last_column) + last_column.total_compressed_size;

    int64_t row_group_mid = start_offset + (end_offset - start_offset) / 2;
    if (!(row_group_mid >= _split_start_offset &&
          row_group_mid < _split_start_offset + _split_size)) {
        return true;
    }
    return false;
}

Status RowGroupReader::_process_row_group_filter(const tparquet::RowGroup& row_group,
                                                 const std::vector<ExprContext*>& conjunct_ctxs,
                                                 bool* filter_group) {
    _process_column_stat_filter(row_group, conjunct_ctxs, filter_group);
    _init_chunk_dicts();
    RETURN_IF_ERROR(_process_dict_filter(filter_group));
    _init_bloom_filter();
    RETURN_IF_ERROR(_process_bloom_filter(filter_group));
    return Status::OK();
}

Status RowGroupReader::_process_column_stat_filter(const tparquet::RowGroup& row_group,
                                                   const std::vector<ExprContext*>& conjunct_ctxs,
                                                   bool* filter_group) {
    int total_group = _file_metadata->num_row_groups();
    // It will not filter if head_group_offset equals tail_group_offset
    int64_t total_rows = 0;
    int64_t total_bytes = 0;
    for (int row_group_id = 0; row_group_id < total_group; row_group_id++) {
        total_rows += row_group.num_rows;
        total_bytes += row_group.total_byte_size;
        for (SlotId slot_id = 0; slot_id < _tuple_desc->slots().size(); slot_id++) {
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
            auto slot_iter = _slot_conjuncts.find(slot_id);
            if (slot_iter == _slot_conjuncts.end()) {
                continue;
            }
            auto statistic = row_group.columns[parquet_col_id].meta_data.statistics;
            if (!statistic.__isset.max || !statistic.__isset.min) {
                continue;
            }
            // Min-max of statistic is plain-encoded value
            *filter_group =
                    _determine_filter_row_group(slot_iter->second, statistic.min, statistic.max);
            if (*filter_group) {
                _filtered_num_row_groups++;
                VLOG_DEBUG << "Filter row group id: " << row_group_id;
                break;
            }
        }
    }
    VLOG_DEBUG << "DEBUG total_rows: " << total_rows;
    VLOG_DEBUG << "DEBUG total_bytes: " << total_bytes;
    VLOG_DEBUG << "Parquet file: " << _file_metadata->schema().debug_string()
               << ", Num of read row group: " << total_group
               << ", and num of skip row group: " << _filtered_num_row_groups;
    return Status::OK();
}

void RowGroupReader::_init_chunk_dicts() {}

Status RowGroupReader::_process_dict_filter(bool* filter_group) {
    return Status();
}

void RowGroupReader::_init_bloom_filter() {}

Status RowGroupReader::_process_bloom_filter(bool* filter_group) {
    RETURN_IF_ERROR(_file_reader->seek(0));
    return Status();
}

int64_t RowGroupReader::_get_row_group_start_offset(const tparquet::RowGroup& row_group) {
    if (row_group.__isset.file_offset) {
        return row_group.file_offset;
    }
    return row_group.columns[0].meta_data.data_page_offset;
}

int64_t RowGroupReader::_get_column_start_offset(const tparquet::ColumnMetaData& column) {
    if (column.__isset.dictionary_page_offset) {
        DCHECK_LT(column.dictionary_page_offset, column.data_page_offset);
        return column.dictionary_page_offset;
    }
    return column.data_page_offset;
}
} // namespace doris::vectorized
