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

#include "olap/rowset/alpha_rowset_reader.h"

namespace doris {

AlphaRowsetReader::AlphaRowsetReader(
        int num_rows_per_row_block, RowsetMeta* rowset_meta,
        std::vector<std::shared_ptr<SegmentGroup>> segment_groups,
        RowsetSharedPtr rowset)
      : _num_rows_per_row_block(num_rows_per_row_block),
        _alpha_rowset_meta(nullptr),
        _segment_groups(segment_groups),
        _rowset(rowset),
        _key_range_size(0) {
    _alpha_rowset_meta = reinterpret_cast<AlphaRowsetMeta*>(rowset_meta);
    Version version = _alpha_rowset_meta->version();
    if (version.first == version.second) {
        _is_cumulative_rowset = false;
    } else {
        _is_cumulative_rowset = true;
    }
}

AlphaRowsetReader::~AlphaRowsetReader() {
    for (auto& row_cursor : _row_cursors) {
        delete row_cursor;
    }
}

OLAPStatus AlphaRowsetReader::init(RowsetReaderContext* read_context) {
    if (read_context == nullptr) {
        return OLAP_ERR_INIT_FAILED;
    }
    _current_read_context = read_context;
    if (_current_read_context->stats != nullptr) {
        _stats = _current_read_context->stats;
    }
    OLAPStatus status = _init_column_datas(read_context);
    return status;
}

bool AlphaRowsetReader::has_next() {
    bool next_flag = false;
    for (int i = 0; i < _column_datas.size(); ++i) {
        auto& row_block = _row_blocks[i];
        if (row_block != nullptr) {
            if (row_block->has_remaining()) {
                next_flag = true;
            } else {
                OLAPStatus status = _get_next_block(i, &_row_blocks[i]);
                if (status == OLAP_ERR_DATA_EOF) {
                    _row_blocks[i] = nullptr;
                    continue;
                } else if (status != OLAP_SUCCESS) {
                    LOG(WARNING) << "_get_next_block failed, status:" << status;
                    return false;
                } else {
                    if (_row_blocks[i] != nullptr && _row_blocks[i]->has_remaining()) {
                        next_flag = true;
                    }
                }
            }
        }
    }
    return next_flag;
}

OLAPStatus AlphaRowsetReader::next(RowCursor** row) {
    OLAPStatus status = OLAP_SUCCESS;
    if (_is_cumulative_rowset) {
        status = _get_next_row_for_cumulative_rowset(row);
    } else {
        status = _get_next_row_for_singleton_rowset(row);
    }
    return status;
}

OLAPStatus AlphaRowsetReader::next_block(std::shared_ptr<RowBlock> block) {
    block->clear();
    size_t num_rows_in_block = 0;
    while (block->pos() < _num_rows_per_row_block) {
        RowCursor* row_cursor = nullptr;
        OLAPStatus status = next(&row_cursor);
        if (status == OLAP_ERR_DATA_EOF && block->pos() > 0) {
            break;
        } else if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "next block failed.status:" << status;
            return status;
        }
        block->set_row(block->pos(), *row_cursor);
        block->pos_inc();
        num_rows_in_block++;
    }
    block->set_pos(0);
    block->set_limit(num_rows_in_block);
    block->finalize(num_rows_in_block);
    return OLAP_SUCCESS;
}

bool AlphaRowsetReader::delete_flag() {
    return _alpha_rowset_meta->delete_flag();
}

Version AlphaRowsetReader::version() {
    return _alpha_rowset_meta->version();
}

VersionHash AlphaRowsetReader::version_hash() {
    return _alpha_rowset_meta->version_hash();
}

void AlphaRowsetReader::close() {
    _column_datas.clear();
}

int64_t AlphaRowsetReader::filtered_rows() {
    return _stats->rows_del_filtered;
}

OLAPStatus AlphaRowsetReader::_get_next_block(size_t pos, RowBlock** row_block) {
    // get next block
    OLAPStatus status = _column_datas[pos]->get_next_block(row_block);
    if (status == OLAP_ERR_DATA_EOF && _key_range_size > 0) {
        // reach the end of one predicate
        // currently, SegmentReader can only support filter one key range a time
        // refresh the predicate and continue read
        _key_range_indices[pos]++;
        OLAPStatus status = OLAP_SUCCESS;
        while (_key_range_indices[pos] < _key_range_size) {
            status = _column_datas[pos]->prepare_block_read(
                    _current_read_context->lower_bound_keys->at(_key_range_indices[pos]),
                    _current_read_context->is_lower_keys_included->at(_key_range_indices[pos]),
                    _current_read_context->upper_bound_keys->at(_key_range_indices[pos]),
                    _current_read_context->is_upper_keys_included->at(_key_range_indices[pos]),
                    row_block);
            if (status == OLAP_ERR_DATA_EOF) {
                _key_range_indices[pos]++;
                continue;
            } else if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "prepare block read failed";
                return status;
            } else {
                break;
            }
        }
        if (_key_range_indices[pos] >= _key_range_size) {
            *row_block = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
        return OLAP_SUCCESS;
    }
    return status;
}

OLAPStatus AlphaRowsetReader::_get_next_row_for_singleton_rowset(RowCursor** row) {
    RowCursor* min_row = nullptr;
    int min_index = -1;
    for (int i = 0; i < _row_blocks.size(); i++) {
        if (_row_blocks[i] == nullptr) {
            continue;
        }
        RowCursor* current_row = _row_cursors[i];
        if (!_row_blocks[i]->has_remaining()) {
            OLAPStatus status = _get_next_block(i, &_row_blocks[i]);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "_get_next_block failed, status:" << status;
                return status;
            }
        }
        size_t pos = _row_blocks[i]->pos();
        _row_blocks[i]->get_row(pos, current_row);
        if (min_row == nullptr || min_row->cmp(*current_row) <  0) {
            min_row = current_row;
            min_index = i;
        }
    }
    if (min_row == nullptr || min_index == -1) {
        return OLAP_ERR_DATA_EOF;
    }
    *row = min_row;
    _row_blocks[min_index]->pos_inc();
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::_get_next_row_for_cumulative_rowset(RowCursor** row) {
    size_t pos = 0;
    if (_row_blocks[pos] == nullptr) {
        return OLAP_ERR_DATA_EOF;
    }
    RowCursor* current_row = _row_cursors[pos];
    OLAPStatus status = OLAP_SUCCESS;
    if (!_row_blocks[pos]->has_remaining()) {
        status = _get_next_block(pos, &_row_blocks[pos]);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "_get_next_block failed, status:" << status;
            return status;
        }
    }
    size_t block_pos = _row_blocks[pos]->pos();
    _row_blocks[pos]->get_row(block_pos, current_row);
    *row = current_row;
    _row_blocks[pos]->pos_inc(); 
    return status;
}

OLAPStatus AlphaRowsetReader::_init_column_datas(RowsetReaderContext* read_context) {
    if (read_context->reader_type == READER_QUERY) {
        if (read_context->lower_bound_keys->size() != read_context->is_lower_keys_included->size()
                || read_context->lower_bound_keys->size() != read_context->upper_bound_keys->size()
                || read_context->upper_bound_keys->size() != read_context->is_upper_keys_included->size()) {
            std::string error_msg = "invalid key range arguments";
            LOG(WARNING) << error_msg;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        _key_range_size = read_context->lower_bound_keys->size();
    }

    for (auto& segment_group : _segment_groups) {
        std::shared_ptr<ColumnData> new_column_data(ColumnData::create(segment_group.get()));
        OLAPStatus status = new_column_data->init();
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "init column data failed";
            return OLAP_ERR_READER_READING_ERROR;
        }
        new_column_data->set_delete_handler(read_context->delete_handler);
        new_column_data->set_stats(_stats);
        new_column_data->set_lru_cache(read_context->lru_cache);
        if (read_context->reader_type == READER_ALTER_TABLE) {
            new_column_data->schema_change_init();
            new_column_data->set_using_cache(read_context->is_using_cache);
        } else {
            new_column_data->set_read_params(*read_context->return_columns,
                    *read_context->load_bf_columns,
                    *read_context->conditions,
                    *read_context->predicates,
                    *read_context->lower_bound_keys,
                    *read_context->upper_bound_keys,
                    read_context->is_using_cache,
                    read_context->runtime_state);
            // filter column data
            if (new_column_data->rowset_pruning_filter()) {
                VLOG(3) << "filter segment group in query in condition. version="
                        << new_column_data->version().first
                        << "-" << new_column_data->version().second;
                continue;
            }
        }

        int ret = new_column_data->delete_pruning_filter();
        if (ret == DEL_SATISFIED) {
            VLOG(3) << "filter segment group in delete predicate:"
                << new_column_data->version().first << ", " << new_column_data->version().second;
            continue;
        } else if (ret == DEL_PARTIAL_SATISFIED) {
            VLOG(3) << "filter segment group partially in delete predicate:"
                << new_column_data->version().first << ", " << new_column_data->version().second;
            new_column_data->set_delete_status(DEL_PARTIAL_SATISFIED);
        } else {
            VLOG(3) << "not filter segment group in delete predicate:"
                << new_column_data->version().first << ", " << new_column_data->version().second;
            new_column_data->set_delete_status(DEL_NOT_SATISFIED);
        }
        _column_datas.emplace_back(new_column_data);
        RowCursor* row_cursor = new RowCursor();
        row_cursor->init(*(_current_read_context->tablet_schema));
        _row_cursors.push_back(row_cursor);

        RowBlock* row_block = nullptr;
        if (_key_range_size > 0) {
            _key_range_indices.push_back(0);
            size_t pos = _key_range_indices.size();
            while (_key_range_indices[pos - 1] < _key_range_size) {
                status = new_column_data->prepare_block_read(
                        read_context->lower_bound_keys->at(_key_range_indices[pos - 1]),
                        read_context->is_lower_keys_included->at(_key_range_indices[pos - 1]),
                        read_context->upper_bound_keys->at(_key_range_indices[pos - 1]),
                        read_context->is_upper_keys_included->at(_key_range_indices[pos - 1]),
                        &row_block);
                if (status == OLAP_ERR_DATA_EOF) {
                    LOG(WARNING) << "prepare block read eof";
                    _key_range_indices[pos - 1]++;
                    continue;
                } else if (status != OLAP_SUCCESS) {
                    LOG(WARNING) << "prepare block read failed";
                    return status;
                } else {
                    break;
                }
            }
            if (_key_range_indices[pos - 1] >= _key_range_size && status == OLAP_ERR_DATA_EOF) {
                row_block = nullptr;
            }
        } else {
            status = new_column_data->get_first_row_block(&row_block);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "get first row block failed";
                return status;
            }
        }
        _row_blocks.push_back(row_block);
    }
    if (_is_cumulative_rowset && _column_datas.size() > 1) {
        LOG(WARNING) << "invalid column_datas for cumulative rowset. column_datas size:"
                << _column_datas.size();
        return OLAP_ERR_READER_READING_ERROR;
    }
    return OLAP_SUCCESS;
}

RowsetSharedPtr AlphaRowsetReader::rowset() {
    return _rowset;
}

}  // namespace doris
