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
}

AlphaRowsetReader::~AlphaRowsetReader() {
    for (auto& row_cursor : _row_cursors) {
        SAFE_DELETE(row_cursor);
    }
    SAFE_DELETE(_dst_cursor);
}

OLAPStatus AlphaRowsetReader::init(RowsetReaderContext* read_context) {
    if (read_context == nullptr) {
        return OLAP_ERR_INIT_FAILED;
    }
    _current_read_context = read_context;
    if (_current_read_context->stats != nullptr) {
        _stats = _current_read_context->stats;
    }

    Version version = _alpha_rowset_meta->version();
    _is_singleton_rowset = (version.first == version.second);
    bool merge = false;
    /*
     * For singleton rowset, there exists three situations.
     *   1. DUP_KEYS tablet has no necessities to merge row in advance.
     *   2. COMPACTION/CHECKSUM/ALTER_TABLET task has no necessities to
     *      merge row in advance.
     *   3. QUERY task will set preaggregation. If preaggregation is
     *      set to be false, it is necessary to merge row in advance.
     * For cumulative rowset, there is no necessities to merge row in advance.
     */
    if (_is_singleton_rowset) {
        if (_current_read_context->tablet_schema->keys_type() == DUP_KEYS) {
            // DUP_KEYS tablet
            _next_block = &AlphaRowsetReader::_union_block;
        } else {
            if (_current_read_context->reader_type == READER_QUERY
                    && !_current_read_context->preaggregation) {
                // QUERY task which set preaggregation to be true.
                _next_block = &AlphaRowsetReader::_merge_block;
                merge = true;
            } else {
                // COMPACTION/CHECKSUM/ALTER_TABLET task
                _next_block = &AlphaRowsetReader::_union_block;
            }
        }
    } else {
        // query task to scan cumulative rowset
        _next_block = &AlphaRowsetReader::_union_block;
    }

    RETURN_NOT_OK(_init_column_datas(read_context));
    if (_is_singleton_rowset && merge) {
        _read_block.reset(new (std::nothrow) RowBlock(_current_read_context->tablet_schema));
        if (_read_block == nullptr) {
            LOG(WARNING) << "new row block failed in reader";
            return OLAP_ERR_MALLOC_ERROR;
        }
        RowBlockInfo block_info;
        block_info.row_num = _current_read_context->tablet_schema->num_rows_per_row_block();
        block_info.null_supported = true;
        _read_block->init(block_info);
        _dst_cursor = new (std::nothrow) RowCursor();
        _dst_cursor->init(*(_current_read_context->tablet_schema));
        for (size_t i = 0; i < _column_datas.size(); ++i) {
            RowCursor* row_cursor = new (std::nothrow) RowCursor();
            row_cursor->init(*(_current_read_context->tablet_schema));
            _row_cursors.push_back(row_cursor);
        }
    }
    _row_blocks.resize(_column_datas.size(), nullptr);
    _first_read_symbols.resize(_column_datas.size(), true);
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::next_block(RowBlock** block) {
    return (this->*_next_block)(block);
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

OLAPStatus AlphaRowsetReader::_union_block(RowBlock** block) {
    size_t pos = 0;
    for (; pos < _column_datas.size(); ++pos) {
        // union block only use one block to store
        OLAPStatus status = _next_block_for_column_data(pos, &_row_blocks[pos]);
        if (status == OLAP_ERR_DATA_EOF) {
            continue;
        } else if (status != OLAP_SUCCESS) {
            return status;
        } else {
            (*block) = _row_blocks[pos];
            return OLAP_SUCCESS;
        }
    }
    if (pos == _row_blocks.size()) {
        *block = nullptr;
        return OLAP_ERR_DATA_EOF;
    }

    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::_merge_block(RowBlock** block) {
    // Row among different segment groups may overlap with each other.
    // Iterate all row_blocks to fetch min row each round.
    OLAPStatus status = OLAP_SUCCESS;
    _read_block->clear();
    size_t num_rows_in_block = 0;
    while (_read_block->pos() < _num_rows_per_row_block) {
        RowCursor* row_cursor = nullptr;
        status = _next_row_for_singleton_rowset(&row_cursor);
        if (status == OLAP_ERR_DATA_EOF && _read_block->pos() > 0) {
            status = OLAP_SUCCESS;
            break;
        } else if (status != OLAP_SUCCESS) {
            return status;
        }
        _read_block->get_row(_read_block->pos(), _dst_cursor);
        _dst_cursor->copy(*row_cursor, _read_block->mem_pool());
        _read_block->pos_inc();
        num_rows_in_block++;
    }
    _read_block->set_pos(0);
    _read_block->set_limit(num_rows_in_block);
    _read_block->finalize(num_rows_in_block);
    *block = _read_block.get();
    return status;
}

OLAPStatus AlphaRowsetReader::_next_row_for_singleton_rowset(RowCursor** row) {
    RowCursor* min_row = nullptr;
    int min_index = -1;
    for (int i = 0; i < _row_blocks.size(); i++) {
        if (_row_blocks[i] == nullptr || !_row_blocks[i]->has_remaining()) {
            OLAPStatus status = _next_block_for_column_data(i, &_row_blocks[i]);
            if (status == OLAP_ERR_DATA_EOF) {
                continue;
            } else if (status != OLAP_SUCCESS) {
                LOG(INFO) << "read next row of singleton rowset failed:" << status;
                return status;
            }
        }
        size_t pos = _row_blocks[i]->pos();
        RowCursor* current_row = _row_cursors[i];
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

OLAPStatus AlphaRowsetReader::_next_block_for_column_data(size_t pos, RowBlock** row_block) {
    OLAPStatus status = OLAP_SUCCESS;
    if (OLAP_UNLIKELY(_first_read_symbols[pos])) {
        if (_key_range_size > 0) {
            status = _fetch_first_block(pos, row_block);
        } else {
            status = _column_datas[pos]->get_first_row_block(row_block);
            if (status != OLAP_SUCCESS && status != OLAP_ERR_DATA_EOF) {
                LOG(WARNING) << "get first row block failed, status:" << status;
            }
        }
        _first_read_symbols[pos] = false;
        return status;
    } else {
        // get next block
        status = _column_datas[pos]->get_next_block(row_block);
        if (status == OLAP_ERR_DATA_EOF && _key_range_size > 0) {
            // reach the end of one predicate
            // currently, SegmentReader can only support filter one key range a time
            // refresh the predicate and continue read
            return _fetch_first_block(pos, row_block);
        }
    }
    return status;
}

OLAPStatus AlphaRowsetReader::_fetch_first_block(size_t pos, RowBlock** row_block) {
    OLAPStatus status = OLAP_SUCCESS;
    _key_range_indices[pos]++;
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
            LOG(WARNING) << "prepare block read failed. status=" << status;
            return status;
        } else {
            break;
        }
    }
    if (_key_range_indices[pos] >= _key_range_size) {
        *row_block = nullptr;
        return OLAP_ERR_DATA_EOF;
    }
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
            if (new_column_data->empty() && new_column_data->zero_num_rows()) {
                continue;
            }
        } else {
            new_column_data->set_read_params(*read_context->return_columns,
                    *read_context->load_bf_columns,
                    *read_context->conditions,
                    *read_context->predicates,
                    *read_context->lower_bound_keys,
                    *read_context->upper_bound_keys,
                    read_context->is_using_cache,
                    read_context->runtime_state);
            // filter 
            if (new_column_data->rowset_pruning_filter()) {
                _stats->rows_stats_filtered += new_column_data->num_rows();
                VLOG(3) << "filter segment group in query in condition. version="
                        << new_column_data->version().first
                        << "-" << new_column_data->version().second;
                continue;
            }
        }

        int ret = new_column_data->delete_pruning_filter();
        if (ret == DEL_SATISFIED) {
            _stats->rows_del_filtered += new_column_data->num_rows();
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
    }

    if (_key_range_size > 0) {
        _key_range_indices.resize(_column_datas.size(), -1);
    }
    if (!_is_singleton_rowset && _column_datas.size() > 1) {
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
