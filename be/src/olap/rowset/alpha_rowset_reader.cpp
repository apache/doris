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

AlphaRowsetReader::AlphaRowsetReader(int num_key_fields, int num_short_key_fields,
        int num_rows_per_row_block, const std::string rowset_path, RowsetMeta* rowset_meta,
        std::vector<std::shared_ptr<SegmentGroup>> segment_groups) : _num_key_fields(num_key_fields),
            _num_short_key_fields(num_short_key_fields),
            _num_rows_per_row_block(num_rows_per_row_block),
            _rowset_path(rowset_path),
            _alpha_rowset_meta(nullptr),
            _segment_groups(segment_groups),
            _key_range_size(0),
            _key_range_index(0) {
    _alpha_rowset_meta = (AlphaRowsetMeta*)rowset_meta;
    Version version = _alpha_rowset_meta->version();
    if (version.first == version.second) {
        _is_cumulative_rowset = false;
    } else {
        _is_cumulative_rowset = true;
    }
}

OLAPStatus AlphaRowsetReader::init(ReaderContext* read_context) {
    _current_read_context = read_context;
    OLAPStatus status = _init_segment_groups(read_context);
    if (status != OLAP_SUCCESS) {
        return status;
    }
    status = _init_column_datas(read_context);
    return status;
}

bool AlphaRowsetReader::has_next() {
    for (auto& row_block : _row_blocks) {
        if (row_block != nullptr) {
            if (row_block->has_remaining()) {
                return true;
            }
        }
    }
    return false;
}

OLAPStatus AlphaRowsetReader::_get_next_block(ColumnData* column_data, RowBlock* row_block) {
     // get next block
     OLAPStatus status = column_data->get_next_block(&row_block);
     if (status != OLAP_SUCCESS) {
        if (status == OLAP_ERR_DATA_EOF) {
            // reach the end of one predicate
            // refresh the predicate and continue read
            status = _refresh_next_block(column_data, row_block);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "refresh next block failed";
                return status;
            }
            return OLAP_SUCCESS;
       }
       return status; 
     }
     return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::_get_next_row_for_singleton_rowset(RowCursor* row) {
    RowCursor* min_row = nullptr;
    int min_index = -1;
    for (int i = 0; i < _row_blocks.size(); i++) {
        RowCursor* current_row = nullptr;
        RowBlock* current_row_block = _row_blocks[i];
        size_t pos = current_row_block->pos();
        current_row_block->get_row(pos, row);
        if (min_row == nullptr || min_row->cmp(*current_row) <  0) {
            min_row = current_row;
            min_index = i;
        }
    }
    if (min_row == nullptr || min_index == -1) {
        return OLAP_ERR_READER_READING_ERROR; 
    }
    row = min_row;
    RowBlock* row_block = _row_blocks[min_index];
    ColumnData* column_data = _column_datas[min_index].get();
    row_block->pos_inc();
    if (!row_block->has_remaining()) {
        OLAPStatus status = _get_next_block(column_data, row_block);
        if (status != OLAP_SUCCESS) {
            return status;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::_get_next_row_for_cumulative_rowset(RowCursor* row) {
    RowBlock* current_row_block = _row_blocks[0];
    size_t pos = current_row_block->pos();
    current_row_block->get_row(pos, row);
    current_row_block->pos_inc();
    if (!current_row_block->has_remaining()) {
        OLAPStatus status = _get_next_block(_column_datas[0].get(), current_row_block);
        if (status != OLAP_SUCCESS) {
            return status;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::next(RowCursor* row) {
    if (_is_cumulative_rowset) {
        return _get_next_row_for_cumulative_rowset(row);
    } else {
        return _get_next_row_for_singleton_rowset(row);
    }
}

void AlphaRowsetReader::close() {
    _column_datas.clear();
}

OLAPStatus AlphaRowsetReader::_init_column_datas(ReaderContext* read_context) {
    for (auto& segment_group : _segment_groups) {
        std::unique_ptr<ColumnData> new_column_data(ColumnData::create(segment_group.get()));
        OLAPStatus status = new_column_data->init();
        if (status != OLAP_SUCCESS) {
            return OLAP_ERR_READER_READING_ERROR; 
        }
        new_column_data->set_delete_handler(read_context->delete_handler);
        new_column_data->set_stats(read_context->stats);
        new_column_data->set_lru_cache(read_context->lru_cache);
        std::vector<ColumnPredicate*> col_predicates;
        for (auto& column_predicate : read_context->predicates) {
            col_predicates.push_back(&column_predicate.second);
        }
        if (read_context->lower_bound_keys.size() != read_context->is_lower_keys_included.size()
                || read_context->lower_bound_keys.size() != read_context->upper_bound_keys.size()
                || read_context->upper_bound_keys.size() != read_context->is_upper_keys_included.size()) {
            std::string error_msg = "invalid key range arguments";
            LOG(WARNING) << error_msg;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        new_column_data->set_read_params(read_context->return_columns,
                                read_context->load_bf_columns,
                                read_context->conditions,
                                col_predicates,
                                read_context->lower_bound_keys,
                                read_context->upper_bound_keys,
                                read_context->is_using_cache,
                                read_context->runtime_state);
        // filter column data
        if (new_column_data->delta_pruning_filter()) {
            VLOG(3) << "filter delta in query in condition:"
                    << new_column_data->version().first << ", " << new_column_data->version().second;
            continue;
        }
        int ret = new_column_data->delete_pruning_filter();
        if (ret == DEL_SATISFIED) {
            VLOG(3) << "filter delta in delete predicate:"
                    << new_column_data->version().first << ", " << new_column_data->version().second;
            continue;
        } else if (ret == DEL_PARTIAL_SATISFIED) {
            VLOG(3) << "filter delta partially in delete predicate:"
                    << new_column_data->version().first << ", " << new_column_data->version().second;
            new_column_data->set_delete_status(DEL_PARTIAL_SATISFIED);
        } else {
            VLOG(3) << "not filter delta in delete predicate:"
                    << new_column_data->version().first << ", " << new_column_data->version().second;
            new_column_data->set_delete_status(DEL_NOT_SATISFIED);
        }
        _column_datas.emplace_back(std::move(new_column_data));
        _key_range_size = read_context->lower_bound_keys.size();
        RowBlock* row_block = nullptr;
        if (_key_range_size > 0) {
            status = new_column_data->prepare_block_read(read_context->lower_bound_keys[_key_range_index],
                    read_context->is_lower_keys_included[_key_range_index],
                    read_context->upper_bound_keys[_key_range_index],
                    read_context->is_upper_keys_included[_key_range_index],
                    &row_block);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "prepare block read failed";
                return status;
            }
            _key_range_index++;
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

OLAPStatus AlphaRowsetReader::_refresh_next_block(ColumnData* column_data, RowBlock* next_block) {
    OLAPStatus status = column_data->get_next_block(&next_block);
    if (status == OLAP_ERR_DATA_EOF) {
        // currently, SegmentReader can only support filter one key range a time
        // use the next predicate range to get data from segment here
        if (_key_range_size > 0 && _key_range_index < _key_range_size) {
            status = column_data->prepare_block_read(_current_read_context->lower_bound_keys[_key_range_index],
                    _current_read_context->is_lower_keys_included[_key_range_index],
                    _current_read_context->upper_bound_keys[_key_range_index],
                    _current_read_context->is_upper_keys_included[_key_range_index],
                    &next_block);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "prepare block read failed";
                return status; 
            }
            _key_range_index++;
        } else {
            // read finished
            next_block = nullptr;
        }
        return OLAP_SUCCESS;
    } else if (status != OLAP_SUCCESS) {
        return status; 
    } else {
        return OLAP_SUCCESS;
    }
}

}  // namespace doris
