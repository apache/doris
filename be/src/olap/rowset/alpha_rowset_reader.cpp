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

#include "olap/row.h"
#include "olap/rowset/alpha_rowset.h"

namespace doris {

AlphaRowsetReader::AlphaRowsetReader(int num_rows_per_row_block, AlphaRowsetSharedPtr rowset,
                                     const std::shared_ptr<MemTracker>& parent_tracker)
        : _num_rows_per_row_block(num_rows_per_row_block),
          _rowset(std::move(rowset)),
          _parent_tracker(parent_tracker),
          _alpha_rowset_meta(
                  std::static_pointer_cast<AlphaRowsetMeta>(_rowset->rowset_meta()).get()),
          _segment_groups(_rowset->_segment_groups),
          _key_range_size(0) {
    _rowset->aquire();
}

AlphaRowsetReader::~AlphaRowsetReader() {
    delete _dst_cursor;
    _rowset->release();
}

OLAPStatus AlphaRowsetReader::init(RowsetReaderContext* read_context) {
    RETURN_NOT_OK(_rowset->load());
    if (read_context == nullptr) {
        return OLAP_ERR_INIT_FAILED;
    }
    _current_read_context = read_context;
    if (_current_read_context->stats != nullptr) {
        _stats = _current_read_context->stats;
    }

    _is_segments_overlapping = _alpha_rowset_meta->is_segments_overlapping();
    _ordinal = 0;

    RETURN_NOT_OK(_init_merge_ctxs(read_context));

    // needs to sort merge only when
    // 1) we are told to return sorted result (need_ordered_result)
    // 2) we have several segment groups (_is_segments_overlapping && _merge_ctxs.size() > 1)
    if (_current_read_context->need_ordered_result && _is_segments_overlapping &&
        _merge_ctxs.size() > 1) {
        _next_block = &AlphaRowsetReader::_merge_block;
        _read_block.reset(new (std::nothrow)
                                  RowBlock(_current_read_context->tablet_schema, _parent_tracker));
        if (_read_block == nullptr) {
            LOG(WARNING) << "new row block failed in reader";
            return OLAP_ERR_MALLOC_ERROR;
        }
        RowBlockInfo block_info;
        block_info.row_num = _current_read_context->tablet_schema->num_rows_per_row_block();
        block_info.null_supported = true;
        _read_block->init(block_info);
        _dst_cursor = new (std::nothrow) RowCursor();
        if (_dst_cursor == nullptr) {
            LOG(WARNING) << "allocate memory for row cursor failed";
            return OLAP_ERR_MALLOC_ERROR;
        }
        if (_current_read_context->reader_type == READER_ALTER_TABLE) {
            // Upon rollup/alter table, seek_columns is nullptr.
            // Under this circumstance, init RowCursor with all columns.
            _dst_cursor->init(*(_current_read_context->tablet_schema));
            for (size_t i = 0; i < _merge_ctxs.size(); ++i) {
                _merge_ctxs[i].row_cursor.reset(new (std::nothrow) RowCursor());
                _merge_ctxs[i].row_cursor->init(*(_current_read_context->tablet_schema));
            }
        } else {
            _dst_cursor->init(*(_current_read_context->tablet_schema),
                              *(_current_read_context->seek_columns));
            for (size_t i = 0; i < _merge_ctxs.size(); ++i) {
                _merge_ctxs[i].row_cursor.reset(new (std::nothrow) RowCursor());
                _merge_ctxs[i].row_cursor->init(*(_current_read_context->tablet_schema),
                                                *(_current_read_context->seek_columns));
            }
        }
        RETURN_NOT_OK(_init_merge_heap());
    } else {
        _next_block = &AlphaRowsetReader::_union_block;
    }
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

int64_t AlphaRowsetReader::filtered_rows() {
    return _stats->rows_del_filtered;
}

OLAPStatus AlphaRowsetReader::_union_block(RowBlock** block) {
    while (_ordinal < _merge_ctxs.size()) {
        // union block only use one block to store
        OLAPStatus status = _pull_next_block(&(_merge_ctxs[_ordinal]));
        if (status == OLAP_ERR_DATA_EOF) {
            _ordinal++;
            continue;
        } else if (status != OLAP_SUCCESS) {
            return status;
        } else {
            (*block) = _merge_ctxs[_ordinal].row_block;
            return OLAP_SUCCESS;
        }
    }
    if (_ordinal == _merge_ctxs.size()) {
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
        status = _pull_next_row_for_merge_rowset_v2(&row_cursor);
        if (status == OLAP_ERR_DATA_EOF && _read_block->pos() > 0) {
            status = OLAP_SUCCESS;
            break;
        } else if (status != OLAP_SUCCESS) {
            return status;
        }

        VLOG_TRACE << "get merged row: " << row_cursor->to_string();

        _read_block->get_row(_read_block->pos(), _dst_cursor);
        copy_row(_dst_cursor, *row_cursor, _read_block->mem_pool());
        _read_block->pos_inc();
        num_rows_in_block++;

        // MergeHeap should advance one step after row been read.
        // This function must be called after copy_row
        // Otherwise, the row has read will be modified instantly before handled.
        // For example:
        // If I have (1, 1), (2, 2), (3, 3) three records.
        // Now I have read (1, 1).
        // Before copy_row, I rebuild the heap
        // The returned row will be (2, 2) instead of (1, 1)
        AlphaMergeContext* merge_ctx = _merge_heap.top();
        _merge_heap.pop();
        RETURN_NOT_OK(_update_merge_ctx_and_build_merge_heap(merge_ctx));
    }
    _read_block->set_pos(0);
    _read_block->set_limit(num_rows_in_block);
    _read_block->finalize(num_rows_in_block);
    *block = _read_block.get();
    return status;
}

OLAPStatus AlphaRowsetReader::_init_merge_heap() {
    if (_merge_heap.empty() && !_merge_ctxs.empty()) {
        for (auto& merge_ctx : _merge_ctxs) {
            RETURN_NOT_OK(_update_merge_ctx_and_build_merge_heap(&merge_ctx));
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::_update_merge_ctx_and_build_merge_heap(AlphaMergeContext* merge_ctx) {
    if (merge_ctx->is_eof) {
        // nothing in this merge ctx, just return
        return OLAP_SUCCESS;
    }

    // get next row block of this merge ctx
    if (merge_ctx->row_block == nullptr || !merge_ctx->row_block->has_remaining()) {
        OLAPStatus status = _pull_next_block(merge_ctx);
        if (status == OLAP_ERR_DATA_EOF) {
            merge_ctx->is_eof = true;
            return OLAP_SUCCESS;
        } else if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "read next row of singleton rowset failed:" << status;
            return status;
        }
    }

    // read the first row, push it into merge heap, and step forward
    RowCursor* current_row = merge_ctx->row_cursor.get();
    merge_ctx->row_block->get_row(merge_ctx->row_block->pos(), current_row);
    _merge_heap.push(merge_ctx);
    merge_ctx->row_block->pos_inc();
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::_pull_next_row_for_merge_rowset_v2(RowCursor** row) {
    // if _merge_heap is not empty, return the row at top, and insert a new row
    // from corresponding merge_ctx
    if (!_merge_heap.empty()) {
        AlphaMergeContext* merge_ctx = _merge_heap.top();
        *row = merge_ctx->row_cursor.get();
        // Must not rebuild merge_heap in this place.
        // Because row have not been copied and is a pointer.
        // If rebuild merge_heap, content in row will be modified.
        return OLAP_SUCCESS;
    } else {
        // all rows are read
        return OLAP_ERR_DATA_EOF;
    }
}

OLAPStatus AlphaRowsetReader::_pull_next_row_for_merge_rowset(RowCursor** row) {
    RowCursor* min_row = nullptr;
    int min_index = -1;

    size_t ordinal = 0;
    while (ordinal < _merge_ctxs.size()) {
        AlphaMergeContext* merge_ctx = &(_merge_ctxs[ordinal]);
        if (merge_ctx->row_block == nullptr || !merge_ctx->row_block->has_remaining()) {
            OLAPStatus status = _pull_next_block(merge_ctx);
            if (status == OLAP_ERR_DATA_EOF) {
                _merge_ctxs.erase(_merge_ctxs.begin() + ordinal);
                continue;
            } else if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "read next row of singleton rowset failed:" << status;
                return status;
            }
        }
        RowCursor* current_row = merge_ctx->row_cursor.get();
        merge_ctx->row_block->get_row(merge_ctx->row_block->pos(), current_row);
        if (min_row == nullptr || compare_row(*min_row, *current_row) > 0) {
            min_row = current_row;
            min_index = ordinal;
        }
        ordinal++;
    }
    if (min_row == nullptr || min_index == -1) {
        return OLAP_ERR_DATA_EOF;
    }
    *row = min_row;
    _merge_ctxs[min_index].row_block->pos_inc();
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetReader::_pull_next_block(AlphaMergeContext* merge_ctx) {
    OLAPStatus status = OLAP_SUCCESS;
    if (OLAP_UNLIKELY(merge_ctx->first_read_symbol)) {
        if (_key_range_size > 0) {
            status = _pull_first_block(merge_ctx);
        } else {
            status = merge_ctx->column_data->get_first_row_block(&(merge_ctx->row_block));
            if (status != OLAP_SUCCESS && status != OLAP_ERR_DATA_EOF) {
                LOG(WARNING) << "get first row block failed, status:" << status;
            }
        }
        merge_ctx->first_read_symbol = false;
        return status;
    } else {
        // get next block
        status = merge_ctx->column_data->get_next_block(&(merge_ctx->row_block));
        if (status == OLAP_ERR_DATA_EOF && _key_range_size > 0) {
            // reach the end of one predicate
            // currently, SegmentReader can only support filter one key range a time
            // refresh the predicate and continue read
            return _pull_first_block(merge_ctx);
        }
    }
    return status;
}

OLAPStatus AlphaRowsetReader::_pull_first_block(AlphaMergeContext* merge_ctx) {
    OLAPStatus status = OLAP_SUCCESS;
    merge_ctx->key_range_index++;
    while (merge_ctx->key_range_index < _key_range_size) {
        status = merge_ctx->column_data->prepare_block_read(
                _current_read_context->lower_bound_keys->at(merge_ctx->key_range_index),
                _current_read_context->is_lower_keys_included->at(merge_ctx->key_range_index),
                _current_read_context->upper_bound_keys->at(merge_ctx->key_range_index),
                _current_read_context->is_upper_keys_included->at(merge_ctx->key_range_index),
                &(merge_ctx->row_block));
        if (status == OLAP_ERR_DATA_EOF) {
            merge_ctx->key_range_index++;
            continue;
        } else if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "prepare block read failed. status=" << status;
            return status;
        } else {
            break;
        }
    }
    if (merge_ctx->key_range_index >= _key_range_size) {
        merge_ctx->row_block = nullptr;
        return OLAP_ERR_DATA_EOF;
    }
    return status;
}

OLAPStatus AlphaRowsetReader::_init_merge_ctxs(RowsetReaderContext* read_context) {
    if (read_context->reader_type == READER_QUERY) {
        if (read_context->lower_bound_keys->size() !=
                    read_context->is_lower_keys_included->size() ||
            read_context->lower_bound_keys->size() != read_context->upper_bound_keys->size() ||
            read_context->upper_bound_keys->size() !=
                    read_context->is_upper_keys_included->size()) {
            std::string error_msg = "invalid key range arguments";
            LOG(WARNING) << error_msg;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        _key_range_size = read_context->lower_bound_keys->size();
    }

    // avoid polluting index stream cache by non-query workload (compaction/alter/checksum)
    const bool use_index_stream_cache = read_context->reader_type == READER_QUERY;

    for (auto& segment_group : _segment_groups) {
        std::unique_ptr<ColumnData> new_column_data(
                ColumnData::create(segment_group.get(), _parent_tracker));
        OLAPStatus status = new_column_data->init();
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "init column data failed";
            return OLAP_ERR_READER_READING_ERROR;
        }
        new_column_data->set_delete_handler(read_context->delete_handler);
        new_column_data->set_stats(_stats);
        if (read_context->reader_type == READER_ALTER_TABLE) {
            new_column_data->schema_change_init();
            new_column_data->set_using_cache(use_index_stream_cache);
            if (new_column_data->empty() && new_column_data->zero_num_rows()) {
                continue;
            }
        } else {
            std::shared_ptr<std::vector<ColumnPredicate*>> predicates =
                    std::make_shared<std::vector<ColumnPredicate*>>();
            if (read_context->predicates != nullptr) {
                predicates->insert(predicates->end(), read_context->predicates->begin(),
                                   read_context->predicates->end());
            }
            // if unique table with rowset [0-x] or [0-1] [2-y] [...],
            // value column predicates can be pushdown on rowset [0-x] or [2-y]
            if (read_context->value_predicates != nullptr && _rowset->keys_type() == UNIQUE_KEYS &&
                (_rowset->start_version() == 0 || _rowset->start_version() == 2)) {
                predicates->insert(predicates->end(), read_context->value_predicates->begin(),
                                   read_context->value_predicates->end());
            }
            new_column_data->set_read_params(
                    *read_context->return_columns, *read_context->seek_columns,
                    *read_context->load_bf_columns, *read_context->conditions, predicates,
                    use_index_stream_cache, read_context->runtime_state);
            // filter
            if (new_column_data->rowset_pruning_filter()) {
                _stats->rows_stats_filtered += new_column_data->num_rows();
                VLOG_NOTICE << "filter segment group in query in condition. version="
                        << new_column_data->version();
                continue;
            }
        }

        int ret = new_column_data->delete_pruning_filter();
        if (ret == DEL_SATISFIED) {
            _stats->rows_del_filtered += new_column_data->num_rows();
            VLOG_NOTICE << "filter segment group in delete predicate:" << new_column_data->version();
            continue;
        } else if (ret == DEL_PARTIAL_SATISFIED) {
            VLOG_NOTICE << "filter segment group partially in delete predicate:"
                    << new_column_data->version();
            new_column_data->set_delete_status(DEL_PARTIAL_SATISFIED);
        } else {
            VLOG_NOTICE << "not filter segment group in delete predicate:"
                    << new_column_data->version();
            new_column_data->set_delete_status(DEL_NOT_SATISFIED);
        }
        AlphaMergeContext merge_ctx;
        merge_ctx.column_data = std::move(new_column_data);
        _merge_ctxs.emplace_back(std::move(merge_ctx));
    }

    if (!_is_segments_overlapping && _merge_ctxs.size() > 1) {
        LOG(WARNING) << "invalid column_data for cumulative rowset. column_data size:"
                     << _merge_ctxs.size();
        return OLAP_ERR_READER_READING_ERROR;
    }
    return OLAP_SUCCESS;
}

RowsetSharedPtr AlphaRowsetReader::rowset() {
    return std::static_pointer_cast<Rowset>(_rowset);
}

bool AlphaMergeContextComparator::operator()(const AlphaMergeContext* x,
                                             const AlphaMergeContext* y) const {
    return compare_row(*(x->row_cursor.get()), *(y->row_cursor.get())) > 0;
}

} // namespace doris
