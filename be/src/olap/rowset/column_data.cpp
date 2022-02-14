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

#include "olap/rowset/column_data.h"

#include "olap/olap_cond.h"
#include "olap/row_block.h"
#include "olap/rowset/segment_reader.h"
#include "olap/storage_engine.h"

namespace doris {

ColumnData* ColumnData::create(SegmentGroup* segment_group,
                               const std::shared_ptr<MemTracker>& parent_tracker) {
    ColumnData* data = new (std::nothrow) ColumnData(segment_group, parent_tracker);
    return data;
}

ColumnData::ColumnData(SegmentGroup* segment_group,
                       const std::shared_ptr<MemTracker>& parent_tracker)
        : _segment_group(segment_group),
          _parent_tracker(parent_tracker),
          _eof(false),
          _conditions(nullptr),
          _col_predicates(nullptr),
          _delete_status(DEL_NOT_SATISFIED),
          _runtime_state(nullptr),
          _schema(segment_group->get_tablet_schema()),
          _is_using_cache(false),
          _segment_reader(nullptr),
          _lru_cache(nullptr) {
    if (StorageEngine::instance() != nullptr) {
        _lru_cache = StorageEngine::instance()->index_stream_lru_cache();
    } else {
        // for independent usage, eg: unit test/segment tool
        _lru_cache = FileHandler::get_fd_cache();
    }
    _num_rows_per_block = _segment_group->get_num_rows_per_row_block();
}

ColumnData::~ColumnData() {
    _segment_group->release();
    SAFE_DELETE(_segment_reader);
}

OLAPStatus ColumnData::init() {
    _segment_group->acquire();

    auto res = _short_key_cursor.init(_segment_group->short_key_columns());
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "key cursor init failed, res:" << res;
        return res;
    }
    return res;
}

OLAPStatus ColumnData::get_next_block(RowBlock** row_block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    _is_normal_read = true;
    auto res = _get_block(false);
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Get next block failed.";
        }
        *row_block = nullptr;
        return res;
    }
    *row_block = _read_block.get();
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_next_row(const RowCursor** row, bool without_filter) {
    _read_block->pos_inc();
    do {
        if (_read_block->has_remaining()) {
            // 1. get one row for vectorized_row_batch
            size_t pos = _read_block->pos();
            _read_block->get_row(pos, &_cursor);
            if (without_filter) {
                *row = &_cursor;
                return OLAP_SUCCESS;
            }

            // when without_filter is true, _include_blocks is nullptr
            if (_read_block->block_status() == DEL_NOT_SATISFIED) {
                *row = &_cursor;
                return OLAP_SUCCESS;
            } else {
                DCHECK(_read_block->block_status() == DEL_PARTIAL_SATISFIED);
                bool row_del_filter =
                        _delete_handler->is_filter_data(_segment_group->version().second, _cursor);
                if (!row_del_filter) {
                    *row = &_cursor;
                    return OLAP_SUCCESS;
                }
                // This row is filtered, continue to process next row
                _stats->rows_del_filtered++;
                _read_block->pos_inc();
            }
        } else {
            // get_next_block
            auto res = _get_block(without_filter);
            if (res != OLAP_SUCCESS) {
                return res;
            }
        }
    } while (true);

    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_seek_to_block(const RowBlockPosition& block_pos, bool without_filter) {
    // TODO(zc): _segment_readers???
    // open segment reader if needed
    if (_segment_reader == nullptr || block_pos.segment != _current_segment) {
        if (block_pos.segment >= _segment_group->num_segments() ||
            (_end_key_is_set && block_pos.segment > _end_segment)) {
            _eof = true;
            return OLAP_ERR_DATA_EOF;
        }
        SAFE_DELETE(_segment_reader);
        std::string file_name;
        file_name = segment_group()->construct_data_file_path(block_pos.segment);
        _segment_reader = new (std::nothrow)
                SegmentReader(file_name, segment_group(), block_pos.segment, _seek_columns,
                              _load_bf_columns, _conditions, _delete_handler, _delete_status,
                              _lru_cache, _runtime_state, _stats, _parent_tracker);
        if (_segment_reader == nullptr) {
            OLAP_LOG_WARNING("fail to malloc segment reader.");
            return OLAP_ERR_MALLOC_ERROR;
        }

        _current_segment = block_pos.segment;
        auto res = _segment_reader->init(_is_using_cache);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to init segment reader. [res=%d]", res);
            return res;
        }
    }

    uint32_t end_block;
    if (_end_key_is_set && block_pos.segment == _end_segment) {
        end_block = _end_block;
    } else {
        end_block = _segment_reader->block_count() - 1;
    }

    VLOG_NOTICE << "seek from " << block_pos.data_offset << " to " << end_block;
    return _segment_reader->seek_to_block(block_pos.data_offset, end_block, without_filter,
                                          &_next_block, &_segment_eof);
}

OLAPStatus ColumnData::_find_position_by_short_key(const RowCursor& key, bool find_last_key,
                                                   RowBlockPosition* position) {
    RowBlockPosition tmp_pos;
    auto res = _segment_group->find_short_key(key, &_short_key_cursor, find_last_key, &tmp_pos);
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            res = OLAP_ERR_DATA_EOF;
        } else {
            OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        }
        return res;
    }
    res = segment_group()->find_prev_point(tmp_pos, position);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("find prev row block failed. [res=%d]", res);
        return res;
    }
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_find_position_by_full_key(const RowCursor& key, bool find_last_key,
                                                  RowBlockPosition* position) {
    RowBlockPosition tmp_pos;
    auto res = _segment_group->find_short_key(key, &_short_key_cursor, false, &tmp_pos);
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            res = OLAP_ERR_DATA_EOF;
        } else {
            OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        }
        return res;
    }
    RowBlockPosition start_position;
    res = segment_group()->find_prev_point(tmp_pos, &start_position);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("find prev row block failed. [res=%d]", res);
        return res;
    }

    RowBlockPosition end_position;
    res = _segment_group->find_short_key(key, &_short_key_cursor, true, &end_position);
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            res = OLAP_ERR_DATA_EOF;
        } else {
            OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        }
        return res;
    }

    // choose min value of end_position and m_end_key_block_position as real end_position
    if (_end_key_is_set) {
        RowBlockPosition end_key_position;
        end_key_position.segment = _end_segment;
        end_key_position.data_offset = _end_block;
        if (end_position > end_key_position) {
            OLAPIndexOffset index_offset;
            index_offset.segment = _end_segment;
            index_offset.offset = _end_block;
            res = segment_group()->get_row_block_position(index_offset, &end_position);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to get row block position. [res=%d]", res);
                return res;
            }
        }
    }

    // ????end_position
    uint32_t distance = segment_group()->compute_distance(start_position, end_position);

    BinarySearchIterator it_start(0u);
    BinarySearchIterator it_end(distance + 1);
    BinarySearchIterator it_result(0u);
    ColumnDataComparator comparator(start_position, this, segment_group());
    try {
        if (!find_last_key) {
            it_result = std::lower_bound(it_start, it_end, key, comparator);
        } else {
            it_result = std::upper_bound(it_start, it_end, key, comparator);
        }
        VLOG_NOTICE << "get result iterator. offset=" << *it_result
                    << ", start_pos=" << start_position.to_string();
    } catch (std::exception& e) {
        LOG(WARNING) << "exception happens when doing seek. exception=" << e.what();
        return OLAP_ERR_STL_ERROR;
    }

    if (*it_result != *it_start) {
        it_result -= 1;
    }

    if (OLAP_SUCCESS != (res = segment_group()->advance_row_block(*it_result, &start_position))) {
        OLAP_LOG_WARNING(
                "fail to advance row_block. [res=%d it_offset=%u "
                "start_pos='%s']",
                res, *it_result, start_position.to_string().c_str());
        return res;
    }

    if (_end_key_is_set) {
        RowBlockPosition end_key_position;
        end_key_position.segment = _end_segment;
        end_key_position.data_offset = _end_block;
        if (end_position > end_key_position) {
            return OLAP_ERR_DATA_EOF;
        }
    }

    *position = start_position;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_seek_to_row(const RowCursor& key, bool find_last_key, bool is_end_key) {
    RowBlockPosition position;
    OLAPStatus res = OLAP_SUCCESS;
    const TabletSchema& tablet_schema = _segment_group->get_tablet_schema();
    FieldType type = tablet_schema.column(key.field_count() - 1).type();
    if (key.field_count() > _segment_group->get_num_short_key_columns() ||
        OLAP_FIELD_TYPE_VARCHAR == type || OLAP_FIELD_TYPE_STRING == type) {
        res = _find_position_by_full_key(key, find_last_key, &position);
    } else {
        res = _find_position_by_short_key(key, find_last_key, &position);
    }
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Fail to find the key.[res=" << res << " key=" << key.to_string()
                         << " find_last_key=" << find_last_key << "]";
        }
        return res;
    }
    bool without_filter = is_end_key;
    res = _seek_to_block(position, without_filter);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING(
                "fail to get row block. "
                "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                res, position.segment, position.block_size, position.data_offset,
                position.index_offset);
        return res;
    }
    res = _get_block(without_filter);
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Fail to find the key.[res=" << res << " key=" << key.to_string()
                         << " find_last_key=" << find_last_key << "]";
        }
        return res;
    }

    const RowCursor* row_cursor = _current_row();
    if (!find_last_key) {
        // 不找last key。 那么应该返回大于等于这个key的第一个，也就是
        // row_cursor >= key
        // 此处比较2个block的行数，是存在一种极限情况：若未找到满足的block，
        // Index模块会返回倒数第二个block，此时key可能是最后一个block的最后一行
        while (res == OLAP_SUCCESS && compare_row_key(*row_cursor, key) < 0) {
            res = _next_row(&row_cursor, without_filter);
        }
    } else {
        // 找last key。返回大于这个key的第一个。也就是
        // row_cursor > key
        while (res == OLAP_SUCCESS && compare_row_key(*row_cursor, key) <= 0) {
            res = _next_row(&row_cursor, without_filter);
        }
    }

    return res;
}

const RowCursor* ColumnData::seek_and_get_current_row(const RowBlockPosition& position) {
    auto res = _seek_to_block(position, true);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "Fail to seek to block in seek_and_get_current_row, res=" << res
                     << ", segment:" << position.segment << ", block:" << position.data_offset;
        return nullptr;
    }
    res = _get_block(true, 1);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "Fail to get block in seek_and_get_current_row, res=" << res
                     << ", segment:" << position.segment << ", block:" << position.data_offset
                     << ", tablet: " << _segment_group->get_tablet_id();
        return nullptr;
    }
    return _current_row();
}

OLAPStatus ColumnData::prepare_block_read(const RowCursor* start_key, bool find_start_key,
                                          const RowCursor* end_key, bool find_end_key,
                                          RowBlock** first_block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    set_eof(false);
    _end_key_is_set = false;
    _is_normal_read = false;
    // set end position
    if (end_key != nullptr) {
        auto res = _seek_to_row(*end_key, find_end_key, true);
        if (res == OLAP_SUCCESS) {
            // we find a
            _end_segment = _current_segment;
            _end_block = _current_block;
            _end_row_index = _read_block->pos();
            _end_key_is_set = true;
        } else if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Find end key failed.key=" << end_key->to_string();
            return res;
        }
        // res == OLAP_ERR_DATA_EOF means there is no end key, then we read to
        // the end of this ColumnData
    }
    set_eof(false);
    if (start_key != nullptr) {
        auto res = _seek_to_row(*start_key, !find_start_key, false);
        if (res == OLAP_SUCCESS) {
            *first_block = _read_block.get();
        } else if (res == OLAP_ERR_DATA_EOF) {
            _eof = true;
            *first_block = nullptr;
            return res;
        } else {
            LOG(WARNING) << "start_key can't be found.key=" << start_key->to_string();
            return res;
        }
    } else {
        // This is used to
        _is_normal_read = true;

        RowBlockPosition pos;
        pos.segment = 0u;
        pos.data_offset = 0u;
        auto res = _seek_to_block(pos, false);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to seek to block in, res=" << res << ", segment:" << pos.segment
                         << ", block:" << pos.data_offset;
            return res;
        }
        res = _get_block(false);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to get block in , res=" << res << ", segment:" << pos.segment
                         << ", block:" << pos.data_offset;
            return res;
        }
        *first_block = _read_block.get();
    }
    return OLAP_SUCCESS;
}

// ColumnData向上返回的列至少由几部分组成:
// 1. return_columns中要求返回的列,即Fetch命令中指定要查询的列.
// 2. condition中涉及的列, 绝大多数情况下这些列都已经在return_columns中.
// 在这个函数里,合并上述几种情况
void ColumnData::set_read_params(const std::vector<uint32_t>& return_columns,
                                 const std::vector<uint32_t>& seek_columns,
                                 const std::set<uint32_t>& load_bf_columns,
                                 const Conditions& conditions,
                                 std::shared_ptr<std::vector<ColumnPredicate*>> col_predicates,
                                 bool is_using_cache, RuntimeState* runtime_state) {
    _conditions = &conditions;
    _col_predicates = col_predicates;
    _need_eval_predicates = !col_predicates->empty();
    _is_using_cache = is_using_cache;
    _runtime_state = runtime_state;
    _return_columns = return_columns;
    _seek_columns = seek_columns;
    _load_bf_columns = load_bf_columns;

    auto res = _cursor.init(_segment_group->get_tablet_schema(), _seek_columns);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init row_cursor";
    }

    _read_vector_batch.reset(new VectorizedRowBatch(&(_segment_group->get_tablet_schema()),
                                                    _return_columns, _num_rows_per_block,
                                                    _parent_tracker));

    _seek_vector_batch.reset(new VectorizedRowBatch(&(_segment_group->get_tablet_schema()),
                                                    _seek_columns, _num_rows_per_block,
                                                    _parent_tracker));

    _read_block.reset(new RowBlock(&(_segment_group->get_tablet_schema()), _parent_tracker));
    RowBlockInfo block_info;
    block_info.row_num = _num_rows_per_block;
    block_info.null_supported = true;
    block_info.column_ids = _seek_columns;
    _read_block->init(block_info);
}

OLAPStatus ColumnData::get_first_row_block(RowBlock** row_block) {
    DCHECK(!_end_key_is_set) << "end key is set while use block interface.";
    _is_normal_read = true;
    _eof = false;

    // to be same with OLAPData, we use segment_group.
    RowBlockPosition block_pos;
    OLAPStatus res = segment_group()->find_first_row_block(&block_pos);
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            *row_block = nullptr;
            _eof = true;
            return res;
        }
        OLAP_LOG_WARNING("fail to find first row block with SegmentGroup.");
        return res;
    }

    res = _seek_to_block(block_pos, false);
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            OLAP_LOG_WARNING("seek to block fail. [res=%d]", res);
        }
        *row_block = nullptr;
        return res;
    }

    res = _get_block(false);
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "fail to load data to row block. res=" << res
                         << ", version=" << version().first << "-" << version().second;
        }
        *row_block = nullptr;
        return res;
    }

    *row_block = _read_block.get();
    return OLAP_SUCCESS;
}

bool ColumnData::rowset_pruning_filter() {
    if (empty() || zero_num_rows()) {
        return true;
    }

    if (!_segment_group->has_zone_maps()) {
        return false;
    }

    return _conditions->rowset_pruning_filter(_segment_group->get_zone_maps());
}

int ColumnData::delete_pruning_filter() {
    if (empty() || zero_num_rows()) {
        // should return DEL_NOT_SATISFIED, because that when creating rollup tablet,
        // the delete version file should preserved for filter data.
        return DEL_NOT_SATISFIED;
    }

    int num_zone_maps = _schema.keys_type() == KeysType::DUP_KEYS ? _schema.num_columns()
                                                                  : _schema.num_key_columns();
    // _segment_group->get_zone_maps().size() < num_zone_maps for a table is schema changed from older version that not support
    // generate zone map for duplicated mode value column, using DEL_PARTIAL_SATISFIED
    if (!_segment_group->has_zone_maps() ||
        _segment_group->get_zone_maps().size() < num_zone_maps) {
        /*
         * if segment_group has no column statistics, we cannot judge whether the data can be filtered or not
         */
        return DEL_PARTIAL_SATISFIED;
    }

    /*
     * the relationship between delete condition A and B is A || B.
     * if any delete condition is satisfied, the data can be filtered.
     * elseif all delete condition is not satisfied, the data can't be filtered.
     * else is the partial satisfied.
    */
    int ret = DEL_PARTIAL_SATISFIED;
    bool del_partial_satisfied = false;
    bool del_satisfied = false;
    for (auto& delete_condition : _delete_handler->get_delete_conditions()) {
        if (delete_condition.filter_version <= _segment_group->version().first) {
            continue;
        }

        Conditions* del_cond = delete_condition.del_cond;
        int del_ret = del_cond->delete_pruning_filter(_segment_group->get_zone_maps());
        if (DEL_SATISFIED == del_ret) {
            del_satisfied = true;
            break;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_satisfied = true;
        } else {
            continue;
        }
    }

    if (del_satisfied) {
        ret = DEL_SATISFIED;
    } else if (del_partial_satisfied) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_NOT_SATISFIED;
    }

    return ret;
}

uint64_t ColumnData::get_filtered_rows() {
    return _stats->rows_del_filtered;
}

OLAPStatus ColumnData::schema_change_init() {
    _is_using_cache = false;

    for (int i = 0; i < _segment_group->get_tablet_schema().num_columns(); ++i) {
        _return_columns.push_back(i);
        _seek_columns.push_back(i);
    }

    auto res = _cursor.init(_segment_group->get_tablet_schema());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row_cursor");
        return res;
    }

    _read_vector_batch.reset(new VectorizedRowBatch(&(_segment_group->get_tablet_schema()),
                                                    _return_columns, _num_rows_per_block));

    _read_block.reset(new RowBlock(&(_segment_group->get_tablet_schema()), _parent_tracker));

    RowBlockInfo block_info;
    block_info.row_num = _num_rows_per_block;
    block_info.null_supported = true;
    _read_block->init(block_info);
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_get_block_from_reader(VectorizedRowBatch** got_batch, bool without_filter,
                                              int rows_read) {
    VectorizedRowBatch* vec_batch = nullptr;
    if (_is_normal_read) {
        vec_batch = _read_vector_batch.get();
    } else {
        vec_batch = _seek_vector_batch.get();
    }
    // If this is normal read
    do {
        vec_batch->clear();
        if (rows_read > 0) {
            vec_batch->set_limit(rows_read);
        }
        // If we are going to read last block, we need to set batch limit to the end of key
        // if without_filter is true and _end_key_is_set is true, this must seek to start row's
        // block, we must load the entire block.
        if (OLAP_UNLIKELY(!without_filter && _end_key_is_set && _next_block == _end_block &&
                          _current_segment == _end_segment)) {
            vec_batch->set_limit(_end_row_index);
            if (_end_row_index == 0) {
                _segment_eof = true;
            }
        }

        if (!_segment_eof) {
            _current_block = _next_block;
            auto res = _segment_reader->get_block(vec_batch, &_next_block, &_segment_eof);
            if (res != OLAP_SUCCESS) {
                return res;
            }
            // Normal case
            *got_batch = vec_batch;
            return OLAP_SUCCESS;
        }
        // When this segment is read over, we reach here.
        // Seek to next segment
        RowBlockPosition block_pos;
        block_pos.segment = _current_segment + 1;
        block_pos.data_offset = 0;
        auto res = _seek_to_block(block_pos, without_filter);
        if (res != OLAP_SUCCESS) {
            return res;
        }
    } while (true);

    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_get_block(bool without_filter, int rows_read) {
    do {
        VectorizedRowBatch* vec_batch = nullptr;
        auto res = _get_block_from_reader(&vec_batch, without_filter, rows_read);
        if (res != OLAP_SUCCESS) {
            return res;
        }
        // evaluate predicates
        if (!without_filter && _need_eval_predicates) {
            SCOPED_RAW_TIMER(&_stats->vec_cond_ns);
            size_t old_size = vec_batch->size();
            for (auto pred : *_col_predicates) {
                pred->evaluate(vec_batch);
            }
            _stats->rows_vec_cond_filtered += old_size - vec_batch->size();
        }
        // if vector is empty after predicate evaluate, get next block
        if (vec_batch->size() == 0) {
            continue;
        }
        SCOPED_RAW_TIMER(&_stats->block_convert_ns);
        // when reach here, we have already read a block successfully
        _read_block->clear();
        vec_batch->dump_to_row_block(_read_block.get());
        return OLAP_SUCCESS;
    } while (true);
    return OLAP_SUCCESS;
}

} // namespace doris
