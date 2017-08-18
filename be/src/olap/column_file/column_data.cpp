// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "column_data.h"

#include "olap/column_file/segment_reader.h"
#include "olap/olap_cond.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"

namespace palo {
namespace column_file {

ColumnData::ColumnData(OLAPIndex* olap_index) : 
        IData(COLUMN_ORIENTED_FILE, olap_index),
        _end_key(NULL),
        _last_end_key(false),
        _is_using_cache(false),
        _segment_reader(NULL),
        _filted_rows(0),
        _current_segment(0),
        _row_block(NULL) {
    _table = olap_index->table();
}

ColumnData::~ColumnData() {
    _olap_index->release();
    SAFE_DELETE(_end_key);
    SAFE_DELETE(_segment_reader);
    SAFE_DELETE(_row_block);
}

OLAPStatus ColumnData::init() {
    OLAPStatus res = OLAP_SUCCESS;
    _olap_index->acquire();
    return res;
}

void ColumnData::set_conjuncts(std::vector<ExprContext*>* query_conjuncts, 
                               std::vector<ExprContext*>* delete_conjuncts) {
}

const RowCursor* ColumnData::get_first_row() {
    OLAPStatus res;

    if (olap_index()->num_segments() == 0) {
        set_eof(true);
        return NULL;
    }

    RowBlockPosition block_pos;
    block_pos.segment = 0u;
    block_pos.data_offset = 0u;

    if (OLAP_SUCCESS != (res = _seek_to_block(block_pos, false))) {
        if (OLAP_ERR_DATA_EOF == res) {
            OLAP_LOG_WARNING("stream EOF. "
                    "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                    res,
                    block_pos.segment, block_pos.block_size,
                    block_pos.data_offset, block_pos.index_offset);
            set_eof(true);
        } else {
            OLAP_LOG_WARNING("fail to get row block. "
                    "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                    res,
                    block_pos.segment, block_pos.block_size,
                    block_pos.data_offset, block_pos.index_offset);
        }

        return NULL;
    }

    return get_next_row();
}

const RowCursor* ColumnData::get_current_row() {
    return _segment_reader->get_current_row();
}

const RowCursor* ColumnData::get_next_row() {
    return _get_next_row(false);
}

const RowCursor* ColumnData::_get_next_row(bool without_filter) {
    const RowCursor* cursor = _segment_reader->get_next_row(without_filter);

    // fast path
    // 查找end_key时，只是根据block的索引(即第一行的key)比较，
    // end_key可能存在于上一个block中
    if (OLAP_LIKELY(NULL != cursor)) {
        if (OLAP_LIKELY(NULL == _end_key 
                || _current_segment < _end_key_block_position.segment 
                || (_current_segment == _end_key_block_position.segment 
                && _segment_reader->current_block() < _end_key_block_position.data_offset))) {
            return cursor;
        } else {
            int cmp = cursor->cmp(*_end_key);

            if ((_last_end_key && cmp > 0) || (!_last_end_key && cmp >= 0)) {
                set_eof(true);
                return NULL;
            } else {
                return cursor;
            }
        }
    } else {
        if (_segment_reader->eof()) {
            if (((_end_key != NULL) && 
                    (_current_segment + 1 <= _end_key_block_position.segment)) || 
                    ((_end_key == NULL) && 
                    (_current_segment + 1 < _olap_index->num_segments()))) {
                OLAPStatus res;
                RowBlockPosition block_pos;
                block_pos.segment = _current_segment + 1;
                block_pos.data_offset = 0u;

                res = _seek_to_block(block_pos, without_filter);
                if (OLAP_SUCCESS != res) {
                    if (OLAP_ERR_DATA_EOF == res) {
                        OLAP_LOG_WARNING("stream EOF. "
                                "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                                res,
                                block_pos.segment, block_pos.block_size,
                                block_pos.data_offset, block_pos.index_offset);
                        set_eof(true);
                    } else {
                        OLAP_LOG_WARNING("fail to get row block. "
                                "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                                res,
                                block_pos.segment, block_pos.block_size,
                                block_pos.data_offset, block_pos.index_offset);
                    }
                } else {
                    // TODO:这里写了一个尾递归, 是可以优化去掉递归的
                    return _get_next_row(without_filter);
                }
            } else {
                set_eof(true);
            }
        } else {
            OLAP_LOG_WARNING("fail to reader segment.");
        }
    }

    return NULL;
}

OLAPStatus ColumnData::_find_row_block(
        const RowCursor& key,
        bool find_last_key,
        RowBlockPosition* block_pos) {
    OLAPStatus res = OLAP_SUCCESS;
    RowCursor helper_cursor;
    
    res = helper_cursor.init(olap_index()->short_key_fields());
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("init helper_cursor fail.");
        return res;
    }

    return olap_index()->find_short_key(key, &helper_cursor, find_last_key, block_pos);
}

OLAPStatus ColumnData::_find_prev_row_block(RowBlockPosition* block_pos) {
    RowBlockPosition current = *block_pos;
    return olap_index()->find_prev_point(current, block_pos);
}

OLAPStatus ColumnData::_seek_to_block(const RowBlockPosition& block_pos, bool without_filter) {
    OLAPStatus res;
    RuntimeProfile::Counter* read_data_timer = NULL;
    if (_profile != NULL) {
        read_data_timer = _profile->get_counter("ReadDataTime");    
    }
    SCOPED_TIMER(read_data_timer);

    // open segment reader if needed
    if (NULL == _segment_reader || _current_segment != block_pos.segment) {
        if (NULL != _segment_reader) {
            add_filted_rows(_segment_reader->get_filted_rows());
        }
        SAFE_DELETE(_segment_reader);
        std::string file_name;
        file_name = _table->construct_data_file_path(olap_index()->version(),
                    olap_index()->version_hash(),
                    block_pos.segment);
        _segment_reader = new(std::nothrow) SegmentReader(
                file_name, _table, olap_index(),  block_pos.segment, _return_columns,
                _load_bf_columns, _conditions, _delete_handler, _delete_status,  _runtime_state);
        if (NULL == _segment_reader) {
            OLAP_LOG_WARNING("fail to malloc segment reader.");
            return OLAP_ERR_MALLOC_ERROR;
        }

        _current_segment = block_pos.segment;
        res = _segment_reader->init(_is_using_cache);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to init segment reader. [res=%d]", res);
            return res;
        }
    }

    uint32_t end_block;

    if (NULL != _end_key && _end_key_block_position.segment == block_pos.segment) {
        end_block = _end_key_block_position.data_offset;
    } else {
        end_block = _segment_reader->block_count() - 1;
    }

    OLAP_LOG_DEBUG("###---### seek from %u to %u", block_pos.data_offset, end_block);
    return _segment_reader->seek_to_block(block_pos.data_offset, end_block, without_filter);
}

OLAPStatus ColumnData::_find_position_by_short_key(
        const RowCursor& key, bool find_last_key, RowBlockPosition *position) {
    OLAPStatus res = OLAP_SUCCESS;
    
    res = _find_row_block(key, find_last_key, position);
    if (OLAP_ERR_INDEX_EOF == res) {
        _eof = true;
        return OLAP_SUCCESS;
    } else if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        return res;
    }

    res = _find_prev_row_block(position);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("find prev row block failed. [res=%d]", res);
        return res;
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_find_position_by_full_key(
        const RowCursor& key, bool find_last_key, RowBlockPosition *position) {
    OLAPStatus res = OLAP_SUCCESS;
    set_eof(false);
    OlapStopWatch time_watch;

    RowBlockPosition start_position;
    res = _find_row_block(key, false, &start_position);

    if (OLAP_ERR_INDEX_EOF == res) {
        set_eof(true);
        return OLAP_SUCCESS;
    } else if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        return res;
    }

    res = _find_prev_row_block(&start_position);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("find prev row block failed. [res=%d]", res);
        return res;
    }

    RowBlockPosition end_position;
    res = _find_row_block(key, true, &end_position);
    if (OLAP_ERR_INDEX_EOF == res) {
        set_eof(true);
        return res;
    } else if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        return res;
    }

    // choose min value of end_position and m_end_key_block_position as real end_position
    if (_end_key != NULL && end_position > _end_key_block_position) {
        OLAPIndexOffset index_offset;
        index_offset.segment = _end_key_block_position.segment;
        index_offset.offset = _end_key_block_position.data_offset;
        res = olap_index()->get_row_block_position(index_offset, &end_position);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get row block position. [res=%d]", res);
            return res;
        }
    }

    // helper rowcursor for OLAPDataComparator
    RowCursor data_helper_cursor;
    if (OLAP_SUCCESS != 
            (res = data_helper_cursor.init(_table->tablet_schema()))) {
        OLAP_LOG_FATAL("fail to init row cursor. [res=%d]", res);
        return res;
    }

    // ????end_position
    uint32_t distance = olap_index()->compute_distance(start_position, end_position);

    BinarySearchIterator it_start(0u);
    BinarySearchIterator it_end(distance + 1);
    BinarySearchIterator it_result(0u);
    ColumnDataComparator comparator(
            start_position, 
            this, 
            olap_index(), 
            &data_helper_cursor);
    try {
        if (!find_last_key) {
            it_result = std::lower_bound(it_start, it_end, key, comparator);
        } else {
            it_result = std::upper_bound(it_start, it_end, key, comparator);
        }
        OLAP_LOG_DEBUG("get result iterator. [offset=%u start_pos='%s']", 
                *it_result, start_position.to_string().c_str());
    } catch (std::exception& e) {
        OLAP_LOG_FATAL("exception happens when doing seek. [e.what='%s']", e.what());
        return OLAP_ERR_STL_ERROR;
    }

    if (*it_result != *it_start) {
        it_result -= 1;
    }

    if (OLAP_SUCCESS != (res = olap_index()->advance_row_block(*it_result, 
                &start_position))) {
        OLAP_LOG_WARNING("fail to advance row_block. [res=%d it_offset=%u "
                "start_pos='%s']", res, *it_result, 
                start_position.to_string().c_str());
        return res;
    }

    if (_end_key && start_position > _end_key_block_position) {
        // 查找的是end key之外的数据
        set_eof(true);
        return OLAP_SUCCESS;
    } else {
        set_eof(false);
    }

    *position = start_position;
    return OLAP_SUCCESS;
}

const RowCursor* ColumnData::find_row(const RowCursor& key, bool find_last_key, bool is_end_key) {
    OLAPStatus res = OLAP_SUCCESS;
    RowBlockPosition position;
    
    _eof = false;
    FieldType type = _table->get_field_type_by_index(key.field_count() - 1);
    if (key.field_count() > _table->num_short_key_fields() || OLAP_FIELD_TYPE_VARCHAR == type) {
        res = _find_position_by_full_key(key, find_last_key, &position);
    } else {
        res = _find_position_by_short_key(key, find_last_key, &position);
    }

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("Fail to find the key.[res=%d key=%s find_last_key=%d]", 
                res, key.to_string().c_str(), find_last_key);
        return NULL;
    } else if (_eof) {
        OLAP_LOG_DEBUG("EOF when find the key.[res=%d key=%s find_last_key=%d]", 
                res, key.to_string().c_str(), find_last_key);
        return NULL;
    }

    bool without_filter = is_end_key;
    res = _seek_to_block(position, without_filter);
    if (OLAP_SUCCESS != res) {
        if (OLAP_ERR_DATA_EOF == res) {
            OLAP_LOG_WARNING("stream EOF. "
                    "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                    res,
                    position.segment, position.block_size,
                    position.data_offset, position.index_offset);
            set_eof(true);
        } else {
            OLAP_LOG_WARNING("fail to get row block. "
                    "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                    res,
                    position.segment, position.block_size,
                    position.data_offset, position.index_offset);
        }

        return NULL;
    }

    const RowCursor* row_cursor = NULL;
    if (!find_last_key) {
        // 不找last key。 那么应该返回大于等于这个key的第一个，也就是
        // row_cursor >= key
        // 此处比较2个block的行数，是存在一种极限情况：若未找到满足的block，
        // Index模块会返回倒数第二个block，此时key可能是最后一个block的最后一行
        while (NULL != (row_cursor = _get_next_row(without_filter)) && !eof()
                && row_cursor->cmp(key) < 0) {}
    } else {
        // 找last key。返回大于这个key的第一个。也就是
        // row_cursor > key
        while (NULL != (row_cursor = _get_next_row(without_filter)) && !eof()
                && row_cursor->cmp(key) <= 0) {}
    }

    return row_cursor;
}

const RowCursor* ColumnData::seek_and_get_current_row(const RowBlockPosition& position) {
    OLAPStatus res = OLAP_SUCCESS;

    res = _seek_to_block(position, true);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to seek block. [res=%d]", res);
        if (OLAP_ERR_DATA_EOF == res) {
            set_eof(true);
        }
        return NULL;
    }

    const RowCursor* cursor = _segment_reader->get_next_row(true);
    if (cursor != NULL) {
        return cursor;
    }

    OLAP_LOG_WARNING("fail to read segment.");
    return NULL;
}

OLAPStatus ColumnData::set_end_key(const RowCursor* end_key, bool find_last_end_key) {
    OLAPStatus res;

    if (NULL == end_key) {
        return OLAP_SUCCESS;
    }

    SAFE_DELETE(_end_key);

    if (NULL == find_row(*end_key, find_last_end_key, true)) {
        OLAP_LOG_DEBUG("end_key can't be found.[end_key=%s]", end_key->to_string().c_str());
        return OLAP_SUCCESS;
    }
    
    _end_key = new(std::nothrow) RowCursor();

    if (NULL == _end_key) {
        OLAP_LOG_WARNING("fail to malloc RowCursor.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    std::vector<size_t> end_key_field_lengths;
    end_key->get_field_buf_lengths(&end_key_field_lengths);
    res = _end_key->init_keys(_table->tablet_schema(), end_key_field_lengths);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to init RowCursor. [res=%d]", res);
        return res;
    }

    _end_key->copy(*end_key);
    _last_end_key = find_last_end_key;

    _end_key_block_position.segment = _current_segment;
    _end_key_block_position.data_offset = _segment_reader->current_block();
    
    return OLAP_SUCCESS;
}

// ColumnData向上返回的列至少由几部分组成:
// 1. return_columns中要求返回的列,即Fetch命令中指定要查询的列.
// 2. condition中涉及的列, 绝大多数情况下这些列都已经在return_columns中.
// 3. start_keys和end_keys需要读取的列.
// 在这个函数里,合并上述几种情况
void ColumnData::set_read_params(
        const std::vector<uint32_t>& return_columns,
        const std::set<uint32_t>& load_bf_columns,
        const Conditions& conditions,
        const std::vector<RowCursor*>& start_keys,
        const std::vector<RowCursor*>& end_keys,
        bool is_using_cache,
        RuntimeState* runtime_state) {
    _conditions = &conditions;
    _is_using_cache = is_using_cache;
    _runtime_state = runtime_state;
    //_return_columns = return_columns;
    _load_bf_columns = load_bf_columns;

    std::unordered_set<uint32_t> column_set;

    for (std::vector<uint32_t>::const_iterator it = return_columns.begin();
            it != return_columns.end(); ++it) {
        column_set.insert(*it);
    }

    for (Conditions::CondColumns::const_iterator it = conditions.columns().begin();
            it != conditions.columns().end(); ++it) {
        column_set.insert(it->first);
    }

    uint32_t max_key_column_count = 0;

    for (std::vector<RowCursor*>::const_iterator it = start_keys.begin();
            it != start_keys.end(); ++it) {
        if ((*it)->field_count() > max_key_column_count) {
            max_key_column_count = (*it)->field_count();
        }
    }

    for (std::vector<RowCursor*>::const_iterator it = end_keys.begin();
            it != end_keys.end(); ++it) {
        if ((*it)->field_count() > max_key_column_count) {
            max_key_column_count = (*it)->field_count();
        }
    }

    for (uint32_t i = 0; i < _table->tablet_schema().size(); i++) {
        if (i < max_key_column_count || column_set.find(i) != column_set.end()) {
            _return_columns.push_back(i);
        }
    }
}

OLAPStatus ColumnData::_load_row_block() {
    OLAPStatus res;

    if (NULL == _row_block 
            || _row_block->allocated_row_num() < _segment_reader->num_rows_in_block()) {
        SAFE_DELETE(_row_block);
        _row_block = new(std::nothrow) RowBlock(_table->tablet_schema());

        if (NULL == _row_block) {
            OLAP_LOG_WARNING("fail to allocate row block.");
            return OLAP_ERR_MALLOC_ERROR;
        }

        RowBlockInfo info;
        info.row_num = _segment_reader->num_rows_in_block();
        info.data_file_type = DataFileType::COLUMN_ORIENTED_FILE;
        info.null_supported = _olap_index->get_null_supported(0);  

        res = _row_block->init(info);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to init row block. [res=%d]", res);
            return res;
        }
    } else {
        _row_block->clear();
    }

    uint32_t i = 0;

    do {
        const RowCursor* cursor = get_next_row();

        if (eof()) {
            break;
        }

        if (NULL == cursor) {
            OLAP_LOG_WARNING("fail to get next row.");
            return OLAP_ERR_COLUMN_DATA_LOAD_BLOCK;
        }

        res = _row_block->set_row(i, *cursor);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to set row. [res=%d]", res);
            return OLAP_ERR_COLUMN_DATA_LOAD_BLOCK;
        }

        i++;
    } while (i < _segment_reader->num_rows_in_block());

    res = _row_block->finalize(i);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize row block. [res=%d]", res);
        return OLAP_ERR_COLUMN_DATA_LOAD_BLOCK;
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::get_row_batch(
        uint8_t* batch_buf,
        uint32_t batch_buf_len,
        uint32_t* start_row_index,
        uint32_t* batch_row_num,
        uint32_t* block_row_num,
        std::vector<uint32_t>& return_columns) {
    return _segment_reader->get_row_batch(batch_buf, batch_buf_len, start_row_index,
            batch_row_num, block_row_num, return_columns);
}

OLAPStatus ColumnData::get_first_row_block(RowBlock** row_block) {
    OLAPStatus res;

    if (NULL == row_block) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // 使用get_next_row_block时不应该使用end key, OLAPData里的实现可以调整一下
    if (NULL != _end_key) {
        OLAP_LOG_WARNING("end key is set while use block interface.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // 这里继续使用olap_index是为了与OLAPData的行为(例如返回值)一致
    res = olap_index()->find_first_row_block(&_row_block_pos);

    if (OLAP_ERR_INDEX_EOF == res) {
        *row_block = NULL;
        set_eof(true);
        return res;
    } else if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to find first row block with OLAPIndex.");
        return res;
    }

    res = _seek_to_block(_row_block_pos, false);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("seek to block fail. [res=%d]", res);
        return res;
    }

    res = _load_row_block();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to load data to row block. [res=%d]", res);

        if (OLAP_ERR_DATA_EOF == res) {
            *row_block = NULL;
            set_eof(true);
        }

        return res;
    }

    *row_block = _row_block;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::get_next_row_block(RowBlock** row_block) {
    if (NULL == row_block) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (NULL == _row_block) {
        OLAP_LOG_WARNING("row block is not initialized.");
        return OLAP_ERR_INIT_FAILED;
    }

    if (eof()) {
        *row_block = NULL;
        return OLAP_ERR_DATA_EOF;
    }

    OLAPStatus res = _load_row_block();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to load data to row block. [res=%d]", res);

        if (OLAP_ERR_DATA_EOF == res) {
            *row_block = NULL;
            set_eof(true);
        }

        return res;
    }

    *row_block = _row_block;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::pickle() {
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::unpickle() {
    return OLAP_SUCCESS;
}

void ColumnData::add_filted_rows(uint64_t filted_rows) {
    _filted_rows += filted_rows;
}

uint64_t ColumnData::get_filted_rows(){
    if (NULL != _segment_reader) {
        _filted_rows += _segment_reader->get_filted_rows();
    }
    return _filted_rows;
}

}  // namespace column_file
}  // namespace palo
