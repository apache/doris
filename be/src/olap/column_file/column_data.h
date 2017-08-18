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

#ifndef BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_COLUMN_DATA_H
#define BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_COLUMN_DATA_H

#include <string>

#include "olap/i_data.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"

namespace palo {

class OLAPTable;

namespace column_file {

class SegmentReader;

// 定义文件的读取接口, 接口定义见IData的定义
class ColumnData : public IData {
public:
    explicit ColumnData(OLAPIndex* olap_index);
    virtual ~ColumnData();

    virtual OLAPStatus init();
    virtual void set_conjuncts(std::vector<ExprContext*>* query_conjuncts, 
                               std::vector<ExprContext*>* delete_conjuncts);
    virtual const RowCursor* get_first_row();
    virtual const RowCursor* get_current_row();
    virtual const RowCursor* get_next_row();
    virtual const RowCursor* find_row(const RowCursor& key, bool find_last_key, bool is_end_key);

    virtual OLAPStatus set_end_key(const RowCursor* end_key, bool find_last_end_key);

    virtual void set_read_params(
            const std::vector<uint32_t>& return_columns,
            const std::set<uint32_t>& load_bf_columns,
            const Conditions& conditions,
            const std::vector<RowCursor*>& start_keys,
            const std::vector<RowCursor*>& end_keys,
            bool is_using_cache,
            RuntimeState* runtime_state);

    virtual OLAPStatus get_first_row_block(RowBlock** row_block);
    virtual OLAPStatus get_next_row_block(RowBlock** row_block);

    virtual OLAPStatus get_row_batch(
            uint8_t* batch_buf,
            uint32_t batch_buf_len,
            uint32_t* start_row_index,
            uint32_t* batch_row_num,
            uint32_t* block_row_num,
            std::vector<uint32_t>& return_columns);

    virtual OLAPStatus pickle();
    virtual OLAPStatus unpickle();

    const RowCursor* seek_and_get_current_row(const RowBlockPosition& position);

    virtual uint64_t get_filted_rows();

    void add_filted_rows(uint64_t filted_rows);

private:
    DISALLOW_COPY_AND_ASSIGN(ColumnData);
    OLAPStatus _find_row_block(const RowCursor& key,
            bool find_last_key,
            RowBlockPosition* block_pos);
    OLAPStatus _find_prev_row_block(RowBlockPosition* block_pos);
    OLAPStatus _seek_to_block(const RowBlockPosition &block_pos, bool without_filter);
    OLAPStatus _load_row_block();

    const RowCursor* _get_next_row(bool without_filter);
    OLAPStatus _find_position_by_short_key(
            const RowCursor& key, bool find_last_key, RowBlockPosition *position);
    OLAPStatus _find_position_by_full_key(
            const RowCursor& key, bool find_last_key, RowBlockPosition *position);

private:
    OLAPTable* _table;
    RowCursor* _end_key;                  // 非NULL表示设置了end key
    bool _last_end_key;
    bool _is_using_cache;
    RowBlockPosition _end_key_block_position;
    std::vector<uint32_t> _return_columns;
    std::set<uint32_t> _load_bf_columns;
    
    SegmentReader* _segment_reader;
    uint64_t _filted_rows;
    uint32_t _current_segment;
    // 下面两个成员只用于block接口
    RowBlock* _row_block;                 // 用于get_first_row_block缓存数据
    RowBlockPosition _row_block_pos;      // 与_row_block对应的pos
};

class ColumnDataComparator {
public:
    ColumnDataComparator(
            RowBlockPosition position,
            ColumnData* olap_data,
            const OLAPIndex* index,
            RowCursor* helper_cursor) : 
            _start_block_position(position),
            _olap_data(olap_data),
            _index(index),
            _helper_cursor(helper_cursor) {}

    ~ColumnDataComparator() {}

    // less comparator function
    bool operator()(const iterator_offset_t& index, const RowCursor& key) const {
        return _compare(index, key, COMPARATOR_LESS);
    }
    // larger comparator function
    bool operator()(const RowCursor& key, const iterator_offset_t& index) const {
        return _compare(index, key, COMPARATOR_LARGER);
    }

private:
    bool _compare(
            const iterator_offset_t& index,
            const RowCursor& key,
            ComparatorEnum comparator_enum) const {
        OLAPStatus res = OLAP_SUCCESS;
        RowBlockPosition position = _start_block_position;
        if (OLAP_SUCCESS != (res = _index->advance_row_block(index, &position))) {
            OLAP_LOG_FATAL("fail to advance row block. [res=%d]", res);
            throw ComparatorException();
        }
        const RowCursor* helper_cursor = _olap_data->seek_and_get_current_row(position);
        if (NULL == helper_cursor) {
            OLAP_LOG_WARNING("fail to seek and get current row.");
            throw ComparatorException();
        }

        if (COMPARATOR_LESS == comparator_enum) {
            return helper_cursor->cmp(key) < 0;
        } else {
            return helper_cursor->cmp(key) > 0;
        }
    }

    const RowBlockPosition _start_block_position;
    ColumnData* _olap_data;
    const OLAPIndex* _index;
    RowCursor* _helper_cursor;
};

}  // namespace column_file
}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_COLUMN_DATA_H
