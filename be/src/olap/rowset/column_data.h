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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_COLUMN_DATA_H
#define DORIS_BE_SRC_OLAP_ROWSET_COLUMN_DATA_H

#include <string>

#include "gen_cpp/olap_file.pb.h"
#include "olap/column_predicate.h"
#include "olap/delete_handler.h"
#include "olap/olap_common.h"
#include "olap/olap_cond.h"
#include "olap/row.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_group.h"
#include "util/runtime_profile.h"

namespace doris {

class Tablet;
class SegmentReader;

// This class is column data reader. this class will be used in two case.
class ColumnData {
public:
    static ColumnData* create(SegmentGroup* segment_group);
    ColumnData(SegmentGroup* segment_group);
    ~ColumnData();

    // 为了与之前兼容, 暴露部分index的接口
    Version version() const { return _segment_group->version(); }
    bool delete_flag() const { return _segment_group->delete_flag(); }
    uint32_t num_segments() const { return _segment_group->num_segments(); }

    Status init();

    Status prepare_block_read(const RowCursor* start_key, bool find_start_key,
                              const RowCursor* end_key, bool find_end_key, RowBlock** first_block);

    Status get_next_block(RowBlock** row_block);

    void set_read_params(const std::vector<uint32_t>& return_columns,
                         const std::vector<uint32_t>& seek_columns,
                         const std::set<uint32_t>& load_bf_columns, const Conditions& conditions,
                         std::shared_ptr<std::vector<ColumnPredicate*>> col_predicates,
                         bool is_using_cache, RuntimeState* runtime_state);

    Status get_first_row_block(RowBlock** row_block);

    // Only used to binary search in full-key find row
    const RowCursor* seek_and_get_current_row(const RowBlockPosition& position);

    void set_using_cache(bool is_using_cache) { _is_using_cache = is_using_cache; }

    void set_stats(OlapReaderStatistics* stats) { _stats = stats; }

    void set_delete_handler(const DeleteHandler* delete_handler) {
        _delete_handler = delete_handler;
    }

    void set_delete_status(const DelCondSatisfied delete_status) { _delete_status = delete_status; }

    // 开放接口查询_eof，让外界知道数据读取是否正常终止
    // 因为这个函数被频繁访问, 从性能考虑, 放在基类而不是虚函数
    bool eof() { return _eof; }
    void set_eof(bool eof) { _eof = eof; }
    bool* eof_ptr() { return &_eof; }

    bool empty() const { return _segment_group->empty(); }
    bool zero_num_rows() const { return _segment_group->zero_num_rows(); }

    // Return true if should be filtered out
    bool rowset_pruning_filter();
    int delete_pruning_filter();
    uint64_t get_filtered_rows();

    SegmentGroup* segment_group() const { return _segment_group; }
    void set_segment_group(SegmentGroup* segment_group) { _segment_group = segment_group; }
    int64_t num_rows() const { return _segment_group->num_rows(); }

    // To compatible with schema change read, use this function to init column data
    // for schema change read. Only called in get_first_row_block
    Status schema_change_init();

private:
    DISALLOW_COPY_AND_ASSIGN(ColumnData);

    // Try to seek to 'key'. If this function returned with OLAP_SUCCESS, current_row()
    // point to the first row meet the requirement.
    // If there is no such row, Status::OLAPInternalError(OLAP_ERR_DATA_EOF) will return.
    // If error happened, other code will return
    Status _seek_to_row(const RowCursor& key, bool find_key, bool is_end_key);

    // seek to block_pos without load that block, caller must call _get_block()
    // to load _read_block with data. If without_filter is false, this will seek to
    // other block. Because the seeked block may be filtered by condition or delete.
    Status _seek_to_block(const RowBlockPosition& block_pos, bool without_filter);

    Status _find_position_by_short_key(const RowCursor& key, bool find_last_key,
                                       RowBlockPosition* position);
    Status _find_position_by_full_key(const RowCursor& key, bool find_last_key,
                                      RowBlockPosition* position);

    // Used in _seek_to_row, this function will goto next row that valid for this
    // ColumnData
    Status _next_row(const RowCursor** row, bool without_filter);

    // get block from reader, just read vector batch from _current_segment.
    // The read batch return by got_batch.
    Status _get_block_from_reader(VectorizedRowBatch** got_batch, bool without_filter,
                                  int rows_read);

    // get block from segment reader. If this function returns OLAP_SUCCESS
    Status _get_block(bool without_filter, int rows_read = 0);

    const RowCursor* _current_row() {
        _read_block->get_row(_read_block->pos(), &_cursor);
        return &_cursor;
    }

private:
    SegmentGroup* _segment_group;
    // 当到达文件末尾或者到达end key时设置此标志
    bool _eof;
    const Conditions* _conditions;
    std::shared_ptr<std::vector<ColumnPredicate*>> _col_predicates;
    const DeleteHandler* _delete_handler = nullptr;
    DelCondSatisfied _delete_status;
    RuntimeState* _runtime_state;
    OlapReaderStatistics* _stats;

    const TabletSchema& _schema;
    // whether in normal read, use return columns to load block
    bool _is_normal_read = false;
    bool _end_key_is_set = false;
    bool _is_using_cache;
    bool _segment_eof = false;
    bool _need_eval_predicates = false;

    std::vector<uint32_t> _return_columns;
    std::vector<uint32_t> _seek_columns;
    std::set<uint32_t> _load_bf_columns;

    SegmentReader* _segment_reader;

    std::unique_ptr<VectorizedRowBatch> _seek_vector_batch;
    std::unique_ptr<VectorizedRowBatch> _read_vector_batch;

    std::unique_ptr<RowBlock> _read_block = nullptr;
    RowCursor _cursor;
    RowCursor _short_key_cursor;

    // Record when last key is found
    uint32_t _current_block = 0;
    uint32_t _current_segment;
    uint32_t _next_block;

    uint32_t _end_segment;
    uint32_t _end_block;
    int64_t _end_row_index = 0;

    size_t _num_rows_per_block;
    Cache* _lru_cache;
};

class ColumnDataComparator {
public:
    ColumnDataComparator(RowBlockPosition position, ColumnData* olap_data,
                         const SegmentGroup* segment_group)
            : _start_block_position(position),
              _olap_data(olap_data),
              _segment_group(segment_group) {}

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
    bool _compare(const iterator_offset_t& index, const RowCursor& key,
                  ComparatorEnum comparator_enum) const {
        Status res = Status::OK();
        RowBlockPosition position = _start_block_position;
        if (!(res = _segment_group->advance_row_block(index, &position))) {
            LOG(WARNING) << "fail to advance row block. res=" << res;
            throw ComparatorException();
        }
        const RowCursor* helper_cursor = _olap_data->seek_and_get_current_row(position);
        if (helper_cursor == nullptr) {
            LOG(WARNING) << "fail to seek and get current row.";
            throw ComparatorException();
        }

        if (COMPARATOR_LESS == comparator_enum) {
            return compare_row_key(*helper_cursor, key) < 0;
        } else {
            return compare_row_key(*helper_cursor, key) > 0;
        }
    }

    const RowBlockPosition _start_block_position;
    ColumnData* _olap_data;
    const SegmentGroup* _segment_group;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_COLUMN_DATA_H
